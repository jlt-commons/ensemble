(ns ensemble.gen-statem
  "The OTP gen_statem behaviour.

  A machine is a record implementing Machine:

      (init this)  -> [:ok state data], [:ok state data actions], [:stop reason],
                      [:error reason] or :ignore
      (handle-event this type content state data)  -> a result
      (terminate this reason state data)

  That is handle_event_function mode: one callback receives every event.  A
  machine that also implements StateFunctions runs in state_functions mode
  instead: (state-functions this) returns a map from each state to its fn,
  called as (f type content data), and handle-event is not called.  An
  event in a state with no fn stops the machine with
  [:undefined-state-function state].

  An event's type is [:call from], :cast, :info, :timeout (the event
  timeout), :state-timeout, [:generic-timeout name], :internal (from a
  :next-event action) or :enter.  Results and actions are gen_statem's; see
  ensemble.statem, which decides what each means, how postponed and inserted
  events are ordered, and which timeouts are armed.

  With {:state-enter true}, the machine is also called with type :enter,
  content the old state, on its first state and after every state change.

  Clients use ensemble.gen-server's call!, cast! and stop!, and
  ensemble.sys, whose state of a machine is [state data]: a gen-statem
  speaks the same protocol, so a call's reply, a dead machine's :noproc and a
  timeout behave exactly as for a gen-server.  Answer a call with a
  [:reply from value] action or with reply!."
  (:require [ensemble.actor :as act :refer [receive]]
            [ensemble.gen-server :as gs]
            [ensemble.logger :as logger]
            [ensemble.signal :as sig]
            [ensemble.statem :as sm]
            [ensemble.timer :as timer]))

(defprotocol Machine
  (init [this] "Return [:ok state data], [:ok state data actions], [:stop reason], [:error reason] or :ignore.")
  (handle-event [this type content state data] "Return a gen_statem result.")
  (terminate [this reason state data] "Run once as the machine stops."))

(defprotocol StateFunctions
  (state-functions [this] "A map from each state to its fn, (f type content data) -> a result."))

(defn- callback
  "Run the machine's callback for an event in state."
  [m type content state data]
  (if (satisfies? StateFunctions m)
    (if-let [f (get (state-functions m) state)]
      (f type content data)
      (act/exit! [:undefined-state-function state]))
    (handle-event m type content state data)))

(def reply! gs/reply!)

(defn- now [] (act/now-ms))

(defn- arm
  "Timers as {key [deadline content]} from the pure timers {key [ms content]}
  of this transition: a timer the transition started runs from now, or to
  its [:abs t] deadline; any other keeps its deadline, with the content an
  update gave it."
  [started pure deadlines]
  (into {} (for [[k [ms content]] pure]
             [k (if (and (not (contains? started k)) (contains? deadlines k))
                  [(first (get deadlines k)) content]
                  [(if (vector? ms) (second ms) (+ (now) ms)) content])])))

(defn- finish
  "Run terminate with reason and exit with it, reporting an abnormal stop
  after event, [type content]."
  [m reason st event]
  (let [reason (try (terminate m reason (:state st) (:data st)) reason
                    (catch Throwable e (act/reason-of e)))]
    (when-not (sig/shutdown? reason)
      (let [[type content] event
            shown (gs/formatted m {:state (:state st) :data (:data st) :reason reason
                                   :message [(if (vector? type) (first type) type) content]})]
        (logger/report! {:level :error :kind :gen-statem-terminate :server (act/self)
                         :name (act/registered-name (act/self))
                         :last-event (:message shown)
                         :state (:state shown) :data (:data shown) :reason (:reason shown)})))
    (act/exit! reason)))

(defn- next-unsuspended
  "next-event-of, for a machine that is not suspended."
  [parent st idle]
  (let [[k [deadline content]] (first (sort-by (comp first val) (:deadlines st)))
        timer-wait (when k (max 0 (- deadline (now))))
        idle? (and idle (or (nil? timer-wait) (< idle timer-wait)))
        wait (if idle? idle timer-wait)
        m (receive
           [[::gs/call from request] [:event [[:call from] request]]]
           [[::gs/cast request] [:event [:cast request]]]
           [[::gs/stop reason] [:stop reason]]
           [[::gs/system from req] [:system from req]]
           [[:EXIT from reason] :when (and (some? parent) (= from parent)) [:stop reason]]
           [[::wake] [:wake]]
           [:after wait (if idle? [:idle] [:timer k content])]
           [msg [:event [:info msg]]])]
    (case (first m)
      ;; the timer that woke a hibernating machine for its next timeout
      :wake (next-unsuspended parent st idle)
      :idle [m st]
      :stop [m st]
      :system [m st]
      :event [m st]
      :timer (let [[_ k content] m
                   type (if (vector? k) [:generic-timeout (second k)] ({:event :timeout :state :state-timeout} k))]
               [[:event [type content]]
                (-> st (update :timers dissoc k) (update :deadlines dissoc k))]))))

(defn- next-event-of
  "Wait for the next external event, or the earliest timer.  Returns
  [event st]; a timer that fires is removed.  With idle ms and no timer
  due sooner, [[:idle] st] when that long passes with no event."
  [parent st idle]
  (if (:suspended (:dbg st))
    [(gs/system-message parent) st]
    (next-unsuspended parent st idle)))

(defn transition!
  "Handle one event: run the callback, then do what ensemble.statem says of
  its result -- send the replies, move the queues, arm the timers, run the
  enter call on a state change.  Returns the machine's next st, or exits."
  [m opts st event kind]
  (let [[type content] event
        ret (try (callback m type content (:state st) (:data st))
                 (catch Throwable e [::crash (act/reason-of e)]))
        r (if (and (vector? ret) (= ::crash (first ret))) ret (sm/result-of kind ret (:state st) (:data st)))]
    (case (first r)
      ::crash (finish m (second r) st event)
      :Bad (finish m (second r) st event)
      :Stop (let [[_ reason replies data] r]
              (doseq [[from v] (map rest replies)] (reply! from v))
              (finish m reason (assoc st :data data) event))
      (:Transition :Repeat)
      (let [[tag state data actions] r
            [_ postpone? inserted replies timeouts] (sm/actions-of actions)
            changed? (not= state (:state st))
            [_ postponed queue] (if (= :enter kind)
                                  [:Queues (:postponed st) (:queue st)]
                                  (sm/next-queues changed? postpone? event (:postponed st) inserted (:queue st)))
            enter? (= :enter kind)
            timers (if enter?
                     (sm/enter-timers (:timers st) timeouts)
                     (sm/next-timers changed? (:timers st) timeouts))
            ;; the event timeout only runs while nothing is waiting to be handled
            timers (if (seq queue) (dissoc timers :event) timers)
            st* (assoc st :state state :data data :postponed postponed :queue queue
                       :timers timers
                       :deadlines (arm (if enter?
                                    (sm/enter-restarted (:timers st) timeouts)
                                    (sm/restarted changed? (:timers st) timeouts))
                                  timers (:deadlines st))
                       :hibernate (sm/hibernate? actions))]
        (doseq [[from v] replies] (reply! from v))
        ;; a repeat runs the enter call again as if the state were new; one
        ;; from an enter call is a keep, as OTP's is
        (if (and (or changed? (and (= :Repeat tag) (not enter?))) (:state-enter opts))
          (transition! m opts st* [:enter (:state st)] :enter)
          st*)))))

(declare run-loop)

(defn- hibernate!
  "Hibernate before waiting for the next event: the stack goes, and the
  next message -- or a timer set to the earliest timeout, which a
  hibernating machine must still see -- runs the loop again.  The wake
  timer of an earlier hibernation is cancelled, so idle cycles do not pile
  them up."
  [m opts parent st]
  (when-let [t (:wake-timer st)] (timer/cancel-timer t {:async true :info false}))
  (let [wake (when-let [deadline (first (sort (map first (vals (:deadlines st)))))]
               (timer/send-after (max 0 (- deadline (now))) (act/self) [::wake]))]
    (act/hibernate! run-loop m opts parent (-> st (dissoc :hibernate) (assoc :wake-timer wake)))))

(defn- status-of
  "What sys/get-status shows of a machine."
  [m parent st dbg]
  (let [shown (gs/formatted m {:state (:state st) :data (:data st)})]
    {:pid (act/self) :name (act/registered-name (act/self)) :module (type m)
     :status (if (:suspended dbg) :suspended :running) :parent parent
     :state (:state shown) :data (:data shown)}))

(defn- system!
  "Answer a system message; sys sees the machine's state as [state data]."
  [m parent st from req]
  (let [[dbg [state data]] (gs/handle-system (:dbg st) req from [(:state st) (:data st)]
                                             #(status-of m parent st %)
                                             #(and (vector? %) (= 2 (count %))))]
    (assoc st :dbg dbg :state state :data data)))

(defn- run-loop [m opts parent st]
  (loop [st st]
    (if-let [event (first (:queue st))]
      (recur (transition! m opts (update st :queue (comp vec rest)) event :event))
      (let [_ (when (:hibernate st) (hibernate! m opts parent st))
            [msg st] (next-event-of parent st (:hibernate-after opts))]
        (case (first msg)
          (:stop :parent) (finish m (second msg) st msg)
          :idle (hibernate! m opts parent st)
          :system (let [[_ from req] msg] (recur (system! m parent st from req)))
          (let [[type content] (second msg)
                st (update st :dbg gs/observed [(if (vector? type) (first type) type) content])]
            (recur (transition! m opts st (second msg) :event))))))))

(defn- begin
  "Run the machine from state and data with init's actions, ack (if any)
  delivered once they are taken.  Never returns."
  [m opts parent ack state data actions]
  (let [st {:state state :data data :postponed [] :queue [] :timers {} :deadlines {}
            :dbg gs/no-debug}
        [_ _ inserted replies timeouts] (sm/actions-of actions)
        timers (sm/next-timers false {} timeouts)
        st (assoc st :queue inserted :timers timers
                  :deadlines (arm (sm/restarted false {} timeouts) timers {}))]
    (doseq [[from v] replies] (reply! from v))
    (when ack (deliver ack [:ok]))
    (let [st (if (:state-enter opts) (transition! m opts st [:enter state] :enter) st)]
      (run-loop m opts parent st))))

(defn- run [m opts parent ack]
  (let [r (try (sm/init-of (init m))
               (catch Throwable e (let [why (act/reason-of e)] [:Fail why why])))
        quit! (fn [answer exit]
                (when parent (act/unlink! parent))
                (deliver ack answer)
                (act/exit! exit))]
    (case (first r)
      :Ignore (quit! [:ignore] :normal)
      :Fail (let [[_ why exit] r] (quit! [:error why] exit))
      :Start (let [[_ state data actions] r]
               (begin m opts parent ack state data actions)))))

(defn enter-loop
  "Make the calling actor machine m, in state with data, as gen_statem's
  enter_loop: init is not called, and actions are taken as init's would
  be.  opts are start's (:state-enter, :hibernate-after), and :parent, the
  actor whose exit stops it.  Never returns."
  ([m opts state data] (enter-loop m opts state data []))
  ([m opts state data actions]
   (let [as (sm/init-of [:ok state data actions])]
     (when (= :Fail (first as)) (act/exit! (second as)))
     (begin m opts (:parent opts) nil state data (nth as 3)))))

(defn- start* [m {:keys [name trap timeout] :as opts} link?]
  (let [ack (promise)
        parent (when link? (act/self))
        a (act/spawn (fn [] (run m opts parent ack))
                     {:name name :link link? :trap trap :initial-call [(type m) :init]})]
    (gs/await-init a ack timeout "gen-statem")))

(defn start
  "Start machine m, returning once init has returned.  Options: :name,
  :trap (trap exits, so a supervisor's shutdown runs terminate),
  :state-enter (make enter calls), :timeout (ms init may take, default
  :infinity; a slower init is killed and start throws {:reason :timeout})
  and :hibernate-after (hibernate after that many ms with no event)."
  ([m] (start m {}))
  ([m opts] (start* m opts false)))

(defn start-link
  "start, linked to the calling actor."
  ([m] (start-link m {}))
  ([m opts] (start* m opts true)))

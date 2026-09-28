(ns ensemble.gen-statem
  "The OTP gen_statem behaviour, in handle_event_function mode: one callback
  receives every event with the current state and data.

  A machine is a record implementing Machine:

      (init this)  -> [state data] or [state data actions]
      (handle-event this type content state data)  -> a result
      (terminate this reason state data)

  An event's type is [:call from], :cast, :info, :timeout (the event
  timeout), :state-timeout, [:generic-timeout name], :internal (from a
  :next-event action) or :enter.  Results and actions are gen_statem's; see
  ensemble.statem, which decides what each means, how postponed and inserted
  events are ordered, and which timeouts are armed.

  With {:state-enter true}, the machine is also called with type :enter,
  content the old state, on its first state and after every state change.

  Clients use ensemble.gen-server's call!, cast! and stop!: a gen-statem
  speaks the same protocol, so a call's reply, a dead machine's :noproc and a
  timeout behave exactly as for a gen-server.  Answer a call with a
  [:reply from value] action or with reply!."
  (:require [ensemble.actor :as act :refer [receive]]
            [ensemble.gen-server :as gs]
            [ensemble.statem :as sm]))

(defprotocol Machine
  (init [this] "Return [state data] or [state data actions].")
  (handle-event [this type content state data] "Return a gen_statem result.")
  (terminate [this reason state data] "Run once as the machine stops."))

(def reply! gs/reply!)

(defn- now [] (System/currentTimeMillis))

(defn- arm
  "Timers as {key [deadline content]} from the pure timers {key [ms content]}
  of this transition: a timer an action set starts now, one the transition
  left alone keeps its deadline."
  [touched pure deadlines]
  (into {} (for [[k v] pure]
             [k (if (and (not (contains? touched k)) (contains? deadlines k))
                  (get deadlines k)
                  [(+ (now) (first v)) (second v)])])))

(defn- finish [m reason st]
  (terminate m reason (:state st) (:data st))
  (act/exit! reason))

(defn- next-event-of
  "Wait for the next external event, or the earliest timer.  Returns
  [event st]; a timer that fires is removed."
  [parent st]
  (let [[k [deadline content]] (first (sort-by (comp first val) (:deadlines st)))
        wait (when k (max 0 (- deadline (now))))
        m (receive
           [[::gs/call from request] [:event [[:call from] request]]]
           [[::gs/cast request] [:event [:cast request]]]
           [[::gs/stop reason] [:stop reason]]
           [[:EXIT from reason] :when (and (some? parent) (= from parent)) [:stop reason]]
           [:after wait [:timer k content]]
           [msg [:event [:info msg]]])]
    (case (first m)
      :stop [m st]
      :event [m st]
      :timer (let [[_ k content] m
                   type (if (vector? k) [:generic-timeout (second k)] ({:event :timeout :state :state-timeout} k))]
               [[:event [type content]]
                (-> st (update :timers dissoc k) (update :deadlines dissoc k))]))))

(defn transition!
  "Handle one event: run the callback, then do what ensemble.statem says of
  its result -- send the replies, move the queues, arm the timers, run the
  enter call on a state change.  Returns the machine's next st, or exits."
  [m opts st event kind]
  (let [[type content] event
        ret (try (handle-event m type content (:state st) (:data st))
                 (catch Throwable e [::crash (act/reason-of e)]))
        r (if (and (vector? ret) (= ::crash (first ret))) ret (sm/result-of kind ret (:state st) (:data st)))]
    (case (first r)
      ::crash (finish m (second r) st)
      :Bad (finish m [:bad-return-value (second r)] st)
      :Stop (let [[_ reason replies data] r]
              (doseq [[from v] (map rest replies)] (reply! from v))
              (finish m reason (assoc st :data data)))
      :Transition
      (let [[_ state data actions] r
            [_ postpone? inserted replies timeouts] (sm/actions-of actions)
            changed? (not= state (:state st))
            [_ postponed queue] (if (= :enter kind)
                                  [:Queues (:postponed st) (:queue st)]
                                  (sm/next-queues changed? postpone? event (:postponed st) inserted (:queue st)))
            timers (sm/next-timers changed? (:timers st) timeouts)
            ;; the event timeout only runs while nothing is waiting to be handled
            timers (if (seq queue) (dissoc timers :event) timers)
            st* (assoc st :state state :data data :postponed postponed :queue queue
                       :timers timers :deadlines (arm (set (map first timeouts)) timers (:deadlines st)))]
        (doseq [[from v] replies] (reply! from v))
        (if (and changed? (:state-enter opts))
          (transition! m opts st* [:enter (:state st)] :enter)
          st*)))))

(defn- run-loop [m opts parent st]
  (loop [st st]
    (if-let [event (first (:queue st))]
      (recur (transition! m opts (update st :queue (comp vec rest)) event :event))
      (let [[msg st] (next-event-of parent st)]
        (if (= :stop (first msg))
          (finish m (second msg) st)
          (recur (transition! m opts st (second msg) :event)))))))

(defn- run [m opts parent ack]
  (let [r (try (init m)
               (catch Throwable e
                 (when parent (act/unlink! parent))
                 (deliver ack [:error (act/reason-of e)])
                 (act/exit! (act/reason-of e))))
        [state data actions] r
        st {:state state :data data :postponed [] :queue [] :timers {} :deadlines {}}
        [_ _ inserted replies timeouts] (sm/actions-of (or actions []))
        timers (sm/next-timers false {} timeouts)
        st (assoc st :queue inserted :timers timers :deadlines (arm (set (map first timeouts)) timers {}))]
    (doseq [[from v] replies] (reply! from v))
    (deliver ack [:ok])
    (let [st (if (:state-enter opts) (transition! m opts st [:enter state] :enter) st)]
      (run-loop m opts parent st))))

(defn- start* [m {:keys [name trap] :as opts} link?]
  (let [ack (promise)
        parent (when link? (act/self))
        a (act/spawn (fn [] (run m opts parent ack)) {:name name :link link? :trap trap})
        hook (act/on-exit! a (fn [reason] (deliver ack [:error reason])))
        r @ack]
    (act/cancel-exit! a hook)
    (if (= :ok (first r))
      a
      (throw (ex-info (str "gen-statem init failed: " (pr-str (second r))) {:reason (second r)})))))

(defn start
  "Start machine m, returning once init has returned.  Options: :name,
  :trap (trap exits, so a supervisor's shutdown runs terminate), and
  :state-enter (make enter calls)."
  ([m] (start m {}))
  ([m opts] (start* m opts false)))

(defn start-link
  "start, linked to the calling actor."
  ([m] (start-link m {}))
  ([m opts] (start* m opts true)))

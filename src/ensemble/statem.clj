(ns ensemble.statem
  "The pure transition rules of OTP's gen_statem.  The gen-statem runtime
  makes every decision here: what a callback's return means, what its
  actions ask for, how the event queue and the postponed events move on, and
  which timeouts are armed after the transition.

  An event is [type content].  A timer is keyed :event, :state or
  [:generic name], and holds [ms content].")

(defn- timeout-ms [t] (if (= :infinity t) nil t))

(defn- time?
  "A timeout's time: ms, or nil or :infinity, which cancel."
  [t]
  (if (integer? t) (not (neg? t)) (or (nil? t) (= :infinity t))))

(defn- event-type?
  "A type a :next-event may insert."
  [t]
  (or (contains? #{:internal :cast :info :timeout :state-timeout} t)
      (and (vector? t) (= 2 (count t)) (contains? #{:call :generic-timeout} (first t)))))

(defn- tagged-kind
  "The kind of an action written as a vector: :postpone, :next-event,
  :reply, :timer or :hibernate, or :bad."
  [a]
  (let [n (count a)]
    (case (first a)
      :postpone (if (and (= 2 n) (boolean? (second a))) :postpone :bad)
      :hibernate (if (and (= 2 n) (boolean? (second a))) :hibernate :bad)
      :next-event (if (and (= 3 n) (event-type? (second a))) :next-event :bad)
      :reply (if (= 3 n) :reply :bad)
      (:timeout :state-timeout) (if (and (= 3 n) (time? (second a))) :timer :bad)
      :generic-timeout (if (and (= 4 n) (time? (nth a 2))) :timer :bad)
      :bad)))

(defn- action-kind
  "The kind of one action: :postpone, :next-event, :reply, :timer,
  :hibernate, or :bad.  A bare time is an event timeout."
  [a]
  (cond
    (vector? a) (tagged-kind a)
    (= :postpone a) :postpone
    (= :hibernate a) :hibernate
    (time? a) (if (nil? a) :bad :timer)
    :else :bad))

(defn- first-of
  "[a] for the first action of actions whose kind is in kinds, or nil."
  [kinds actions]
  (when (seq actions)
    (if (contains? kinds (action-kind (first actions)))
      [(first actions)]
      (first-of kinds (rest actions)))))

(defn- action-list
  "Actions as a callback gives them -- a list, or one action alone --
  as a list: [:Ok actions], or [:Bad action] for the first that is none."
  [x]
  (let [as (if (and (vector? x) (= :bad (action-kind x))) x [x])
        bad (first-of #{:bad} as)]
    (if bad [:Bad (first bad)] [:Ok as])))

(defn init-of
  "What init's ret means: [:Start state data actions], [:Ignore], or
  [:Fail reason exit], start failing with reason and the process exiting
  with exit.  init returns [:ok state data], [:ok state data actions],
  [:stop reason], [:error reason] or :ignore; anything else fails with
  [:bad-return-from-init ret], and a bad action with
  [:bad-action-from-state-function action]."
  [ret]
  (let [n (if (vector? ret) (count ret) 0)
        tag (when (pos? n) (first ret))
        bad [:bad-return-from-init ret]]
    (cond
      (= :ignore ret) [:Ignore]
      (and (= :ok tag) (= 3 n)) [:Start (nth ret 1) (nth ret 2) []]
      (and (= :ok tag) (= 4 n))
      (let [as (action-list (nth ret 3))]
        (if (= :Ok (first as))
          [:Start (nth ret 1) (nth ret 2) (second as)]
          (let [why [:bad-action-from-state-function (second as)]] [:Fail why why])))
      (and (= :stop tag) (= 2 n)) [:Fail (nth ret 1) (nth ret 1)]
      (and (= :error tag) (= 2 n)) [:Fail (nth ret 1) :normal]
      :else [:Fail bad bad])))

(defn- shape-of
  "A callback's return by its shape alone: [:T state data actions],
  [:S reason replies data], or nil."
  [ret state data]
  (let [n (if (vector? ret) (count ret) 0)
        tag (when (pos? n) (first ret))
        acts (fn [i] (if (< i n) (nth ret i) []))]
    (cond
      (and (= :next-state tag) (<= 3 n 4)) [:T (nth ret 1) (nth ret 2) (acts 3)]
      (and (= :keep-state tag) (<= 2 n 3)) [:T state (nth ret 1) (acts 2)]
      (and (= :keep-state-and-data tag) (<= 1 n 2)) [:T state data (acts 1)]
      (and (= :stop tag) (<= 2 n 3)) [:S (nth ret 1) [] (if (= 3 n) (nth ret 2) data)]
      (and (= :stop-and-reply tag) (<= 3 n 4)) [:S (nth ret 1) (nth ret 2) (if (= 4 n) (nth ret 3) data)]
      :else nil)))

(defn result-of
  "What a callback's return ret means, given the current state and data.
  kind is :enter for a state enter call, else :event.
  [:Transition state data actions], [:Stop reason replies data] or
  [:Bad reason], the reason the machine stops with, as OTP names it."
  [kind ret state data]
  (let [sh (shape-of ret state data)]
    (case (first sh)
      nil [:Bad [:bad-return-from-state-function ret]]
      :S (let [[_ why replies d] sh
               as (action-list replies)
               not-reply (when (= :Ok (first as))
                           (first-of #{:postpone :next-event :timer :hibernate} (second as)))]
           (cond
             (= :Bad (first as)) [:Bad [:bad-reply-action-from-state-function (second as)]]
             not-reply [:Bad [:bad-reply-action-from-state-function (first not-reply)]]
             :else [:Stop why (second as) d]))
      :T (let [[_ st d acts] sh
               as (action-list acts)]
           (cond
             (= :Bad (first as)) [:Bad [:bad-action-from-state-function (second as)]]
             (and (= :enter kind) (not= state st))
             [:Bad [:bad-state-enter-return-from-state-function ret]]
             (and (= :enter kind) (first-of #{:postpone :next-event} (second as)))
             [:Bad [:bad-state-enter-action-from-state-function
                    (first (first-of #{:postpone :next-event} (second as)))]]
             :else [:Transition st d (second as)])))))

(defn- timer-of
  "A timer action as [key ms content]."
  [a]
  (case (if (vector? a) (first a) :bare)
    :bare [:event (timeout-ms a) a]
    :timeout [:event (timeout-ms (nth a 1)) (nth a 2)]
    :state-timeout [:state (timeout-ms (nth a 1)) (nth a 2)]
    :generic-timeout [[:generic (nth a 1)] (timeout-ms (nth a 2)) (nth a 3)]))

(defn actions-of
  "Split actions into [:Actions postpone? inserted replies timers], each list
  in the order the actions give it: inserted events as [type content],
  replies as [from value], timers as [key ms content].  result-of has
  refused any action that is none of these."
  [actions]
  (reduce (fn [[_ p ins reps tms] a]
            (case (action-kind a)
              :postpone   [:Actions (if (vector? a) (boolean (second a)) true) ins reps tms]
              :next-event [:Actions p (conj ins [(nth a 1) (nth a 2)]) reps tms]
              :reply      [:Actions p ins (conj reps [(nth a 1) (nth a 2)]) tms]
              :timer      [:Actions p ins reps (conj tms (timer-of a))]
              [:Actions p ins reps tms]))
          [:Actions false [] [] []]
          actions))

(defn hibernate?
  "Do actions ask to hibernate before the next event?  :hibernate or
  [:hibernate bool]; the last of them decides."
  [actions]
  (reduce (fn [h a]
            (if (= :hibernate (action-kind a))
              (if (vector? a) (boolean (second a)) true)
              h))
          false actions))

(defn next-queues
  "The postponed events and the event queue after handling event.  changed?
  is whether the state changed, postpone? whether the event was postponed,
  inserted the :next-event events.  [:Queues postponed queue]."
  [changed? postpone? event postponed inserted queue]
  (let [kept (if postpone? (conj (vec postponed) event) (vec postponed))]
    (if changed?
      [:Queues [] (vec (concat inserted kept queue))]
      [:Queues kept (vec (concat inserted queue))])))

(defn next-timers
  "The timers after a transition: the event timeout cleared, the state
  timeout cleared on a state change, then each timer action applied in order
  (a nil time cancels)."
  [changed? timers timeouts]
  (reduce (fn [ts [k ms content]]
            (if (nil? ms) (dissoc ts k) (assoc ts k [ms content])))
          (cond-> (dissoc timers :event) changed? (dissoc :state))
          timeouts))

(ns ensemble.statem
  "The pure transition rules of OTP's gen_statem.  The gen-statem runtime
  makes every decision here: what a callback's return means, what its
  actions ask for, how the event queue and the postponed events move on, and
  which timeouts are armed after the transition.

  An event is [type content].  A timer is keyed :event, :state or
  [:generic name], and holds [ms content]; ms is [:abs t] for a deadline
  on the monotonic clock.")

(defn- timeout-ms [t] (if (= :infinity t) nil t))

(defn- time?
  "A timeout's time: ms, or nil or :infinity, which cancel."
  [t]
  (if (integer? t) (not (neg? t)) (or (nil? t) (= :infinity t))))

(defn- abs-opts?
  "A timeout's options: a map whose :abs, if there, is a boolean."
  [o]
  (and (map? o) (or (not (contains? o :abs)) (boolean? (get o :abs)))))

(defn- abs?
  "Do a timeout's options make its time absolute?"
  [o]
  (true? (get o :abs)))

(defn- abs-time?
  "A time with its options: an absolute one may be any integer, as a
  deadline on the clock is."
  [t o]
  (if (abs? o) (or (integer? t) (time? t)) (time? t)))

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
      (:timeout :state-timeout)
      (cond
        (= 2 n) (if (= :cancel (second a)) :timer :bad)
        (= 3 n) (if (or (= :update (second a)) (time? (second a))) :timer :bad)
        (= 4 n) (if (and (abs-opts? (nth a 3)) (abs-time? (second a) (nth a 3))) :timer :bad)
        :else :bad)
      :generic-timeout
      (cond
        (= 3 n) (if (= :cancel (nth a 2)) :timer :bad)
        (= 4 n) (if (or (= :update (nth a 2)) (time? (nth a 2))) :timer :bad)
        (= 5 n) (if (and (abs-opts? (nth a 4)) (abs-time? (nth a 2) (nth a 4))) :timer :bad)
        :else :bad)
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
  [:R state data actions] for a repeat, [:S reason replies data], or nil."
  [ret state data]
  (let [n (if (vector? ret) (count ret) 0)
        tag (when (pos? n) (first ret))
        acts (fn [i] (if (< i n) (nth ret i) []))]
    (cond
      (and (= :next-state tag) (<= 3 n 4)) [:T (nth ret 1) (nth ret 2) (acts 3)]
      (and (= :keep-state tag) (<= 2 n 3)) [:T state (nth ret 1) (acts 2)]
      (and (= :keep-state-and-data tag) (<= 1 n 2)) [:T state data (acts 1)]
      (and (= :repeat-state tag) (<= 2 n 3)) [:R state (nth ret 1) (acts 2)]
      (and (= :repeat-state-and-data tag) (<= 1 n 2)) [:R state data (acts 1)]
      (and (= :stop tag) (<= 2 n 3)) [:S (nth ret 1) [] (if (= 3 n) (nth ret 2) data)]
      (and (= :stop-and-reply tag) (<= 3 n 4)) [:S (nth ret 1) (nth ret 2) (if (= 4 n) (nth ret 3) data)]
      :else nil)))

(defn result-of
  "What a callback's return ret means, given the current state and data.
  kind is :enter for a state enter call, else :event.
  [:Transition state data actions], [:Repeat state data actions] (keep the
  state, and run its enter call again), [:Stop reason replies data] or
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
      (:T :R)
         (let [[tag st d acts] sh
               as (action-list acts)]
           (cond
             (= :Bad (first as)) [:Bad [:bad-action-from-state-function (second as)]]
             (and (= :enter kind) (not= state st))
             [:Bad [:bad-state-enter-return-from-state-function ret]]
             (and (= :enter kind) (first-of #{:postpone :next-event} (second as)))
             [:Bad [:bad-state-enter-action-from-state-function
                    (first (first-of #{:postpone :next-event} (second as)))]]
             (= :R tag) [:Repeat st d (second as)]
             :else [:Transition st d (second as)])))))

(defn- time-of
  "A timer action's time as a timer holds it: ms, [:abs t], :update, or nil
  to cancel."
  [t opts]
  (cond
    (= :cancel t) nil
    (= :update t) :update
    (nil? (timeout-ms t)) nil
    (abs? opts) [:abs t]
    :else t))

(defn- timer-of
  "A timer action as [key time content]."
  [a]
  (if (vector? a)
    (let [[tag x y z w] a]
      (case tag
        (:timeout :state-timeout)
        [({:timeout :event :state-timeout :state} tag) (time-of x z) (if (= :cancel x) nil y)]
        :generic-timeout [[:generic x] (time-of y w) (if (= :cancel y) nil z)]))
    [:event (timeout-ms a) a]))

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

(defn- set-timer
  "ts with one timer action applied: a nil time cancels, :update changes a
  running timer's content -- or starts one of time 0 -- and any other time
  starts the timer."
  [ts [k ms content]]
  (cond
    (nil? ms) (dissoc ts k)
    (= :update ms) (if (contains? ts k) (assoc ts k [(first (get ts k)) content]) (assoc ts k [0 content]))
    :else (assoc ts k [ms content])))

(defn- carried
  "The timers a transition starts from: the event timeout cleared, the state
  timeout cleared on a state change."
  [changed? timers]
  (if changed? (dissoc timers :event :state) (dissoc timers :event)))

(defn next-timers
  "The timers after a transition: the event timeout cleared, the state
  timeout cleared on a state change, then each timer action applied in
  order."
  [changed? timers timeouts]
  (reduce set-timer (carried changed? timers) timeouts))

(defn- started-keys
  "The keys of the timers that timeouts start anew from ts."
  [ts timeouts]
  (second
   (reduce (fn [[ts started] [k ms _ :as t]]
             [(set-timer ts t)
              (cond
                (nil? ms) (disj started k)
                (and (= :update ms) (contains? ts k)) started
                :else (conj started k))])
           [ts #{}]
           timeouts)))

(defn restarted
  "The keys of the timers a transition starts anew: those an action sets,
  and those an update finds not running.  An update of a running timer
  keeps its deadline, and a cancel ends whatever was started."
  [changed? timers timeouts]
  (started-keys (carried changed? timers) timeouts))

(defn enter-timers
  "The timers after an enter call: its timer actions applied to the
  transition's, which it keeps whole -- the event timeout included, since
  no event has arrived since."
  [timers timeouts]
  (reduce set-timer timers timeouts))

(defn enter-restarted
  "The keys of the timers an enter call starts anew."
  [timers timeouts]
  (started-keys timers timeouts))

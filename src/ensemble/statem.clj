(ns ensemble.statem
  "The pure transition rules of OTP's gen_statem.  The gen-statem runtime
  makes every decision here: what a callback's return means, what its
  actions ask for, how the event queue and the postponed events move on, and
  which timeouts are armed after the transition.

  An event is [type content].  A timer is keyed :event, :state or
  [:generic name], and holds [ms content].")

(defn- actions? [x] (vector? x))

(defn- timeout-ms [t] (if (= :infinity t) nil t))

(defn- action-kind
  "The kind of one action: :postpone, :next-event, :reply, :timer, or :bad."
  [a]
  (cond
    (= :postpone a) :postpone
    (and (vector? a) (= :postpone (first a)) (= 2 (count a))) :postpone
    (and (vector? a) (= :next-event (first a)) (= 3 (count a))) :next-event
    (and (vector? a) (= :reply (first a)) (= 3 (count a))) :reply
    (and (vector? a) (contains? #{:timeout :state-timeout} (first a)) (= 3 (count a))) :timer
    (and (vector? a) (= :generic-timeout (first a)) (= 4 (count a))) :timer
    :else :bad))

(defn- enter-ok?
  "An enter call may not postpone or insert events."
  [actions]
  (not-any? (fn [a] (contains? #{:postpone :next-event} (action-kind a))) actions))

(defn result-of
  "What a callback's return ret means, given the current state and data.
  kind is :enter for a state enter call, else :event.
  [:Transition state data actions], [:Stop reason replies data] or
  [:Bad ret]."
  [kind ret state data]
  (let [n (if (vector? ret) (count ret) 0)
        tag (when (pos? n) (first ret))
        acts (fn [i] (if (< i n) (nth ret i) []))
        r (cond
            (and (= :next-state tag) (<= 3 n 4) (actions? (acts 3)))
            [:Transition (nth ret 1) (nth ret 2) (acts 3)]
            (and (= :keep-state tag) (<= 2 n 3) (actions? (acts 2)))
            [:Transition state (nth ret 1) (acts 2)]
            (and (= :keep-state-and-data tag) (<= 1 n 2) (actions? (acts 1)))
            [:Transition state data (acts 1)]
            (and (= :stop tag) (<= 2 n 3))
            [:Stop (nth ret 1) [] (if (= 3 n) (nth ret 2) data)]
            (and (= :stop-and-reply tag) (<= 3 n 4) (actions? (nth ret 2)))
            [:Stop (nth ret 1) (nth ret 2) (if (= 4 n) (nth ret 3) data)]
            :else [:Bad ret])]
    (if (and (= :enter kind) (= :Transition (first r))
             (or (not= state (nth r 1)) (not (enter-ok? (nth r 3)))))
      [:Bad ret]
      r)))

(defn- timer-of
  "A timer action as [key ms content]."
  [a]
  (case (first a)
    :timeout [:event (timeout-ms (nth a 1)) (nth a 2)]
    :state-timeout [:state (timeout-ms (nth a 1)) (nth a 2)]
    :generic-timeout [[:generic (nth a 1)] (timeout-ms (nth a 2)) (nth a 3)]))

(defn actions-of
  "Split actions into [:Actions postpone? inserted replies timers], each list
  in the order the actions give it: inserted events as [type content],
  replies as [from value], timers as [key ms content].  An action that is
  none of these is ignored."
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

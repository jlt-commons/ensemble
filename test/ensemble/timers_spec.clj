(ns ensemble.timers-spec
  "Contract for ensemble.timers: the timer table behind erlang's
  send_after, start_timer, cancel_timer and read_timer, and the timer
  module's intervals, from their docs.

  - A timer fires once, when its time has passed: its message goes to its
    destination then, and it is gone.  A timer of 0 ms fires at the next
    look.  Timers fire in deadline order; two with the same deadline fire in
    the order they were started.
  - cancel_timer answers the time left in ms and removes the timer, so it
    never fires; for a timer that has fired, or was cancelled, or never
    was, it answers false and changes nothing.  read_timer answers the
    same without removing anything.
  - When a process exits, every timer whose destination is its pid is
    cancelled, as erlang's are.  A timer to a name is not: the name is
    looked up when it fires.
  - An interval (timer:send_interval, apply_interval) fires every Time ms,
    at fixed deadlines, until cancelled; it is linked to the process that
    started it and ends when that process exits.

  A timer is [:Timer ref deadline dest msg every owner]: every is nil for a
  one-shot timer, owner is the process an interval is linked to (nil for
  none).  Deadlines are milliseconds of a monotonic clock."
  (:require [writ.spec :refer [spec data ann refine graph law calls same]]
            [ensemble.timers :as tm]
            [ensemble.timer :as timer]))

(spec ensemble.timers {:require :proved})

(refine Every [e Any] (or (nil? e) (nat-int? e)))
(data Timer (Timer Any Int Any Any Every Any))
(data Cancelled (Cancelled (Vec Timer) Any))
(data Due (Due (Vec Any) (Vec Timer)))

(ann arm [(Vec Timer) Any Int Nat Any Any Any Any -> (Vec Timer)])
(ann cancel [(Vec Timer) Any Int -> Cancelled])
(ann left [(Vec Timer) Any Int -> Any])
(ann due [(Vec Timer) Int -> Due])
(ann fire [(Vec Timer) Int -> (Vec Timer)])
(ann gone [(Vec Timer) Any -> (Vec Timer)])
(ann next-deadline [(Vec Timer) -> Any])

;; --- the state graph ----------------------------------------------------

;; a table moves on by starting, cancelling, firing and exits
;; a look changes the table only when a timer is due, and an exit only
;; when a timer is to or started by the process
(graph table
  {:states {:table (Vec Timer), :cancelled Cancelled, :due Due}
   :edges  {:table {[arm _ Any Int Nat Any Any Any Any] #{:table}
                    [cancel _ Any Int] #{:cancelled}
                    [due _ Int] #{:due}
                    [fire _ Int] {:to #{:table}
                                  :when (fn [t now] (some (fn [x] (case (first x) :Timer (let [[_ _ at] x] (<= at now)))) t))}
                    [gone _ Any] {:to #{:table}
                                  :when (fn [t p] (some (fn [x] (case (first x)
                                                                  :Timer (let [[_ _ _ d _ _ o] x] (or (= d p) (= o p)))))
                                                        t))}}}
   :final  [:cancelled :due]})

;; --- the spec's vocabulary ----------------------------------------------

(defn one-shot
  "A table of one timer, ref r, started at now for ms, to d with m."
  [r now ms d m]
  (tm/arm [] r now ms d m nil nil))

(defn fired [d] (case (first d) :Due (let [[_ f] d] f)))
(defn table-of [c] (case (first c) :Cancelled (let [[_ t] c] t)))
(defn answer-of [c] (case (first c) :Cancelled (let [[_ _ a] c] a)))

;; --- reading and cancelling ---------------------------------------------

(law a-new-timer-has-its-time-left
  (forall [r Keyword, now Int, ms Nat, d Keyword, m Any]
    (=> (pos? ms) (= (left (one-shot r now ms d m) r now) ms))))

(law time-left-counts-down
  (forall [r Keyword, now Int, ms Nat, k Nat, d Keyword, m Any]
    (=> (< k ms) (= (left (one-shot r now ms d m) r (+ now k)) (- ms k)))))

(law a-timer-whose-time-has-passed-has-none-left
  (forall [r Keyword, now Int, ms Nat, k Nat, d Keyword, m Any]
    (=> (>= k ms) (= (left (one-shot r now ms d m) r (+ now k)) false))))

(law cancel-answers-the-time-left
  (forall [r Keyword, now Int, ms Nat, k Nat, d Keyword, m Any]
    (= (answer-of (cancel (one-shot r now ms d m) r (+ now k)))
       (left (one-shot r now ms d m) r (+ now k)))))

(law a-cancelled-timer-is-gone
  (forall [r Keyword, now Int, ms Nat, k Nat, d Keyword, m Any]
    (= (table-of (cancel (one-shot r now ms d m) r (+ now k))) [])))

(law a-cancelled-timer-never-fires
  (forall [r Keyword, now Int, ms Nat, k Nat, later Int, d Keyword, m Any]
    (= (fired (due (table-of (cancel (one-shot r now ms d m) r (+ now k))) later)) [])))

(law cancelling-what-is-not-there-answers-false-and-changes-nothing
  (forall [t (Vec Timer), r Any, now Int]
    (=> (not-any? (fn [x] (case (first x) :Timer (let [[_ ref] x] (= r ref)))) t)
        (= (cancel t r now) [:Cancelled t false]))))

(law reading-changes-nothing
  (forall [r Keyword, s Keyword, now Int, ms Nat, d Keyword, m Any]
    (=> (not= r s) (= (left (one-shot r now ms d m) s now) false))))

;; --- firing -------------------------------------------------------------

(law a-timer-fires-when-its-time-has-passed
  (forall [r Keyword, now Int, ms Nat, k Nat, d Keyword, m Any]
    (=> (>= k ms) (= (due (one-shot r now ms d m) (+ now k)) [:Due [[d m]] []]))))

(law a-timer-waits-until-then
  (forall [r Keyword, now Int, ms Nat, k Nat, d Keyword, m Any]
    (=> (< k ms) (= (due (one-shot r now ms d m) (+ now k)) [:Due [] (one-shot r now ms d m)]))))

(law a-zero-timer-fires-at-the-next-look
  (forall [r Keyword, now Int, d Keyword, m Any]
    (= (fired (due (one-shot r now 0 d m) now)) [[d m]])))

(law timers-fire-in-deadline-order
  (forall [now Int, a Nat, b Nat, d Keyword, m Any, n Any]
    (=> (< a b)
        (= (fired (due (arm (arm [] :late now b d m nil nil) :early now a d n nil nil) (+ now b)))
           [[d n] [d m]]))))

(law equal-deadlines-fire-in-start-order
  (forall [now Int, a Nat, d Keyword, e Keyword, m Any, n Any]
    (= (fired (due (arm (arm [] :first now a d m nil nil) :second now a e n nil nil) (+ now a)))
       [[d m] [e n]])))

(law due-leaves-the-table-fire-does
  (forall [t (Vec Timer), now Int]
    (same (nth (due t now) 2) (fire t now))))

(law only-the-expired-fire
  (forall [now Int, a Nat, b Nat, d Keyword, m Any, n Any]
    (=> (< a b)
        (= (due (arm (arm [] :x now a d m nil nil) :y now b d n nil nil) (+ now a))
           [:Due [[d m]] (arm [] :y now b d n nil nil)]))))

(law an-interval-fires-and-comes-back-at-fixed-deadlines
  (forall [r Keyword, now Int, ms Nat, d Keyword, m Any, o Keyword]
    (=> (pos? ms)
        (= (due (arm [] r now ms d m ms o) (+ now ms))
           [:Due [[d m]] [[:Timer r (+ now ms ms) d m ms o]]]))))

(law equal-deadline-intervals-come-back-in-start-order
  (forall [now Int, ms Nat, d Keyword, m Any, n Any, o Keyword]
    (=> (pos? ms)
        (= (fire (arm (arm [] :first now ms d m ms o) :second now ms d n ms o) (+ now ms))
           [[:Timer :first (+ now ms ms) d m ms o] [:Timer :second (+ now ms ms) d n ms o]]))))

;; --- exits --------------------------------------------------------------

(law an-exit-cancels-the-timers-to-the-process
  (forall [r Keyword, now Int, ms Nat, p Keyword, m Any]
    (= (gone (one-shot r now ms p m) p) [])))

(law an-exit-cancels-the-intervals-it-started
  (forall [r Keyword, now Int, ms Nat, d Keyword, m Any, o Keyword]
    (= (gone (arm [] r now ms d m ms o) o) [])))

(law an-exit-leaves-other-timers-alone
  (forall [r Keyword, now Int, ms Nat, d Keyword, m Any, p Keyword]
    (=> (not= d p) (= (gone (one-shot r now ms d m) p) (one-shot r now ms d m)))))

(law an-exit-keeps-the-other-timers-in-order
  (forall [now Int, a Nat, b Nat, d Keyword, e Keyword, p Keyword, m Any]
    (=> (and (not= d p) (not= e p))
        (= (gone (arm (arm [] :x now a d m nil nil) :y now b e m nil nil) p)
           (arm (arm [] :x now a d m nil nil) :y now b e m nil nil)))))

;; --- when to look next --------------------------------------------------

(law the-next-look-is-the-earliest-deadline
  (forall [now Int, a Nat, b Nat, d Keyword, m Any]
    (= (next-deadline (arm (arm [] :x now a d m nil nil) :y now b d m nil nil))
       (+ now (min a b)))))

(law an-empty-table-needs-no-look
  (= (next-deadline []) nil))

;; --- the timer server decides through these ------------------------------

(calls timer/serve {:through [tm/arm tm/cancel tm/left tm/due tm/gone tm/next-deadline]})
(calls tm/due {:through [tm/fire]})

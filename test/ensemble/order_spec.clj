(ns ensemble.order-spec
  "Contract for ensemble.order: the decisions of an OTP supervisor, as the
  supervisor docs state them.

  - A :permanent child is always restarted, a :temporary one never, and a
    :transient one only when it exits with a reason other than :normal,
    :shutdown or [:shutdown term].
  - When a child is restarted, :one-for-one restarts it alone; :one-for-all
    first stops every other child, then starts them all again; :rest-for-one
    does that for the children started after it.  Children stop in reverse
    start order and start in start order.  A :temporary child the strategy
    stops is not started again.
  - :simple-one-for-one is one-for-one for children started from one
    template: a child is restarted alone.
  - More than intensity restarts within period and the supervisor gives up.
  - A significant child that terminates on its own and is not restarted
    shuts its supervisor down: at once under :any-significant, and when it
    was the last significant child running under :all-significant.  Under
    :never, the default, nothing does."
  (:require [writ.spec :refer [spec data ann refine graph law calls]]
            [ensemble.order :as ord]
            [ensemble.supervisor :as sup]))

(spec ensemble.order {:require :proved})

(refine Strategy [s Keyword] (contains? #{:one-for-one :one-for-all :rest-for-one :simple-one-for-one} s))
(refine AutoShutdown [a Keyword] (contains? #{:never :any-significant :all-significant} a))
(refine Restart  [r Keyword] (contains? #{:permanent :transient :temporary} r))

(data Plan (Plan (Vec Nat) (Vec Nat)))
(data Verdict (Allow (Vec Int)) (Exceed (Vec Int)))

(ann restart? [Keyword Any -> Bool])
(ann index-of [(List Nat) Nat -> Any])
(ann restart-plan [Strategy (Vec (Tuple Nat Restart)) Nat -> Plan])
(ann stop-order [(Vec Nat) -> (Vec Nat)])
(ann intensity [(Vec Int) Int Nat Nat -> Verdict])
(ann auto-shutdown? [AutoShutdown Bool Restart Any Nat -> Bool])

;; --- the state graph ----------------------------------------------------

(refine Allowed  [v Verdict] (= :Allow (first v)))
(refine Exceeded [v Verdict] (= :Exceed (first v)))

;; a child's exit leads to a plan for the children; the restart times so far
;; lead, with one more restart, to allowing it or giving up
(graph supervise
  {:states {:children (Vec (Tuple Nat Restart)), :plan Plan,
            :times (Vec Int), :allow Allowed, :exceed Exceeded}
   :edges  {:children {[restart-plan Strategy _ Nat] #{:plan}}
            :times    {[intensity _ Int Nat Nat] #{:allow :exceed}}}})

;; --- the spec's vocabulary ----------------------------------------------

(defn orderly? [r]
  (or (= :normal r) (= :shutdown r)
      (and (vector? r) (= 2 (count r)) (= :shutdown (first r)))))

(defn distinct-ids? [cs] (= (count cs) (count (distinct (map first cs)))))

(defn model-plan
  "The plan, straight from the docs' wording.  Where id stands among the
  children is index-of's, whose own laws say what it is."
  [strategy cs id]
  (let [i (ord/index-of (mapv first cs) id)]
    (if (nil? i)
      [:Plan [] []]
      (let [hit (case strategy
                  (:one-for-one :simple-one-for-one) [(nth cs i)]
                  :one-for-all cs
                  :rest-for-one (drop i cs))
            others (remove #(= id (first %)) hit)]
        [:Plan (vec (reverse (map first others)))
               (vec (for [c hit :when (or (= id (first c)) (not= :temporary (second c)))]
                      (first c)))]))))

(defn model-intensity [times now period max-r]
  (let [recent (conj (vec (filter #(<= (- now %) period) times)) now)]
    [(if (> (count recent) max-r) :Exceed :Allow) recent]))

(def abc [[1 :permanent] [2 :transient] [3 :temporary]])

;; --- whether an exit earns a restart ------------------------------------

(law permanent-always-restarts (forall [r Any] (restart? :permanent r)))

;; a restart type that is not one of the three is read as :permanent, OTP's
;; default: a child is restarted unless its spec says otherwise
(law any-other-type-is-permanent
  (forall [t Keyword, r Any]
    (=> (not (contains? #{:transient :temporary} t)) (restart? t r))))

(law temporary-never-restarts (forall [r Any] (not (restart? :temporary r))))

(law transient-restarts-only-on-an-abnormal-exit
  (forall [r Any] (= (restart? :transient r) (not (orderly? r)))))

(law transient-examples
  (and (not (restart? :transient :normal))
       (not (restart? :transient :shutdown))
       (not (restart? :transient [:shutdown :why]))
       (restart? :transient :boom)
       (restart? :transient :killed)))

;; --- what the strategy stops and starts ---------------------------------

(law one-for-one-touches-only-the-child
  (= (restart-plan :one-for-one abc 2) [:Plan [] [2]]))

(law one-for-all-stops-the-rest-in-reverse-and-starts-all-but-temporary
  (= (restart-plan :one-for-all abc 1) [:Plan [3 2] [1 2]]))

(law rest-for-one-touches-the-child-and-later
  (= (restart-plan :rest-for-one abc 2) [:Plan [3] [2]]))

(law a-temporary-child-that-failed-is-its-own-start
  (= (restart-plan :one-for-all abc 3) [:Plan [2 1] [1 2 3]]))

(law an-absent-id-has-no-index
  (forall [cs (Vec (Tuple Nat Restart)), id Nat]
    (=> (not-any? #(= id (first %)) cs)
        (nil? (index-of (mapv first cs) id)))))

(law a-found-index-is-a-position
  (forall [ids (List Nat), id Nat]
    (=> (some? (index-of ids id))
        (and (integer? (index-of ids id)) (<= 0 (index-of ids id))))))

(law a-child-index-is-a-position
  (forall [cs (Vec (Tuple Nat Restart)), id Nat]
    (=> (some? (index-of (mapv first cs) id))
        (and (integer? (index-of (mapv first cs) id)) (<= 0 (index-of (mapv first cs) id))))))

(law the-restarted-from-a-child-include-it
  (forall [xs (Vec (Tuple Nat Restart)), id Nat]
    (=> (some? (index-of (mapv first xs) id))
        (some #{id}
              (for [c (drop (index-of (mapv first xs) id) xs)
                    :when (or (= id (first c)) (not= :temporary (second c)))]
                (first c))))))

(law an-index-names-a-child-with-the-id
  (forall [cs (Vec (Tuple Nat Restart)), id Nat]
    (=> (some? (index-of (mapv first cs) id))
        (and (< (index-of (mapv first cs) id) (count cs))
             (= id (first (nth cs (index-of (mapv first cs) id))))))))

(law a-child-present-has-an-index
  (forall [cs (Vec (Tuple Nat Restart)), i Nat]
    (=> (< i (count cs))
        (integer? (index-of (mapv first cs) (first (nth cs i)))))))

(law index-of-example (= (index-of [4 7 9] 7) 1))

(law an-unknown-child-plans-nothing
  (forall [s Strategy, cs (Vec (Tuple Nat Restart))]
    (=> (not-any? #(= 99 (first %)) cs)
        (= (restart-plan s cs 99) [:Plan [] []]))))

(law restart-plan-is-the-docs
  (forall [s Strategy, cs (Vec (Tuple Nat Restart)), id Nat]
    (=> (distinct-ids? cs)
        (= (restart-plan s cs id) (model-plan s cs id)))))

(law the-failed-child-is-never-stopped
  (forall [s Strategy, cs (Vec (Tuple Nat Restart)), id Nat]
    (not (some #{id} (second (restart-plan s cs id))))))

(law the-failed-child-is-always-started
  (forall [s Strategy, cs (Vec (Tuple Nat Restart)), i Nat]
    (=> (< i (count cs))
        (some #{(first (nth cs i))} (nth (restart-plan s cs (first (nth cs i))) 2)))))

(law simple-one-for-one-restarts-the-child-alone
  (forall [cs (Vec (Tuple Nat Restart)), id Nat]
    (= (restart-plan :simple-one-for-one cs id) (restart-plan :one-for-one cs id))))

;; --- shutting down on its own -------------------------------------------

(law never-means-no-child-shuts-it-down
  (forall [sig Bool, r Restart, reason Any, left Nat]
    (not (auto-shutdown? :never sig r reason left))))

(law an-insignificant-child-never-shuts-it-down
  (forall [a AutoShutdown, r Restart, reason Any, left Nat]
    (not (auto-shutdown? a false r reason left))))

(law a-child-that-is-restarted-shuts-nothing-down
  (forall [a AutoShutdown, r Restart, reason Any, left Nat]
    (=> (restart? r reason) (not (auto-shutdown? a true r reason left)))))

(law any-significant-shuts-down-when-one-ends
  (forall [r Restart, reason Any, left Nat]
    (=> (not (restart? r reason)) (auto-shutdown? :any-significant true r reason left))))

(law all-significant-shuts-down-when-the-last-ends
  (forall [r Restart, reason Any, left Nat]
    (=> (not (restart? r reason))
        (= (auto-shutdown? :all-significant true r reason left) (= 0 left)))))

(law a-transient-child-ending-normally-shuts-it-down
  (and (auto-shutdown? :any-significant true :transient :normal 3)
       (not (auto-shutdown? :any-significant true :transient :crashed 3))
       (auto-shutdown? :any-significant true :temporary :crashed 3)))

(law stop-order-reverses-start-order
  (forall [ids (Vec Nat)] (= (stop-order ids) (vec (reverse ids)))))

(law stop-order-example (= (stop-order [1 2 3]) [3 2 1]))

;; --- restart intensity --------------------------------------------------

(law intensity-is-the-docs
  (forall [times (Vec Int), now Int, period Nat, max-r Nat]
    (= (intensity times now period max-r) (model-intensity times now period max-r))))

(law one-restart-in-a-quiet-period-is-allowed
  (= (intensity [] 1000 5000 1) [:Allow [1000]]))

(law a-second-restart-inside-the-period-exceeds-intensity-one
  (= (intensity [0] 1000 5000 1) [:Exceed [0 1000]]))

(law old-restarts-fall-out-of-the-period
  (= (intensity [0] 6000 5000 1) [:Allow [6000]]))

(law intensity-zero-allows-no-restart
  (forall [now Int, period Nat] (= (first (intensity [] now period 0)) :Exceed)))

;; --- the supervisor decides through these -------------------------------

(calls sup/restart {:through [ord/restart-plan]})
(calls sup/child-exited {:through [ord/restart? ord/intensity ord/restart-plan ord/auto-shutdown?]})
(calls sup/stop-all {:through [ord/stop-order]})

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
  - More than intensity restarts within period and the supervisor gives up."
  (:require [writ.spec :refer [spec data ann refine graph law calls]]
            [ensemble.order :as ord]
            [ensemble.supervisor :as sup]))

(spec ensemble.order {:require :proved})

(refine Strategy [s Keyword] (contains? #{:one-for-one :one-for-all :rest-for-one} s))
(refine Restart  [r Keyword] (contains? #{:permanent :transient :temporary} r))

(data Plan (Plan (Vec Nat) (Vec Nat)))
(data Verdict (Allow (Vec Int)) (Exceed (Vec Int)))

(ann restart? [Keyword Any -> Bool])
(ann restart-plan [Strategy (Vec (Tuple Nat Restart)) Nat -> Plan])
(ann stop-order [(Vec Nat) -> (Vec Nat)])
(ann intensity [(Vec Int) Int Nat Nat -> Verdict])

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
  "The plan, straight from the docs' wording."
  [strategy cs id]
  (let [ids (mapv first cs)
        i (first (keep-indexed (fn [i x] (when (= x id) i)) ids))]
    (if (nil? i)
      [:Plan [] []]
      (let [hit (case strategy
                  :one-for-one [(nth cs i)]
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

(law an-unknown-child-plans-nothing
  {:require :tested :because "children of unknown length are outside the prover"}
  (forall [s Strategy, cs (Vec (Tuple Nat Restart))]
    (=> (not-any? #(= 99 (first %)) cs)
        (= (restart-plan s cs 99) [:Plan [] []]))))

(law restart-plan-is-the-docs
  {:require :tested :because "children of unknown length are outside the prover"}
  (forall [s Strategy, cs (Vec (Tuple Nat Restart)), id Nat]
    (=> (distinct-ids? cs)
        (= (restart-plan s cs id) (model-plan s cs id)))))

(law the-failed-child-is-never-stopped-and-always-started
  {:require :tested :because "children of unknown length are outside the prover"}
  (forall [s Strategy, cs (Vec (Tuple Nat Restart)), i Nat]
    (=> (and (distinct-ids? cs) (< i (count cs)))
        (let [id (first (nth cs i))
              [_ stop start] (restart-plan s cs id)]
          (and (not (some #{id} stop)) (some #{id} start))))))

(law stop-order-reverses-start-order
  (forall [ids (Vec Nat)] (= (stop-order ids) (vec (reverse ids)))))

(law stop-order-example (= (stop-order [1 2 3]) [3 2 1]))

;; --- restart intensity --------------------------------------------------

(law intensity-is-the-docs
  {:require :tested :because "a vector of unknown length is outside the prover"}
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
(calls sup/child-exited {:through [ord/restart? ord/intensity ord/restart-plan]})
(calls sup/stop-all {:through [ord/stop-order]})

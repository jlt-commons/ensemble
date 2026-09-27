(ns ensemble.order-spec
  "Contract for ensemble.order: which children restart after a failure, in what
  order, and the order the children stop in.

  Children have a start order.  A restart keeps that order, so a restarted
  child sees its predecessors already running.  Stop order is start order
  reversed, so a child stops before the children that depend on it."
  (:require [writ.spec :refer [spec data ann refine graph law]]))

(spec ensemble.order)

;; a decision about the children: restart all of them, restart from an index,
;; restart just one, or the order to stop them in
(data Order (RestartAll) (RestartFrom Nat) (RestartOnly Nat) (StopOrder (Vec Nat)))

;; the strategies a supervisor restarts under -- refined so the generator
;; builds exactly these, not an arbitrary keyword
(refine Strategy [s Keyword]
  (contains? #{:one-for-one :one-for-all :rest-for-one} s))

(ann restart-order [Strategy (Vec Nat) Nat -> Order])
(ann plan-ids [Order (Vec Nat) -> (Vec Nat)])
(ann stop-order [(Vec Nat) -> Order])

;; --- the state graph ----------------------------------------------------

(refine AllChildren [d Order] (= :RestartAll (first d)))
(refine FromChild   [d Order] (= :RestartFrom (first d)))
(refine OnlyChild   [d Order] (= :RestartOnly (first d)))
(refine StopAll     [d Order] (= :StopOrder (first d)))

;; the children in start order yield a restart decision or a stop order; each
;; decision expands, through plan-ids, back to an ordered list of children
(graph order
  {:start  [:ids [1 2 3]]
   :states {:ids (Vec Nat), :all AllChildren, :from FromChild, :only OnlyChild, :stop StopAll}
   :edges  {:ids  {[restart-order Strategy _ Nat] #{:all :from :only}
                   [stop-order] #{:stop}}
            :all  {[plan-ids (Vec Nat)] #{:ids}}
            :from {[plan-ids (Vec Nat)] #{:ids}}
            :only {[plan-ids (Vec Nat)] #{:ids}}}})

;; --- the restart decision -----------------------------------------------

(law one-for-all-restarts-everything
  (= (restart-order :one-for-all [1 2 3] 2) [:RestartAll]))

(law one-for-one-restarts-just-the-failed
  (= (restart-order :one-for-one [1 2 3] 2) [:RestartOnly 2]))

(law rest-for-one-restarts-the-failed-and-later
  (= (restart-order :rest-for-one [1 2 3] 2) [:RestartFrom 1]))

(law an-unknown-id-restarts-alone
  (= (restart-order :rest-for-one [1 2 3] 9) [:RestartOnly 9]))

;; --- the decision expands to the children to restart ---------------------

(law all-children-expand-to-the-start-order
  (= (plan-ids [:RestartAll] [1 2 3]) [1 2 3]))

(law from-the-failed-child-expands-to-the-tail
  (= (plan-ids [:RestartFrom 1] [1 2 3]) [2 3]))

(law just-the-one-child-expands-to-itself
  (= (plan-ids [:RestartOnly 2] [1 2 3]) [2]))

;; --- the stop order -----------------------------------------------------

(law stop-order-reverses-the-start-order
  (= (stop-order [1 2 3]) [:StopOrder [3 2 1]]))

(law stop-order-of-nothing-is-empty
  (= (stop-order []) [:StopOrder []]))

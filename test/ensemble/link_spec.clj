(ns ensemble.link-spec
  "Contract for ensemble.link: the exit-signal semantics of Erlang links as
  pure data.

  A reason is a keyword; :normal is benign, :kill is untrappable, anything else
  is abnormal.  A link edge is undirected.  An abnormal exit kills every
  non-trapping process reachable through links, each re-propagating the same
  reason; a trapping process is informed but lives, and stops propagation."
  (:require [writ.spec :refer [spec data ann law graph]]))

(spec ensemble.link)

(data Action (Ignore) (Deliver Keyword) (Die Keyword))
(data Closure (Closure (Set Nat) (Set Nat)))

(ann abnormal? [Keyword -> Bool])
(ann exit-action [Bool Keyword -> Action])
(ann death-closure [(Vec (Vec Nat)) (Set Nat) Nat Keyword -> Closure])

;; --- the state graph ----------------------------------------------------

;; a reason is classified into a verdict, which decides the action a process
;; takes; the same reason, given a link topology, closes out the pids linked to
;; the one that exited -- the state is the last argument there, marked _
(graph exit
  {:start  [:reason :boom]
   :states {:reason  Keyword
            :verdict Bool
            :action  Action
            :closure Closure}
   :edges  {:reason  {[abnormal?] #{:verdict}
                      [death-closure (Vec (Vec Nat)) (Set Nat) Nat _] #{:closure}}
            :verdict {[exit-action Keyword] #{:action}}}})

(def l12  [[1 2]])
(def l123 [[1 2] [2 3]])

;; --- reason classification ----------------------------------------------

(law normal-is-not-abnormal (not (abnormal? :normal)))
(law boom-is-abnormal (abnormal? :boom))
(law kill-is-abnormal (abnormal? :kill))

;; --- per-process action -------------------------------------------------

(law normal-mortal-is-ignored (= (exit-action false :normal) [:Ignore]))
(law normal-trapper-is-informed (= (exit-action true :normal) [:Deliver :normal]))
(law boom-mortal-dies (= (exit-action false :boom) [:Die :boom]))
(law boom-trapper-is-informed (= (exit-action true :boom) [:Deliver :boom]))
(law kill-mortal-dies (= (exit-action false :kill) [:Die :killed]))
(law kill-trapper-dies-too (= (exit-action true :kill) [:Die :killed]))

;; --- the transitive closure ---------------------------------------------

(law no-links-nothing-happens
  (= (death-closure [] #{} 1 :boom) [:Closure #{} #{}]))

(law normal-leaves-a-mortal-neighbour-alone
  (= (death-closure l12 #{} 1 :normal) [:Closure #{} #{}]))

(law normal-informs-a-trapping-neighbour
  (= (death-closure l12 #{2} 1 :normal) [:Closure #{} #{2}]))

(law boom-kills-a-mortal-neighbour
  (= (death-closure l12 #{} 1 :boom) [:Closure #{2} #{}]))

(law boom-informs-a-trapping-neighbour
  (= (death-closure l12 #{2} 1 :boom) [:Closure #{} #{2}]))

(law boom-propagates-through-a-chain
  (= (death-closure l123 #{} 1 :boom) [:Closure #{2 3} #{}]))

(law a-trapper-stops-propagation
  (= (death-closure l123 #{2} 1 :boom) [:Closure #{} #{2}]))

(law the-origin-never-kills-itself
  (= (death-closure l12 #{} 2 :boom) [:Closure #{1} #{}]))

(law kill-reaches-even-a-trapper
  (= (death-closure l12 #{2} 1 :kill) [:Closure #{2} #{}]))

;; --- the whole domain, so nothing off the examples is left open ---------

(law abnormal-is-exactly-non-normal
  (forall [r Keyword] (= (abnormal? r) (not= :normal r))))

(law exit-action-is-total
  (forall [t Bool, r Keyword]
    (= (exit-action t r)
       (cond (= :kill r)     [:Die :killed]
             t               [:Deliver r]
             (not= :normal r) [:Die r]
             :else           [:Ignore]))))

(defn ref-closure
  "Differential reference for death-closure: the same least fixpoint, written
  as a naive iterate-to-convergence, so the forall law pins the real one over
  every topology and reason."
  [links traps origin reason]
  (let [nbrs (fn [p] (reduce (fn [acc e]
                               (let [[a b] e]
                                 (cond (= p a) (conj acc b)
                                       (= p b) (conj acc a)
                                       :else acc)))
                             [] links))
        step (fn [s]
               (reduce (fn [acc p]
                         (if (or (contains? (:dead acc) p)
                                 (contains? (:informed acc) p))
                           acc
                           (case (cond (= :kill reason)      :Die
                                       (contains? traps p)   :Deliver
                                       (not= :normal reason) :Die
                                       :else                 :Ignore)
                             :Die     (update acc :dead conj p)
                             :Deliver (update acc :informed conj p)
                             acc)))
                       s
                       (mapcat nbrs (:dead s))))
        fixed (loop [s {:dead #{origin} :informed #{}}]
                (let [n (step s)] (if (= n s) s (recur n))))]
    [:Closure (disj (:dead fixed) origin) (:informed fixed)]))

(law death-closure-is-the-fixpoint
  (forall [links (Vec (Vec Nat)), traps (Set Nat), origin Nat, reason Keyword]
    (=> (every? (fn [e] (= 2 (count e))) links)
        (= (death-closure links traps origin reason)
           (ref-closure links traps origin reason)))))

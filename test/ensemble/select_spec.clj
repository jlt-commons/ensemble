(ns ensemble.select-spec
  "Contract for ensemble.select: selective receive over several compiled
  patterns at once.

  The mailbox is scanned oldest message first; the first message that matches
  any clause wins.  The reply carries the index of the clause that matched, the
  message, and the mailbox left behind -- the matched message removed, every
  earlier (skipped) and later message still in place."
  (:require [writ.spec :refer [spec data ann refine graph law]]))

(spec ensemble.select)

(data Mailbox Empty (Msg Any Mailbox))
(data Pattern Wild Nil (Lit Any) (Bind Symbol) (Cons Pattern Pattern))
(data Scan None (Take Nat Any Mailbox))

(ann find-first-of [Mailbox (Vec Pattern) -> Scan])

;; --- the state graph ----------------------------------------------------

(refine Idle    [mb Mailbox] (= :Empty (first mb)))
(refine Pending [mb Mailbox] (= :Msg (first mb)))
(refine Missed  [r Scan] (= :None (first r)))
(refine Taken   [r Scan] (= :Take (first r)))

;; an empty mailbox never gives up a message; a pending one may, whichever
;; clause it matches
(graph receive
  {:states {:idle Idle, :pending Pending, :missed Missed, :taken Taken}
   :edges  {:idle    {[find-first-of (Vec Pattern)] #{:missed}}
            :pending {[find-first-of (Vec Pattern)] #{:missed :taken}}}})

(defn build [vs] (reduce (fn [mb v] [:Msg v mb]) [:Empty] (reverse vs)))

(def wild [:Wild])

(def p-a [:Cons [:Lit :a] [:Cons [:Bind 'x] [:Nil]]])
(def p-b [:Cons [:Lit :b] [:Cons [:Bind 'y] [:Nil]]])

;; --- no match ------------------------------------------------------------

(law none-when-empty
  (forall [ps (Vec Pattern)] (= (find-first-of [:Empty] ps) [:None])))

(law none-when-nothing-matches
  (= (find-first-of (build [[:z 1]]) [p-a]) [:None]))

;; --- the oldest match wins ----------------------------------------------

(law takes-oldest-wildcard
  (forall [v Any, xs (Vec Any)]
    (= (find-first-of (build (into [v] xs)) [wild])
       [:Take 0 v (build xs)])))

(law skips-non-matching-and-keeps-it
  (forall [a Any, b Any, c Any]
    (= (find-first-of (build [a b c]) [wild]) [:Take 0 a (build [b c])])))

(law picks-oldest-tagged
  (= (find-first-of (build [[:a 1] [:b 2]]) [p-a])
     [:Take 0 [:a 1] (build [[:b 2]])]))

(law removes-only-the-first-occurrence
  (= (find-first-of (build [[:a 1] [:a 2] [:a 3]]) [p-a])
     [:Take 0 [:a 1] (build [[:a 2] [:a 3]])]))

;; --- clause order and message order interact ----------------------------

(law clause-order-decides-index
  (= (find-first-of (build [[:b 2]]) [p-a p-b]) [:Take 1 [:b 2] [:Empty]]))

(law oldest-message-wins-over-clause-order
  (= (find-first-of (build [[:b 2] [:a 1]]) [p-a p-b])
     [:Take 1 [:b 2] (build [[:a 1]])]))

(law keeps-ahead-and-behind
  (= (find-first-of (build [[:z 9] [:a 1] [:z 8]]) [p-a])
     [:Take 0 [:a 1] (build [[:z 9] [:z 8]])]))

;; --- a skipped message is never discarded, whatever its value -----------

(law keeps-skipped-nil
  (= (find-first-of (build [nil [:a 1]]) [p-a])
     [:Take 0 [:a 1] (build [nil])]))

(law keeps-nil-between-matches
  (= (find-first-of (build [nil [:a 1] :tail]) [p-a])
     [:Take 0 [:a 1] (build [nil :tail])]))

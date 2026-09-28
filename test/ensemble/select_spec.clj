(ns ensemble.select-spec
  "Contract for ensemble.select: Erlang's selective receive.

  From the Erlang reference manual: the first message in the mailbox is
  matched against the patterns in order; if one matches (and its guard
  holds) the message is removed and that clause is chosen.  Otherwise the
  next message is tried, and so on.  A message no clause takes stays in the
  mailbox, in its place.  A receive that resumes after new messages arrive
  does not re-try the messages it has already tried."
  (:require [writ.spec :refer [spec data ann refine graph law calls]]
            [ensemble.actor :as act]
            [ensemble.match :as match]
            [ensemble.select :as select]))

(spec ensemble.select {:require :proved})

(data Pattern Wild Nil (Lit Any) (Bind Symbol) (Cons Pattern Pattern))
(data Hit (Hit Nat (Map Symbol Any)) (Miss))
(data Scan (Take Nat Nat (Map Symbol Any)) (None Nat))

(ann clause-of [(Vec Pattern) (-> Nat (Map Symbol Any) Bool) Any -> Hit])
(ann scan [(Vec Any) (Vec Pattern) (-> Nat (Map Symbol Any) Bool) Nat -> Scan])
(ann without [(Vec Any) Nat -> (Vec Any)])

;; --- the state graph ----------------------------------------------------

(refine Taken  [r Scan] (= :Take (first r)))
(refine Missed [r Scan] (= :None (first r)))

(defn yes [_ _] true)

(graph receive
  {:states {:mailbox (Vec Any), :taken Taken, :missed Missed}
   :edges  {:mailbox {[scan (Vec Pattern) 'yes Nat] #{:taken :missed}}}
   :tested {:mailbox "a mailbox of unknown length needs induction"}})

;; --- the spec's vocabulary ----------------------------------------------

(def p-a [:Cons [:Lit :a] [:Cons [:Bind 'x] [:Nil]]])
(def p-b [:Cons [:Lit :b] [:Cons [:Bind 'y] [:Nil]]])

(defn big-x
  "A guard: clause 0 takes only an x above 10."
  [k env] (or (not= 0 k) (and (integer? (get env 'x)) (> (get env 'x) 10))))

(defn model-scan
  "The manual's receive, message by message and clause by clause."
  [msgs pats ok? start]
  (or (first (for [i (range start (count msgs))
                   k (range (count pats))
                   :let [env (match/capture (nth pats k) (nth msgs i))]
                   :when (and (some? env) (ok? k env))]
               [:Take i k env]))
      [:None (max start (count msgs))]))

(defn remove-at [v i] (vec (concat (take i v) (drop (inc i) v))))

;; --- which message, which clause ----------------------------------------

(law scan-is-the-manual
  {:require :tested :because "a mailbox of unknown length needs induction, which the prover does not do over scan's keep and range"}
  (forall [msgs (Vec Any), start Nat]
    (and (= (scan msgs [p-a p-b] yes start) (model-scan msgs [p-a p-b] yes start))
         (= (scan msgs [p-a [:Wild]] big-x start) (model-scan msgs [p-a [:Wild]] big-x start)))))

(law the-oldest-message-wins-over-clause-order
  (= (scan [[:b 2] [:a 1]] [p-a p-b] yes 0) [:Take 0 1 {'y 2}]))

(law clause-order-decides-for-one-message
  (= (scan [[:a 1]] [[:Wild] p-a] yes 0) [:Take 0 0 {}]))

(law a-failed-guard-lets-a-later-clause-take-it
  (= (scan [[:a 1]] [p-a [:Bind 'm]] big-x 0) [:Take 0 1 {'m [:a 1]}]))

(law a-failed-guard-skips-the-message
  (= (scan [[:a 1] [:a 20]] [p-a] big-x 0) [:Take 1 0 {'x 20}]))

(law nothing-matches
  {:require :tested :because "a mailbox of unknown length needs induction, which the prover does not do over scan's keep and range"}
  (forall [msgs (Vec Any)]
    (= (scan msgs [[:Lit ::never-sent]] yes 0) [:None (count msgs)])))

(law a-resumed-scan-skips-what-it-tried
  (= (scan [[:a 1] [:a 2]] [p-a] yes 1) [:Take 1 0 {'x 2}]))

(law an-empty-mailbox-yields-nothing
  (forall [start Nat] (= (scan [] [[:Wild]] yes start) [:None start])))

;; --- the rest of the mailbox stays in place ------------------------------

(law without-removes-exactly-one
  {:require :tested :because "a vector of unknown length needs induction over subvec and into"}
  (forall [msgs (Vec Any), i Nat]
    (=> (< i (count msgs))
        (= (without msgs i) (remove-at msgs i)))))

(law clause-of-names-the-first-accepting-clause
  (forall [m Any]
    (= (clause-of [p-a [:Wild]] yes m)
       (if (some? (match/capture p-a m)) [:Hit 0 (match/capture p-a m)] [:Hit 1 {}]))))

(law clause-of-misses
  (= (clause-of [p-a p-b] yes [:c 1]) [:Miss]))

;; --- the runtime's receive goes through the scan -------------------------

(calls act/receive-match {:through [select/scan select/without]})

(ns ensemble.mailbox-spec
  "Contract for ensemble.mailbox: FIFO enqueue, and find-first -- Erlang
  selective receive over the pure queue."
  (:require [writ.spec :refer [spec data ann refine graph law]]))

(spec ensemble.mailbox)

(data Mailbox Empty (Msg Any Mailbox))
(data Scan None (Take Any Mailbox))
(data Pattern Wild Nil (Lit Any) (Bind Symbol) (Cons Pattern Pattern))

(ann enqueue [Mailbox Any -> Mailbox])
(ann dequeue [Mailbox -> Mailbox])
(ann size [Mailbox -> Nat])
(ann msgs [Mailbox -> (List Any)])
(ann scan [Mailbox Pattern -> Scan])
(ann find-first [Mailbox Pattern -> Scan])

;; --- the state graph ----------------------------------------------------

(refine Idle    [mb Mailbox] (= :Empty (first mb)))
(refine Pending [mb Mailbox] (= :Msg (first mb)))
(refine Missed  [r Scan] (= :None (first r)))
(refine Taken   [r Scan] (= :Take (first r)))
(refine Zero    [n Nat] (= 0 n))
(refine Counted [n Nat] (< 0 n))

;; a message makes a mailbox pending, and only a pending mailbox has one to
;; give up: dequeue or a matching receive can empty it again
(graph mailbox
  {:start  [:idle [:Empty]]
   :states {:idle Idle, :pending Pending, :missed Missed, :taken Taken,
            :zero Zero, :counted Counted, :contents (List Any)}
   :edges  {:idle    {[enqueue Any]        #{:pending}
                      [dequeue]            #{:idle}
                      [scan Pattern]       #{:missed}
                      [find-first Pattern] #{:missed}
                      [size]               #{:zero}
                      [msgs]               #{:contents}}
            :pending {[enqueue Any]        #{:pending}
                      [dequeue]            #{:idle :pending}
                      [scan Pattern]       #{:missed :taken}
                      [find-first Pattern] #{:missed :taken}
                      [size]               #{:counted}
                      [msgs]               #{:contents}}}})

(defn build [vs] (reduce (fn [mb v] [:Msg v mb]) [:Empty] (reverse vs)))

;; --- FIFO ---------------------------------------------------------------

(law enqueue-appends
  (forall [xs (Vec Any), v Any]
    (= (msgs (enqueue (build xs) v)) (concat (seq xs) [v]))))

(law build-preserves-order
  (forall [xs (Vec Any)] (= (vec (msgs (build xs))) xs)))

(law size-counts
  (forall [xs (Vec Any)] (= (size (build xs)) (count xs))))

(law dequeue-drops-front
  (forall [a Any, xs (Vec Any)] (= (dequeue (build (into [a] xs))) (build xs))))

(law dequeue-empty (= (dequeue [:Empty]) [:Empty]))

;; --- selective receive --------------------------------------------------

(law find-first-empty
  (forall [p Pattern] (= (find-first [:Empty] p) [:None])))

(law find-first-none-when-no-match
  (forall [xs (Vec Nat)] (= (find-first (build xs) [:Lit :nope]) [:None])))

(law find-first-takes-oldest
  (forall [a Any, xs (Vec Any)]
    (= (find-first (build (into [a] xs)) [:Wild]) [:Take a (build xs)])))

(law find-first-skips-and-keeps
  (forall [a Nat, b Nat, c Nat]
    (= (find-first (build [a b c]) [:Lit b]) [:Take b (build [a c])])))

(law find-first-keeps-tail
  (forall [a Nat, b Nat, c Nat]
    (= (find-first (build [a b c]) [:Lit a]) [:Take a (build [b c])])))

(def tagged [:Cons [:Lit :pair] [:Cons [:Bind 'x] [:Cons [:Bind 'y] [:Nil]]]])

(law find-first-matches-tuple
  (= (find-first (build [[:pair 1 2]]) tagged) [:Take [:pair 1 2] [:Empty]]))

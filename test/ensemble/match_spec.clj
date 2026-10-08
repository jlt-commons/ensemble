(ns ensemble.match-spec
  "The contract for ensemble.match: a compiled receive pattern matched against
  a message either fails (nil) or yields the bindings it captured."
  (:require [writ.spec :refer [spec data ann refine graph law same]]))

(spec ensemble.match {:require :proved})

(data Pattern
  Wild
  Nil
  (Lit Any)
  (Bind Symbol)
  (Cons Pattern Pattern)
  IsMap
  (Has Any Pattern Pattern))

(ann capture [Pattern Any -> Any])

;; --- the state graph ----------------------------------------------------

(refine WildP  [p Pattern] (= :Wild (first p)))
(refine BindP  [p Pattern] (= :Bind (first p)))
(refine LitP   [p Pattern] (= :Lit (first p)))
(refine TupleP [p Pattern] (contains? #{:Nil :Cons} (first p)))
(refine MapP   [p Pattern] (contains? #{:IsMap :Has} (first p)))
(refine Matched [b Any] (map? b))
(refine Outcome [b Any] (or (nil? b) (map? b)))

;; a wildcard or a binder takes any message; a literal or a tuple may also
;; refuse it, with nil -- which ones match is pinned by the laws below
(graph capture
  {:states {:wild WildP, :bind BindP, :lit LitP, :tuple TupleP, :map MapP,
            :matched Matched, :outcome Outcome}
   :edges  {:wild  {[capture Any] #{:matched}}
            :bind  {[capture Any] #{:matched}}
            :lit   {[capture Any] #{:outcome}}
            :tuple {[capture Any] #{:outcome}}
            :map   {[capture Any] #{:outcome}}}
   :tested {:tuple "capture recurses over a Pattern of any depth, a recursive data type the prover does not unfold"
            :map "capture recurses over a Pattern of any depth, a recursive data type the prover does not unfold"}})

(def two-tuple [:Cons [:Wild] [:Nil]])
(def bind-pair [:Cons [:Bind 'l] [:Cons [:Bind 'r] [:Nil]]])
(def tagged-pair [:Cons [:Lit :pair] bind-pair])

(law wildcard-matches-anything
  (forall [m Any] (= (capture [:Wild] m) {})))

(law wildcard-binds-nothing
  (forall [p Pattern, m Any]
    (=> (= p [:Wild]) (= (capture p m) {}))))

(law nil-matches-only-empty
  (forall [xs (Vec Any)] (= (capture [:Nil] xs) (if (empty? xs) {} nil))))

(law lit-matches-itself
  (forall [v Any] (= (capture [:Lit v] v) {})))

(law lit-rejects-other
  (forall [v Any, w Any] (=> (not= v w) (nil? (capture [:Lit v] w)))))

(law bind-captures
  (forall [s Symbol, v Any] (= (capture [:Bind s] v) {s v})))

(law cons-rejects-nonseq
  (forall [n Int] (nil? (capture two-tuple n))))

(law cons-length-is-exact
  (forall [a Any, b Any]
    (and (= (capture bind-pair [a b]) {'l a 'r b})
         (nil? (capture bind-pair [a]))
         (nil? (capture bind-pair [a b []])))))

(law cons-binds-left-to-right
  (forall [a Any, b Any]
    (= (capture tagged-pair [:pair a b]) {'l a 'r b})))

(law cons-tag-must-match
  (forall [a Any, b Any, k Keyword]
    (=> (not= k :pair)
        (nil? (capture tagged-pair [k a b])))))

;; --- a name bound twice must match equal values (Erlang's rule) ----------

(def twice [:Cons [:Bind 'x] [:Cons [:Bind 'x] [:Nil]]])

(law a-repeated-name-matches-equal-elements
  (forall [a Any] (= (capture twice [a a]) {'x a})))

(law a-repeated-name-rejects-different-elements
  (forall [a Any, b Any] (=> (not= a b) (nil? (capture twice [a b])))))

(law distinct-names-bind-independently
  (forall [a Any, b Any] (= (capture bind-pair [a b]) {'l a 'r b})))

;; --- NaN ------------------------------------------------------------------
;; Erlang has no NaN, and Clojure's = says NaN is not NaN.  capture compares
;; as = does but with a NaN the same as a NaN, so a pattern that names a NaN
;; matches one.

(law a-binder-captures-any-value-nan-too
  (forall [s Symbol, v Any!] (same (capture [:Bind s] v) {s v})))

(law a-wildcard-matches-nan (= {} (capture [:Wild] ##NaN)))

(law a-nan-literal-matches-nan (= {} (capture [:Lit ##NaN] ##NaN)))

(law a-nan-literal-matches-nothing-else
  (forall [v Any] (nil? (capture [:Lit ##NaN] v))))

(law a-name-bound-twice-matches-two-nans (same {'x ##NaN} (capture twice [##NaN ##NaN])))

(law a-nan-in-a-tuple-matches-a-nan-there
  (= {} (capture [:Cons [:Lit 1] [:Cons [:Lit ##NaN] [:Nil]]] [1 ##NaN])))

;; --- list tails ------------------------------------------------------------
;; [x & r] compiles to a Cons whose tail is a pattern itself, not Nil: the
;; tail matches whatever is left of the message, as Erlang's [H|T]

(def head-tail [:Cons [:Bind 'h] [:Bind 't]])

(law a-tail-takes-the-rest
  (forall [a Any, b Any, c Any]
    (= (capture head-tail [a b c]) {'h a 't [b c]})))

(law a-tail-may-be-empty
  (forall [a Any] (= (capture head-tail [a]) {'h a 't []})))

(law a-tail-needs-a-head
  (and (nil? (capture head-tail [])) (nil? (capture head-tail 7))))

(law a-tail-can-be-a-tuple
  (forall [a Any, b Any]
    (and (= {} (capture [:Cons [:Lit a] [:Nil]] [a]))
         (= {'h a} (capture [:Cons [:Bind 'h] [:Cons [:Lit b] [:Nil]]] [a b])))))

;; --- maps ----------------------------------------------------------------
;; {:k p} compiles to [:Has :k p [:IsMap]]: the message must be a map
;; holding every key named, each value matching its pattern.  Keys not named
;; are allowed, as in Erlang's #{k := V}.

(law ismap-matches-any-map
  (forall [m (Map Keyword Int)] (= {} (capture [:IsMap] m))))

(law ismap-rejects-a-non-map
  (and (nil? (capture [:IsMap] [1 2])) (nil? (capture [:IsMap] nil)) (nil? (capture [:IsMap] 3))))

(def has-k [:Has :k [:Bind 'v] [:IsMap]])

(law has-binds-the-value
  (forall [n Int, x Int] (= {'v n} (capture has-k {:k n :other x}))))

(law has-needs-the-key
  (forall [n Int] (nil? (capture has-k {:j n}))))

(law has-takes-a-nil-value-that-is-there
  (= {'v nil} (capture has-k {:k nil})))

(law has-value-must-match
  (forall [n Int]
    (and (= {} (capture [:Has :k [:Lit n] [:IsMap]] {:k n}))
         (nil? (capture [:Has :k [:Lit n] [:IsMap]] {:k (inc n)})))))

(law has-chains
  (forall [a Int, b Int]
    (= {'x a 'y b}
       (capture [:Has :a [:Bind 'x] [:Has :b [:Bind 'y] [:IsMap]]] {:a a :b b}))))

(law has-agrees-on-a-repeated-name
  (forall [a Int]
    (and (= {'x a} (capture [:Has :a [:Bind 'x] [:Has :b [:Bind 'x] [:IsMap]]] {:a a :b a}))
         (nil? (capture [:Has :a [:Bind 'x] [:Has :b [:Bind 'x] [:IsMap]]] {:a a :b (inc a)})))))

(law has-rejects-a-non-map
  (forall [n Int] (nil? (capture has-k [:k n]))))

(ns ensemble.pattern-spec
  "Contract for ensemble.pattern: a source pattern form compiles to the
  tagged-data form ensemble.match/capture understands, and bound-syms reports
  the symbols it binds, in source order.

  A pattern form is one of _ (wildcard), a symbol (binds the whole message), a
  vector (a tuple, matched element-wise and right-nested, with & naming a
  pattern for the rest), a map (a map holding the keys named, each value
  matching its pattern) or any other value, a literal that matches itself.  A symbol already bound where the receive
  is written -- one of the pinned names -- is not a binder: it compiles to
  [:Pin sym], which matches the local's value, as Erlang's receive does."
  (:require [writ.spec :refer [spec data ann refine graph law calls]]
            [ensemble.actor]))

(spec ensemble.pattern {:require :proved})

(data Pattern
  Wild
  Nil
  (Lit Any)
  (Bind Symbol)
  (Pin Symbol)
  (Cons Pattern Pattern)
  IsMap
  (Has Any Pattern Pattern))

(ann compile-form [Any (Set Symbol) -> Pattern])
(ann compile-atom [Any (Set Symbol) -> Pattern])
(ann pattern-error [Any (Set Symbol) -> (Opt String)])
(ann atom-binder [Any (Set Symbol) -> (List Symbol)])
(ann steps [Any -> Int])
(ann binders-in [Int Any (Set Symbol) -> (List Symbol)])
(ann compile-in [Int Any (Set Symbol) -> Pattern])
(ann error-in [Int Any (Set Symbol) Bool -> (Opt String)])
(ann bound-syms [Any (Set Symbol) -> (List Symbol)])

;; --- the state graph ----------------------------------------------------

;; the forms a pattern is written as; _ is a single value, pinned by
;; wildcard-is-wild below rather than generated
(refine Name    [s Symbol] (not= s '_))
(refine Literal [f Any] (not (or (symbol? f) (vector? f) (map? f))))
(refine Binder  [f (Vec Symbol)] (boolean (and (some #(not= '_ %) f) (not-any? #{'&} f))))

;; what each compiles to
(refine BindP  [p Pattern] (= :Bind (first p)))
(refine LitP   [p Pattern] (= :Lit (first p)))
(refine TupleP [p Pattern] (contains? #{:Nil :Cons} (first p)))

;; and the names each binds
(refine NoNames  [xs (List Symbol)] (empty? xs))
(refine OneName  [xs (List Symbol)] (= 1 (count xs)))
(refine AnyNames [xs (List Symbol)] (not (empty? xs)))

(def none #{})

(graph compile
  {:states {:name Name, :literal Literal, :tuple-form (Vec Any), :binder Binder,
            :bind BindP, :lit LitP, :tuple TupleP,
            :no-names NoNames, :one-name OneName, :names AnyNames}
   :edges  {:name       {[compile-form 'none] #{:bind},  [bound-syms 'none] #{:one-name}}
            :literal    {[compile-form 'none] #{:lit},   [bound-syms 'none] #{:no-names}}
            :tuple-form {[compile-form 'none] #{:tuple}}
            :binder     {[compile-form 'none] #{:tuple}, [bound-syms 'none] #{:names}}}
   :tested {:tuple-form "a tuple form of unknown length needs induction"
            [:literal compile-form] "the prover does not carry the refinement's (not (map? f)) into compile-form's test"
            :binder "a tuple form of unknown length needs induction"}})

;; --- compile-form: atoms -------------------------------------------------

(law wildcard-is-wild (= (compile-form '_ #{}) [:Wild]))

(law symbol-binds
  (forall [s Symbol] (=> (not= s '_) (= (compile-form s #{}) [:Bind s]))))

(law keyword-is-literal
  (forall [k Keyword] (= (compile-form k #{}) [:Lit k])))

(law string-is-literal
  (forall [s String] (= (compile-form s #{}) [:Lit s])))

(law number-is-literal
  (forall [n Int] (= (compile-form n #{}) [:Lit n])))

(law bool-is-literal
  (forall [b Bool] (= (compile-form b #{}) [:Lit b])))

(law nil-is-literal (= (compile-form nil #{}) [:Lit nil]))

;; --- compile-form: tuples ------------------------------------------------

(law empty-tuple (= (compile-form [] #{}) [:Nil]))

(law unit-tuple (= (compile-form '[a] #{}) [:Cons [:Bind 'a] [:Nil]]))

(law pair-tuple
  (= (compile-form '[a b] #{}) [:Cons [:Bind 'a] [:Cons [:Bind 'b] [:Nil]]]))

(law tuple-keeps-order
  (= (compile-form '[:pair x y] #{})
     [:Cons [:Lit :pair] [:Cons [:Bind 'x] [:Cons [:Bind 'y] [:Nil]]]]))

(law tuple-nests
  (= (compile-form '[:data [:ok v]] #{})
     [:Cons [:Lit :data]
            [:Cons [:Cons [:Lit :ok] [:Cons [:Bind 'v] [:Nil]]] [:Nil]]]))

(law wildcard-element
  (= (compile-form '[_ a] #{}) [:Cons [:Wild] [:Cons [:Bind 'a] [:Nil]]]))

(law literal-element
  (= (compile-form '[1 2] #{}) [:Cons [:Lit 1] [:Cons [:Lit 2] [:Nil]]]))

;; a list or a set is a literal, kept as written, so it matches a message
;; equal to it
(law list-is-literal (= (compile-form '(a b) #{}) [:Lit '(a b)]))

(law literal-inside-tuple-is-kept
  (= (compile-form '[x (f y)] #{})
     [:Cons [:Bind 'x] [:Cons [:Lit '(f y)] [:Nil]]]))

;; --- list tails -------------------------------------------------------------

(law a-tail-after-amp
  (= (compile-form '[h & t] #{}) [:Cons [:Bind 'h] [:Bind 't]]))

(law a-tail-after-several
  (= (compile-form '[:a b & _] #{}) [:Cons [:Lit :a] [:Cons [:Bind 'b] [:Wild]]]))

(law a-tail-can-be-a-tuple
  (= (compile-form '[h & [x]] #{}) [:Cons [:Bind 'h] [:Cons [:Bind 'x] [:Nil]]]))

(law a-tail-binds-in-order (= (vec (bound-syms '[h & t] #{})) '[h t]))

;; a & with nothing before it, or not one pattern after it, is not a pattern
(law a-good-tail-is-no-error (nil? (pattern-error '[h & [x y]] #{})))
(def no-head "& in a pattern needs an element before it")
(def not-one-tail "& in a pattern takes exactly one pattern after it")

(law amp-first-is-an-error (= no-head (pattern-error '[& t] #{})))
(law amp-last-is-an-error (= not-one-tail (pattern-error '[h &] #{})))
(law amp-with-two-after-is-an-error (= not-one-tail (pattern-error '[h & t u] #{})))
(law a-nested-bad-tail-is-an-error (= no-head (pattern-error '[:x [& t]] #{})))
(law a-bad-tail-in-a-map-is-an-error (= no-head (pattern-error '{:k [& t]} #{})))

;; --- maps --------------------------------------------------------------------

(law empty-map-is-any-map (= (compile-form {} #{}) [:IsMap]))

(law a-map-names-a-key
  (= (compile-form '{:k v} #{}) [:Has :k [:Bind 'v] [:IsMap]]))

(law a-map-chains-its-entries
  (= (compile-form '{:a x :b y} #{})
     [:Has :a [:Bind 'x] [:Has :b [:Bind 'y] [:IsMap]]]))

;; a bound name as a key is kept as the name: the receive macro puts in its value
(law a-map-key-may-be-pinned
  (= (compile-form '{k v} '#{k}) [:Has 'k [:Bind 'v] [:IsMap]]))

(law a-map-value-nests
  (= (compile-form '{:r [:ok x]} #{})
     [:Has :r [:Cons [:Lit :ok] [:Cons [:Bind 'x] [:Nil]]] [:IsMap]]))

(law a-map-inside-a-tuple
  (= (compile-form '[:m {:k v}] #{})
     [:Cons [:Lit :m] [:Cons [:Has :k [:Bind 'v] [:IsMap]] [:Nil]]]))

(law a-map-binds-its-values (= (set (bound-syms '{:a x :b [y z]} #{})) '#{x y z}))

(law a-map-key-does-not-bind (= (vec (bound-syms '{k 1} '#{k})) []))

;; --- bound-syms ----------------------------------------------------------

(law bound-wildcard-empty (= (vec (bound-syms '_ #{})) []))

(law bound-symbol
  (forall [s Symbol] (=> (not= s '_) (= (vec (bound-syms s #{})) [s]))))

(law bound-literal-empty
  (forall [k Keyword] (= (vec (bound-syms k #{})) [])))

(law bound-tuple-in-order (= (vec (bound-syms '[a b c] #{})) '[a b c]))

(law bound-tuple-nested-and-skipped
  (= (vec (bound-syms '[a [:tag b] _ c] #{})) '[a b c]))

(law bound-empty-tuple (= (vec (bound-syms '[] #{})) []))

;; --- pinned names ---------------------------------------------------------

(law a-pinned-name-compiles-to-a-pin
  (forall [x Symbol] (=> (not= x '_) (= (compile-form x #{x}) [:Pin x]))))

(law a-pinned-name-inside-a-tuple
  (= (compile-form '[ref reply] '#{ref})
     [:Cons [:Pin 'ref] [:Cons [:Bind 'reply] [:Nil]]]))

(law a-pinned-name-is-not-bound
  (= (vec (bound-syms '[ref reply ref] '#{ref})) '[reply]))

(law an-unpinned-name-still-binds
  (forall [x Symbol, y Symbol]
    (=> (and (not= x y) (not= x '_))
        (= (compile-form x #{y}) [:Bind x]))))

(law a-repeated-binder-is-named-once
  (= (vec (bound-syms '[a a b] #{})) '[a b]))

(law an-unbound-key-is-an-error
  (= "a map pattern's key must be a value or a bound name, not k" (pattern-error '{k v} #{})))
(law a-bound-key-is-no-error (nil? (pattern-error '{k v} '#{k})))
(law a-literal-key-is-no-error (nil? (pattern-error '{:k v "s" [a & b]} #{})))
(law an-atom-is-no-error
  (forall [x Any] (=> (not (or (vector? x) (map? x))) (nil? (pattern-error x #{})))))

;; --- the receive macro compiles its patterns here ------------------------

(calls ensemble.actor/receive {:through [ensemble.pattern/pattern-error ensemble.pattern/compile-form
                                          ensemble.pattern/bound-syms]})

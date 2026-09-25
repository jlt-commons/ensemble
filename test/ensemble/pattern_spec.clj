(ns ensemble.pattern-spec
  "Contract for ensemble.pattern: a source pattern form compiles to the
  tagged-data form ensemble.match/capture understands, and bound-syms reports
  the symbols it binds, in source order.

  A pattern form is one of _ (wildcard), a symbol (binds the whole message), a
  non-vector literal (matches itself), or a vector (a tuple, matched
  element-wise and right-nested)."
  (:require [writ.spec :refer [spec data ann law]]))

(spec ensemble.pattern)

(data Pattern
  Wild
  Nil
  (Lit Any)
  (Bind Symbol)
  (Cons Pattern Pattern))

(ann compile-form [Any -> Pattern])
(ann bound-syms [Any -> (List Symbol)])

;; --- compile-form: atoms -------------------------------------------------

(law wildcard-is-wild (= (compile-form '_) [:Wild]))

(law symbol-binds
  (forall [s Symbol] (=> (not= s '_) (= (compile-form s) [:Bind s]))))

(law keyword-is-literal
  (forall [k Keyword] (= (compile-form k) [:Lit k])))

(law string-is-literal
  (forall [s String] (= (compile-form s) [:Lit s])))

(law number-is-literal
  (forall [n Int] (= (compile-form n) [:Lit n])))

(law bool-is-literal
  (forall [b Bool] (= (compile-form b) [:Lit b])))

(law nil-is-literal (= (compile-form nil) [:Lit nil]))

;; --- compile-form: tuples ------------------------------------------------

(law empty-tuple (= (compile-form []) [:Nil]))

(law unit-tuple (= (compile-form '[a]) [:Cons [:Bind 'a] [:Nil]]))

(law pair-tuple
  (= (compile-form '[a b]) [:Cons [:Bind 'a] [:Cons [:Bind 'b] [:Nil]]]))

(law tuple-keeps-order
  (= (compile-form '[:pair x y])
     [:Cons [:Lit :pair] [:Cons [:Bind 'x] [:Cons [:Bind 'y] [:Nil]]]]))

(law tuple-nests
  (= (compile-form '[:data [:ok v]])
     [:Cons [:Lit :data]
            [:Cons [:Cons [:Lit :ok] [:Cons [:Bind 'v] [:Nil]]] [:Nil]]]))

(law wildcard-element
  (= (compile-form '[_ a]) [:Cons [:Wild] [:Cons [:Bind 'a] [:Nil]]]))

(law literal-element
  (= (compile-form '[1 2]) [:Cons [:Lit 1] [:Cons [:Lit 2] [:Nil]]]))

;; --- bound-syms ----------------------------------------------------------

(law bound-wildcard-empty (= (vec (bound-syms '_)) []))

(law bound-symbol
  (forall [s Symbol] (=> (not= s '_) (= (vec (bound-syms s)) [s]))))

(law bound-literal-empty
  (forall [k Keyword] (= (vec (bound-syms k)) [])))

(law bound-tuple-in-order (= (vec (bound-syms '[a b c])) '[a b c]))

(law bound-tuple-nested-and-skipped
  (= (vec (bound-syms '[a [:tag b] _ c])) '[a b c]))

(law bound-empty-tuple (= (vec (bound-syms '[])) []))

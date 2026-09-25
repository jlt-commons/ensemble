(ns ensemble.pattern
  "Compile a receive pattern as written in source into the tagged-data form
  ensemble.match/capture understands.

  A pattern form is one of:

      _              wildcard, binds nothing
      sym            binds the whole message to sym
      :kw 1 \"s\" nil  a literal: matches a message equal to it
      [p1 p2 ...]    a tuple: matches a sequential message element-wise, so
                     [:pair x y] matches [:pair 1 2] binding x 1, y 2

  Vectors nest, so [x [:tag y]] is a tuple whose second element is itself a
  tuple.  Any other value, a map or a list among them, is a literal as
  written.")

(defn- compile-atom
  "A pattern form that is not a tuple."
  [p]
  (cond
    (= p '_) [:Wild]
    (symbol? p) [:Bind p]
    :else [:Lit p]))

(defn- compile-tuple
  "The elements of a tuple pattern, compiled and right-nested."
  [ps]
  (if (empty? ps)
    [:Nil]
    (let [p (first ps)]
      [:Cons (if (vector? p) (compile-tuple p) (compile-atom p))
             (compile-tuple (rest ps))])))

(defn compile-form
  "Compile a pattern form into the tagged-data form capture matches."
  [pat]
  (if (vector? pat) (compile-tuple pat) (compile-atom pat)))

(defn bound-syms
  "The symbols a pattern binds, in order."
  [pat]
  (filter (fn [x] (and (symbol? x) (not= x '_)))
          (tree-seq vector? seq pat)))

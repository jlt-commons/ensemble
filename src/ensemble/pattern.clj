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
  tuple.")

(defn compile-form
  "Compile a pattern form into the tagged-data form capture matches."
  [pat]
  (cond
    (= pat '_) [:Wild]
    (symbol? pat) [:Bind pat]
    (vector? pat) (reduce (fn [acc p] [:Cons (compile-form p) acc])
                          [:Nil]
                          (reverse pat))
    :else [:Lit pat]))

(defn bound-syms
  "The symbols a pattern binds, in order."
  [pat]
  (cond
    (symbol? pat) (if (= pat '_) [] [pat])
    (vector? pat) (mapcat bound-syms pat)
    :else []))

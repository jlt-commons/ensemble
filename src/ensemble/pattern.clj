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
  tuple."
  (:require [clojure.walk :as walk]))

(defn compile-form
  "Compile a pattern form into the tagged-data form capture matches."
  [pat]
  (walk/postwalk
   (fn [p]
     (cond
       (= p '_) [:Wild]
       (symbol? p) [:Bind p]
       (vector? p) (reduce (fn [acc e] [:Cons e acc]) [:Nil] (reverse p))
       :else [:Lit p]))
   pat))

(defn bound-syms
  "The symbols a pattern binds, in order."
  [pat]
  (filter (fn [x] (and (symbol? x) (not= x '_)))
          (tree-seq vector? seq pat)))

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
  written.

  As in Erlang, a name already bound where the receive is written is not a
  binder: it matches the value it is bound to.  compile-form takes the set of
  such names and compiles each to [:Pin sym], which the receive macro turns
  into a literal of the local's value.  So

      (let [ref (make-ref)] (receive [[ref reply] reply]))

  takes only the message whose first element is that ref.")

(defn- compile-atom
  "A pattern form that is not a tuple."
  [p pinned]
  (cond
    (= p '_) [:Wild]
    (and (symbol? p) (contains? pinned p)) [:Pin p]
    (symbol? p) [:Bind p]
    :else [:Lit p]))

(defn- compile-tuple
  "The elements of a tuple pattern, compiled and right-nested."
  [ps pinned]
  (if (empty? ps)
    [:Nil]
    (let [p (first ps)]
      [:Cons (if (vector? p) (compile-tuple p pinned) (compile-atom p pinned))
             (compile-tuple (rest ps) pinned)])))

(defn compile-form
  "Compile a pattern form into the tagged-data form capture matches, with the
  names in pinned matched by value rather than bound."
  [pat pinned]
  (if (vector? pat) (compile-tuple pat pinned) (compile-atom pat pinned)))

(defn- atom-binder
  "The binder a pattern form that is not a tuple introduces, if any."
  [p pinned]
  (if (and (symbol? p) (not= p '_) (not (contains? pinned p))) [p] []))

(defn- tuple-binders
  "The binders of a tuple pattern's elements, in order, repeats included."
  [ps pinned]
  (if (empty? ps)
    []
    (let [p (first ps)]
      (concat (if (vector? p) (tuple-binders p pinned) (atom-binder p pinned))
              (tuple-binders (rest ps) pinned)))))

(defn bound-syms
  "The symbols a pattern binds, in order, leaving out the pinned ones."
  [pat pinned]
  (distinct (if (vector? pat) (tuple-binders pat pinned) (atom-binder pat pinned))))

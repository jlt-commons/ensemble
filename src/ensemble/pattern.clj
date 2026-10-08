(ns ensemble.pattern
  "Compile a receive pattern as written in source into the tagged-data form
  ensemble.match/capture understands.

  A pattern form is one of:

      _              wildcard, binds nothing
      sym            binds the whole message to sym
      :kw 1 \"s\" nil  a literal: matches a message equal to it
      [p1 p2 ...]    a tuple: matches a sequential message element-wise, so
                     [:pair x y] matches [:pair 1 2] binding x 1, y 2
      [p1 & pt]      a list tail, Erlang's [H|T]: p1 matches the first
                     element and pt the rest, so [:args h & t] matches
                     [:args 1 2 3] binding h 1, t (2 3)
      {k p ...}      a map holding at least the keys named, each value
                     matching its pattern, as Erlang's #{k := P}.  A key is
                     a value or a name bound where the receive is written;
                     it never binds

  Patterns nest, so [x [:tag y]] is a tuple whose second element is itself a
  tuple, and {:at [x y]} a map whose :at is a pair.  Any other value, a list
  or a set among them, is a literal as written.

  A tuple matches any sequential message, a list as well as a vector: a
  Clojure program rarely tells them apart, and Erlang's tuple/list split has
  no counterpart here.

  A map's key is compiled as written, [:Has k p q]: a symbol there can only
  be a bound name (pattern-error refuses any other), and the receive macro
  puts in its value.

  As in Erlang, a name already bound where the receive is written is not a
  binder: it matches the value it is bound to.  compile-form takes the set of
  such names and compiles each to [:Pin sym], which the receive macro turns
  into a literal of the local's value.  So

      (let [ref (make-ref)] (receive [[ref reply] reply]))

  takes only the message whose first element is that ref.")

(defn- compile-atom
  "A pattern form that is neither a tuple nor a map."
  [p pinned]
  (cond
    (= p '_) [:Wild]
    (contains? pinned p) [:Pin p]
    (symbol? p) [:Bind p]
    :else [:Lit p]))

(defn- steps
  "How many steps a walk over pat may take: it counts down one per element
  of a tuple, entry of a map and level of nesting, so that writ can see the
  walk ends.  Twice the forms in pat, and one, is more than any walk below
  takes."
  [pat]
  (inc (* 2 (count (tree-seq coll? seq pat)))))

(defn- error-in
  "pattern-error, with n steps left; lead? is false on the rest of a tuple,
  after its first element."
  [n pat pinned lead?]
  (cond
    ;; never so: steps leaves more than enough
    (not (pos? n)) nil
    (vector? pat)
    (cond
      (empty? pat) nil
      (= '& (first pat)) (cond
                           lead? "& in a pattern needs an element before it"
                           (not= 2 (count pat)) "& in a pattern takes exactly one pattern after it"
                           :else (error-in (dec n) (second pat) pinned true))
      :else (or (error-in (dec n) (first pat) pinned true)
                (error-in (dec n) (vec (rest pat)) pinned false)))
    (map? pat)
    (if (empty? pat)
      nil
      (let [[k v] (first pat)]
        (if (and (symbol? k) (not (contains? pinned k)))
          (str "a map pattern's key must be a value or a bound name, not " k)
          (or (error-in (dec n) v pinned true)
              (error-in (dec n) (dissoc pat k) pinned true)))))
    :else nil))

(defn pattern-error
  "What is wrong with pattern form pat, as a message, or nil when it is a
  pattern.  A & needs an element before it and takes exactly one pattern
  after it; a map's key that is an unbound name could only bind, and a key
  never binds."
  [pat pinned]
  (if (or (vector? pat) (map? pat)) (error-in (steps pat) pat pinned true) nil))

(defn- compile-in
  "compile-form, with n steps left."
  [n pat pinned]
  (cond
    (not (pos? n)) [:Wild]
    (vector? pat) (cond
                    (empty? pat) [:Nil]
                    (= '& (first pat)) (compile-in (dec n) (second pat) pinned)
                    :else [:Cons (compile-in (dec n) (first pat) pinned)
                                 (compile-in (dec n) (vec (rest pat)) pinned)])
    (map? pat) (if (empty? pat)
                 [:IsMap]
                 (let [[k v] (first pat)]
                   [:Has k (compile-in (dec n) v pinned)
                         (compile-in (dec n) (dissoc pat k) pinned)]))
    :else (compile-atom pat pinned)))

(defn compile-form
  "Compile a pattern form into the tagged-data form capture matches, with the
  names in pinned matched by value rather than bound.  A tuple's elements
  are right-nested, the one pattern after & standing for the rest; a map's
  entries likewise, ending in a test that the message is a map."
  [pat pinned]
  (if (or (vector? pat) (map? pat)) (compile-in (steps pat) pat pinned) (compile-atom pat pinned)))

(defn- atom-binder
  "The binder a pattern form that is neither a tuple nor a map introduces,
  if any."
  [p pinned]
  (if (and (symbol? p) (not= p '_) (not (contains? pinned p))) [p] []))

(defn- binders-in
  "The binders of a pattern form, in order, repeats included, with n steps
  left."
  [n p pinned]
  (cond
    (not (pos? n)) []
    (vector? p) (cond
                  (empty? p) []
                  (= '& (first p)) (binders-in (dec n) (second p) pinned)
                  :else (concat (binders-in (dec n) (first p) pinned)
                                (binders-in (dec n) (vec (rest p)) pinned)))
    (map? p) (if (empty? p)
               []
               (let [[k v] (first p)]
                 (concat (binders-in (dec n) v pinned)
                         (binders-in (dec n) (dissoc p k) pinned))))
    :else (atom-binder p pinned)))

(defn bound-syms
  "The symbols a pattern binds, in order, leaving out the pinned ones."
  [pat pinned]
  (distinct (if (or (vector? pat) (map? pat)) (binders-in (steps pat) pat pinned) (atom-binder pat pinned))))

(ns ensemble.match
  "Structural matcher for receive patterns.

  A compiled pattern is tagged data.  A tuple pattern is right-nested, so the
  pattern [x [:tag y]] compiles to

      [:Cons [:Bind x]
             [:Cons [:Cons [:Lit :tag] [:Cons [:Bind y] [:Nil]]]
                    [:Nil]]]

  and matching walks the pattern and the message together, one element of each
  at a time.  A compiled pattern is one of:

      [:Wild]        matches anything, binds nothing
      [:Nil]         matches an empty sequential message
      [:Lit v]       matches a message equal to v
      [:Bind s]      matches anything, binds s to the message
      [:Cons p q]    matches a non-empty sequential message whose first
                     element matches p and rest matches q
      [:IsMap]       matches any map
      [:Has k p q]   matches a map holding key k whose value there matches
                     p, and which itself matches q

  As in Erlang, a name bound twice in one pattern must match equal values:
  [x x] matches [1 1] but not [1 2].

  Values are compared as = does, except that a NaN matches a NaN: a
  literal NaN pattern takes a NaN message, and [x x] matches [NaN NaN].
  Erlang has no NaN; Clojure's = says NaN is not NaN, which would make a
  pattern that names one match nothing.

  capture returns the bindings it captured as a map, or nil when the pattern
  does not match.")

(defn- same?
  "= with a NaN the same as a NaN.  Only a NaN is a number not = to itself.
  A NaN inside a collection compares as = does: a tuple pattern takes a
  message apart element by element, so each NaN it names meets one here."
  [a b]
  (or (= a b)
      ;; the NaN test first: on any other value it settles this at once
      (and (not= a a) (not= b b) (number? a) (number? b))))

(defn- agree
  "b and c merged, or nil when they bind a name to different values."
  [b c]
  (when (every? (fn [k] (or (not (contains? b k)) (same? (get b k) (get c k))))
                (keys c))
    (merge b c)))

(defn capture
  "Match compiled pattern p against message m.  Return a bindings map (empty
  when the pattern captures nothing) or nil."
  [p m]
  (case (first p)
    :Wild {}
    :Nil  (if (and (sequential? m) (empty? m)) {} nil)
    :Lit  (if (same? (nth p 1) m) {} nil)
    :Bind {(nth p 1) m}
    :Cons (let [[_ hd tl] p]
            (if (and (sequential? m) (seq m))
              ;; the tail is only tried once the head matches
              (let [b (capture hd (first m))]
                (if (nil? b)
                  nil
                  (let [c (capture tl (rest m))]
                    (if (nil? c) nil (agree b c)))))
              nil))
    :IsMap (if (map? m) {} nil)
    :Has (let [[_ k vp q] p]
           (if (and (map? m) (contains? m k))
             (let [b (capture vp (get m k))]
               (if (nil? b)
                 nil
                 (let [c (capture q m)]
                   (if (nil? c) nil (agree b c)))))
             nil))))

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

  capture returns the bindings it captured as a map, or nil when the pattern
  does not match.")

(defn capture
  "Match compiled pattern p against message m.  Return a bindings map (empty
  when the pattern captures nothing) or nil."
  [p m]
  (case (first p)
    :Wild {}
    :Nil  (if (and (sequential? m) (empty? m)) {} nil)
    :Lit  (if (= (nth p 1) m) {} nil)
    :Bind {(nth p 1) m}
    :Cons (let [[_ hd tl] p]
            (if (and (sequential? m) (seq m))
              (let [b (capture hd (first m))
                    c (capture tl (rest m))]
                (if (or (nil? b) (nil? c))
                  nil
                  (merge b c)))
              nil))))

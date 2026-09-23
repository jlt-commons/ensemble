(ns ensemble.select
  "Selective receive over several patterns at once.

  An actor's receive has one clause per pattern and the mailbox is scanned
  oldest first; the first message that matches any clause wins, with the index
  of the clause that matched.  This is ensemble.mailbox/find-first lifted from
  one pattern to a vector of them, and it keeps the same guarantee: every
  message ahead of the match, and every message behind it, stays in the
  mailbox."
  (:require [ensemble.match :as match]
            [ensemble.mailbox :as mb]))

(defn- idx-match
  "Index in pats of the first pattern msg matches, or nil."
  [pats msg]
  (loop [i 0 ps pats]
    (if (seq ps)
      (if (nil? (match/capture (first ps) msg))
        (recur (inc i) (rest ps))
        i)
      nil)))

(defn find-first-of
  "[:None], or [:Take pat-idx msg rest] with rest the mailbox minus msg."
  [mbx pats]
  (let [xs (vec (mb/msgs mbx))
        n (count xs)]
    (loop [i 0]
      (if (< i n)
        (let [m (nth xs i)
              k (idx-match pats m)]
          (if (nil? k)
            (recur (inc i))
            [:Take k m (reduce mb/enqueue [:Empty]
                              (concat (subvec xs 0 i) (subvec xs (inc i))))]))
        [:None]))))

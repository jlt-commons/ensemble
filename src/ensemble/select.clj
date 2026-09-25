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
  (first (keep-indexed
          (fn [i p]
            (let [b (match/capture p msg)]
              (if (nil? b) nil i)))
          pats)))

(defn find-first-of
  "[:None], or [:Take pat-idx msg rest] with rest the mailbox minus msg."
  [mbx pats]
  (let [xs  (vec (mb/msgs mbx))
        hit (first (keep-indexed
                    (fn [i m]
                      (let [k (idx-match pats m)]
                        (if (nil? k) nil [k i])))
                    xs))]
    (if hit
      (let [[k i] hit]
        [:Take k (nth xs i)
         (reduce mb/enqueue [:Empty]
                 (concat (subvec xs 0 i) (subvec xs (inc i))))])
      [:None])))

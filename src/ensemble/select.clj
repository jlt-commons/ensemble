(ns ensemble.select
  "Selective receive over a mailbox, as pure data.

  A mailbox is a vector of messages, oldest first.  A receive has one clause
  per compiled pattern, each with an optional guard.  The scan walks the
  mailbox oldest first; for each message it tries the clauses in order, and
  the first clause whose pattern matches and whose guard accepts the bindings
  wins.  The matched message is removed and every other message, ahead of it or
  behind it, stays where it was.  That is Erlang's receive: a message no clause
  wants is stepped over, never discarded.

  A guard is the fn ok?, called as (ok? clause-index bindings); a clause with
  no guard is one whose ok? answers true.

  The scan can resume: [:None n] says the first n messages were tried and none
  matched, so after new messages arrive a waiting receive scans from n rather
  than from the start (Erlang's receive does the same)."
  (:require [ensemble.match :as match]))

(defn clause-of
  "The first clause msg satisfies: [:Hit k env] with k the clause index and env
  its bindings, or [:Miss]."
  [pats ok? msg]
  (loop [ps (seq pats), k 0]
    (if ps
      (let [env (match/capture (first ps) msg)]
        (if (and (some? env) (ok? k env))
          [:Hit k env]
          (recur (next ps) (inc k))))
      [:Miss])))

(defn scan
  "Scan msgs from index start.  [:Take i k env]: message i is the first at or
  after start that a clause takes, clause k, binding env.  [:None n]: none
  does, n being the count of messages scanned up to."
  [msgs pats ok? start]
  (let [n (count msgs)]
    (loop [i start]
      (if (< i n)
        (let [r (clause-of pats ok? (nth msgs i))]
          (case (first r)
            :Hit (let [[_ k env] r] [:Take i k env])
            :Miss (recur (inc i))))
        [:None (max start n)]))))

(defn without
  "msgs with the message at index i removed, the rest in order.  Taking the
  oldest, the usual case, costs nothing: it is a subvec."
  [msgs i]
  (if (zero? i)
    (subvec msgs 1)
    (into (subvec msgs 0 i) (subvec msgs (inc i)))))

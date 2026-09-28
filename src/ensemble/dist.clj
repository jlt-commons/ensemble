(ns ensemble.dist
  "The decisions of Erlang distribution, as pure functions: where a send
  goes, what a pid inside a message is on arrival, and what a lost
  connection does to the links and monitors that cross it.  ensemble.node
  makes every one of them through these.

  A pid on the wire is [:Pid node id]; a destination is a pid, [:Name nm]
  or [:At nm node], Erlang's {Name, Node}."
  (:require [clojure.walk :as walk]))

(defn route
  "Where a send to dest goes from node self: [:Here id], [:There node id],
  [:Named nm] or [:NamedThere node nm]."
  [self dest]
  (case (first dest)
    :Pid (let [[_ n id] dest] (if (= n self) [:Here id] [:There n id]))
    :Name (let [[_ nm] dest] [:Named nm])
    :At (let [[_ nm n] dest] (if (= n self) [:Named nm] [:NamedThere n nm]))))

(defn- own-pid? [self x]
  (and (vector? x) (= 3 (count x)) (= :Pid (first x)) (= self (second x))))

(defn arrive
  "A message as node self receives it: each [:Pid self id] inside -- in a
  vector, a map, a set, at any depth -- is this node's own process again,
  [:Local id]; every other value is as it came."
  [self v]
  (walk/postwalk (fn [x] (if (own-pid? self x) [:Local (nth x 2)] x)) v))

(defn on-nodedown
  "What losing node n does here, given the links and monitors from local
  processes to processes elsewhere and the node monitors: each link to a
  process of n is broken with :noconnection, each monitor of one gets a
  DOWN with :noconnection, each monitor of n gets [:nodedown n].  In that
  order; entries for other nodes do nothing."
  [n links mons nmons]
  (vec (concat
         (keep (fn [l] (let [[_ me pn pid] l]
                         (when (= pn n) [:Signal me [:Pid pn pid] :noconnection])))
               links)
         (keep (fn [m] (let [[_ me ref pn pid] m]
                         (when (= pn n) [:Deliver me [:DOWN ref :process [:Pid pn pid] :noconnection]])))
               mons)
         (keep (fn [x] (let [[_ me mn] x]
                         (when (= mn n) [:Deliver me [:nodedown mn]])))
               nmons))))

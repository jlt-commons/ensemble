(ns ensemble.order
  "The pure decisions of supervision.  The supervisor makes every restart
  decision here, so these are the rules that run.

  Children have a start order, and a child is [id restart-type], the type one
  of :permanent, :transient or :temporary.

  restart? decides whether a child's exit earns a restart at all.  If it does,
  restart-plan says what the strategy does about it: which running children
  are stopped first (in stop order, start order reversed, so a child stops
  before the children it depends on) and which are then started (in start
  order, so a started child sees its predecessors running).  A :temporary
  child the strategy stops is not started again.  intensity decides whether
  the restart is allowed, or whether too many restarts inside the period mean
  the supervisor must give up."
  (:require [ensemble.signal :as sig]))

(defn restart?
  "Does a child of restart type exiting with reason earn a restart?
  :permanent always, :temporary never, :transient only when the reason is not
  an orderly stop (:normal, :shutdown, [:shutdown term])."
  [restart reason]
  (cond
    (= :temporary restart) false
    (= :transient restart) (not (sig/shutdown? reason))
    :else true))

(defn- index-of
  [ids id]
  (first (keep-indexed (fn [i x] (when (= x id) i)) ids)))

(defn- affected
  "The children the strategy touches when id fails, in start order."
  [strategy children id]
  (let [i (index-of (mapv first children) id)]
    (cond
      (nil? i) []
      (= :one-for-all strategy) (vec children)
      (= :rest-for-one strategy) (vec (subvec (vec children) i))
      :else [(nth children i)])))

(defn restart-plan
  "The plan for restarting id, which has exited, under strategy:
  [:Plan stop start].  stop is the other affected children in stop order;
  start is the affected children to start again, in start order -- id itself
  and every stopped child that is not :temporary.  An id that is not a child
  plans nothing."
  [strategy children id]
  (let [hit (affected strategy children id)]
    [:Plan
     (vec (reverse (keep (fn [c] (when (not= id (first c)) (first c))) hit)))
     (vec (keep (fn [c] (when (or (= id (first c)) (not= :temporary (second c)))
                          (first c)))
                hit))]))

(defn stop-order
  "The ids in the order the children stop: start order reversed."
  [ids]
  (vec (reverse ids)))

(defn intensity
  "Record a restart at time now (ms) against the restart times so far.  At most
  max-r restarts may fall within period-ms; one more and the supervisor must
  shut down.  [:Allow times] with times the restarts still inside the period,
  now included, or [:Exceed times]."
  [times now period-ms max-r]
  (let [recent (vec (concat (filter (fn [t] (<= (- now t) period-ms)) times) [now]))]
    (if (> (count recent) max-r)
      [:Exceed recent]
      [:Allow recent])))

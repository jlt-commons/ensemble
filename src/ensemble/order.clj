(ns ensemble.order
  "Pure ordering decisions for supervision: which children restart after a
  failure, in what order, and the order the children stop in.

  restart-order names the children to restart; plan-ids turns that decision
  into the ordered ids.  Restarts follow start order, so a restarted child
  sees its predecessors already running.  Stop order is start order reversed,
  so a child stops before the children that depend on it.")

(defn- index-of
  [ids id]
  (first (keep-indexed (fn [i x] (when (= x id) i)) ids)))

(defn restart-order
  "The decision for a failure of id under strategy:
    [:RestartAll]      restart every child, in start order
    [:RestartFrom i]   restart the child at index i and every child after it
    [:RestartOnly id]  restart just the child id
  An unknown strategy, or an id not among ids, restarts id alone."
  [strategy ids id]
  (cond
    (= strategy :one-for-all)  [:RestartAll]
    (= strategy :rest-for-one) (if-let [i (index-of ids id)]
                                 [:RestartFrom i]
                                 [:RestartOnly id])
    :else                      [:RestartOnly id]))

(defn plan-ids
  "The ids a restart decision restarts, in start order."
  [decision ids]
  (case (first decision)
    :RestartAll  (vec ids)
    :RestartFrom (vec (subvec (vec ids) (nth decision 1)))
    :RestartOnly [(nth decision 1)]
    :StopOrder   (nth decision 1)))

(defn stop-order
  "The ids in the order the children stop: start order reversed."
  [ids]
  [:StopOrder (vec (reverse ids))])

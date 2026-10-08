(ns ensemble.pg
  "Process groups over every connected node, as OTP's pg: a group is a name
  with processes in it, a process joins from its own node, and every node
  learns of it.

  Each node runs a scope process, which keeps the groups its own processes
  joined and what each peer has told it of theirs.  A join or a leave is
  sent to every peer; when two nodes connect, each sends the other its own
  groups; when a connection is lost, the peer's members are dropped.  A
  process that exits leaves all its groups.  Membership is eventually
  consistent, as pg's is: a member just joined on one node shows on the
  others a moment later.

  A process may join a group more than once, and is then a member as many
  times, until it leaves as often."
  (:require [ensemble.actor :as act]
            [ensemble.gen-server :as gs]
            [ensemble.node :as node]
            [ensemble.process :as proc]))

(defonce ^:private views
  ;; node -> {group [member ...]}: what each node's scope knows, local
  ;; members first, written only by the scope
  (atom {}))

(defn- view
  "The members of each group as st holds them."
  [st]
  (let [groups (into (set (keys (:local st))) (mapcat keys (vals (:remote st))))]
    (into {}
          (for [g groups
                :let [ms (into (vec (get-in st [:local g])) (mapcat #(get % g) (vals (:remote st))))]
                :when (seq ms)]
            [g ms]))))

(defn- publish! [st] (swap! views assoc (act/node) (view st)) st)

(defn- notify!
  "Tell the monitors of group g that pids joined or left it (kind :join or
  :leave)."
  [st kind g pids]
  (when (seq pids)
    (doseq [[ref w] (get-in st [:watchers g])] (act/! w [ref kind g (vec pids)])))
  st)

(defn- removed
  "The pids, from members, that are in xs; each as often as it is in xs and
  no more often than in members."
  [members xs]
  (loop [ms members, xs xs, out []]
    (if-let [x (first xs)]
      (let [i (.indexOf ^java.util.List ms x)]
        (if (neg? i)
          (recur ms (rest xs) out)
          (recur (into (subvec ms 0 i) (subvec ms (inc i))) (rest xs) (conj out x))))
      [ms out])))

(defn- broadcast!
  "Cast msg to the scope on every connected node; one that has gone down
  meanwhile is not connected to again for it."
  [msg]
  (node/without-connecting
    (doseq [n (act/nodes)] (gs/cast! [:At ::scope n] msg))))

(defn- leave-local
  "st with pids leaving local group g, told to monitors and peers."
  [st g pids]
  (let [[ms gone] (removed (vec (get-in st [:local g])) pids)]
    (when (seq gone) (broadcast! [:leave (act/node) g gone]))
    (-> st
        (assoc-in [:local g] ms)
        (notify! :leave g gone))))

(defn- set-remote
  "st with peer's groups replaced by groups, the change told to monitors."
  [st peer groups]
  (let [old (get-in st [:remote peer] {})
        st (if (seq groups) (assoc-in st [:remote peer] groups) (update st :remote dissoc peer))]
    (reduce (fn [s g]
              (let [before (get old g []) after (get groups g [])]
                ;; what is left of before once after's are taken out left,
                ;; and what is left of after once before's are, joined
                (-> s
                    (notify! :leave g (first (removed before after)))
                    (notify! :join g (first (removed after before))))))
            st
            (into (set (keys old)) (keys groups)))))

(defrecord Scope []
  gs/Server
  (init [_]
    (node/monitor-nodes! true)
    [:ok (publish! {:local {} :remote {} :mons {} :watchers {}})])
  (handle-call [_ req _ st]
    (case (first req)
      :join (let [[_ g pids] req
                  st (reduce (fn [s p] (if (some #(= p (second %)) (:mons s))
                                         s
                                         (assoc-in s [:mons (act/monitor! p)] [:member p])))
                             st pids)]
              (broadcast! [:join (act/node) g pids])
              [:reply :ok (publish! (-> (update-in st [:local g] (fnil into []) pids)
                                        (notify! :join g pids)))])
      :leave (let [[_ g pids] req] [:reply :ok (publish! (leave-local st g pids))])
      :monitor (let [[_ g w] req
                     ref (act/make-ref)
                     st (assoc-in st [:watchers g ref] w)
                     st (assoc-in st [:mons (act/monitor! w)] [:watcher g ref])]
                 [:reply [ref (get (view st) g [])] st])
      :demonitor (let [[_ g ref] req
                       mref (some (fn [[m v]] (when (= v [:watcher g ref]) m)) (:mons st))]
                   (when mref (act/demonitor! mref {:flush true}))
                   [:reply :ok (cond-> (update-in st [:watchers g] dissoc ref)
                                 mref (update :mons dissoc mref))])))
  (handle-cast [_ req st]
    (case (first req)
      :join (let [[_ peer g pids] req]
              [:noreply (publish! (-> (update-in st [:remote peer g] (fnil into []) pids)
                                      (notify! :join g pids)))])
      :leave (let [[_ peer g pids] req
                   [ms gone] (removed (vec (get-in st [:remote peer g])) pids)]
               [:noreply (publish! (-> (assoc-in st [:remote peer g] ms)
                                       (notify! :leave g gone)))])
      :sync (let [[_ peer groups] req] [:noreply (publish! (set-remote st peer groups))])))
  (handle-info [_ msg st]
    (cond
      (and (vector? msg) (= :DOWN (first msg)))
      (let [[_ mref _ p] msg
            [kind & more] (get-in st [:mons mref])
            st (update st :mons dissoc mref)]
        (case kind
          :member [:noreply (publish! (reduce (fn [s [g ms]]
                                                (leave-local s g (filterv #(= p %) ms)))
                                              st (:local st)))]
          :watcher (let [[g ref] more] [:noreply (update-in st [:watchers g] dissoc ref)])
          [:noreply st]))
      (and (vector? msg) (= :nodeup (first msg)))
      ;; the peer may be down again by now: the sync must not reconnect it
      (do (node/without-connecting
            (gs/cast! [:At ::scope (second msg)] [:sync (act/node) (into {} (filter (comp seq val)) (:local st))]))
          [:noreply st])
      (and (vector? msg) (= :nodedown (first msg)))
      [:noreply (publish! (set-remote st (second msg) {}))]
      :else [:noreply st]))
  (handle-timeout [_ st] [:noreply st])
  (handle-continue [_ _ st] [:noreply st])
  (terminate [_ _ _] nil))

(defn- ensure-scope!
  "This node's scope, started if it is not running."
  []
  (or (act/whereis ::scope)
      (try (gs/start (->Scope) {:name ::scope})
           (catch Throwable _ (act/whereis ::scope)))))

(node/on-start! ::scope (fn [_] (ensure-scope!)))

(defn- local-pids
  "pids as a vector, each a live process of this node; else a badarg."
  [pids]
  (let [ps (if (sequential? pids) (vec pids) [pids])]
    (doseq [p ps]
      (when-not (and (act/actor? p) (= (act/node) (proc/-node p)))
        (throw (ex-info "a process joins a group from its own node" {:reason :badarg :process p}))))
    ps))

(defn join
  "Add pids (one process or several) to group.  Each must be a process of
  this node.  Returns :ok."
  [group pids]
  (gs/call! (ensure-scope!) [:join group (local-pids pids)]))

(defn leave
  "Take pids out of group.  Returns :ok."
  [group pids]
  (gs/call! (ensure-scope!) [:leave group (local-pids pids)]))

(defn get-members
  "The members of group on every node, as this node knows them."
  [group]
  (ensure-scope!)
  (get-in @views [(act/node) group] []))

(defn get-local-members
  "The members of group on this node."
  [group]
  (filterv #(= (act/node) (proc/-node %)) (get-members group)))

(defn which-groups
  "The groups with a member anywhere."
  []
  (ensure-scope!)
  (vec (keys (get @views (act/node)))))

(defn monitor
  "Watch group from the current actor: [ref members], members as they are
  now; from then on the actor receives [ref :join group pids] and [ref
  :leave group pids] as processes join and leave it."
  [group]
  (gs/call! (ensure-scope!) [:monitor group (act/self)]))

(defn demonitor
  "Stop watching group under ref.  Returns :ok."
  [group ref]
  (gs/call! (ensure-scope!) [:demonitor group ref]))

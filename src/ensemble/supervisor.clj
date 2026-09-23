(ns ensemble.supervisor
  "An OTP supervisor.

  A supervisor starts children from specs and watches them; when a child exits
  it restarts children according to its strategy, unless too many restarts
  happen too fast, in which case the whole tree shuts down.

  A child spec is a map:

      {:id      any     identity, unique within the supervisor
       :start   (fn [] actor)   returns a started child actor
       :restart :permanent | :transient | :temporary   (default :permanent)}

  Strategies: :one-for-one restarts only the failed child, :one-for-all
  restarts every child, and :rest-for-one restarts the failed child and every
  child started after it.

  A child's restart type decides whether its exit warrants a restart:
  :permanent always, :transient only on an abnormal exit, :temporary never.

  Jolt fibers cannot be cancelled, so terminating a child only stops the
  supervisor tracking it; the child's own body keeps running until it returns.
  Each start carries a generation, so a superseded child's later exit is
  ignored instead of triggering a spurious restart."
  (:require [jolt.fibers :as fib]
            [ensemble.actor :as act]
            [ensemble.gen-server :as gs]))

(defn- start-child-actor [spec] ((:start spec)))

(defn- watch!
  "Spawn a fiber that blocks until the child exits, then tells the supervisor.
  The generation tags which incarnation of the child this watcher guards."
  [sup id gen actor]
  (fib/spawn
   (fn []
     (let [reason (try (act/join actor) :normal
                       (catch Throwable e e))]
       (act/! sup [:info [:child-exit id gen reason]])))))

(defn- start-and-register [sup child]
  (let [gen (inc (:gen child 0))
        a (start-child-actor (:spec child))]
    (watch! sup (:id child) gen a)
    (assoc child :actor a :gen gen)))

(defn- restart?
  [restart reason]
  (case restart
    :temporary false
    :transient (not (or (= :normal reason) (= :shutdown reason)))
    true))

(defn- prune-window [times max-ms now]
  (filterv (fn [t] (<= (- now t) max-ms)) times))

(defn- child-index [children id]
  (first (keep-indexed (fn [i c] (when (= (:id c) id) i)) children)))

(defn- restart-ids [strategy children id]
  (let [ids (mapv :id children)]
    (case strategy
      :one-for-all (set ids)
      :rest-for-one (set (subvec ids (child-index children id)))
      (hash-set id))))

(defn- drop-child [st id]
  (update st :children (fn [cs] (filterv (fn [c] (not= (:id c) id)) cs))))

(defrecord Supervisor [strategy max-restarts max-seconds initial]
  gs/Server
  (init [_]
    {:restarts []
     :children (mapv (fn [spec]
                       (start-and-register (act/self)
                                           {:id (:id spec)
                                            :spec spec
                                            :restart (:restart spec :permanent)}))
                     initial)})
  (handle-call [_ _from msg st]
    (case (first msg)
      :start-child
      (let [child (start-and-register (act/self)
                                      {:id (nth msg 1)
                                       :spec (nth msg 2)
                                       :restart (nth msg 3 :permanent)})]
        [:reply (:id child) (update st :children conj child)])
      :terminate-child
      [:reply :ok (drop-child st (nth msg 1))]
      :remove-child
      [:reply :ok (drop-child st (nth msg 1))]
      :remove-and-terminate-child
      (let [id (nth msg 1)
            c (first (filter (fn [x] (= (:id x) id)) (:children st)))]
        (when-let [a (:actor c)] (act/! a [:ensemble/shutdown :shutdown]))
        [:reply :ok (drop-child st id)])
      :get-child
      (let [c (first (filter (fn [x] (= (:id x) (nth msg 1))) (:children st)))]
        [:reply (:actor c) st])
      :which-children
      [:reply (mapv :id (:children st)) st]))
  (handle-cast [_ _msg st] [:noreply st])
  (handle-info [_ msg st]
    (let [[_ id gen reason] msg
          child (first (filter (fn [c] (and (= (:id c) id) (= (:gen c) gen)))
                               (:children st)))
          now (System/currentTimeMillis)
          window (prune-window (:restarts st) (* 1000 max-seconds) now)]
      (cond
        (nil? child) [:noreply st]
        (not (restart? (:restart child) reason)) [:noreply (drop-child st id)]
        (> (inc (count window)) max-restarts) [:stop :shutdown (assoc st :restarts window)]
        :else
        (let [ids (restart-ids strategy (:children st) id)
              children (mapv (fn [c]
                               (if (contains? ids (:id c))
                                 (start-and-register (act/self) c)
                                 c))
                             (:children st))]
          [:noreply {:children children :restarts (conj window now)}]))))
  (handle-timeout [_ st] [:noreply st])
  (terminate [_ _reason _st] nil))

(defn start-supervisor
  "Start a supervisor.  Options:

      :strategy      :one-for-one | :one-for-all | :rest-for-one
      :max-restarts  restarts allowed within :max-seconds before shutdown
      :max-seconds   the window :max-restarts is counted over
      :children      a seq of child specs to start before the supervisor runs"
  ([] (start-supervisor {}))
  ([{:keys [strategy max-restarts max-seconds children]
     :or   {strategy :one-for-one max-restarts 3 max-seconds 5}}]
   (gs/gen-server (->Supervisor strategy max-restarts max-seconds children))))

(defn start-child!
  "Start a child from spec under sup.  Returns the child id."
  [sup id spec]
  (gs/call! sup [:start-child id spec (:restart spec :permanent)]))

(defn terminate-child! [sup id]
  (gs/call! sup [:terminate-child id]))

(defn get-child
  "The live child actor registered under id, or nil."
  [sup id]
  (gs/call! sup [:get-child id]))

(defn remove-child!
  "Stop tracking the child under id, leaving its actor running."
  [sup id]
  (gs/call! sup [:remove-child id]))

(defn remove-and-terminate-child!
  "Stop tracking the child under id and best-effort stop it.  Jolt fibers cannot
  be cancelled, so the supervisor sends the child a shutdown message (which a
  gen-server child honours) and otherwise just untracks it."
  [sup id]
  (gs/call! sup [:remove-and-terminate-child id]))

(defn which-children!
  "The ids of the supervisor's live children, in start order."
  [sup]
  (gs/call! sup [:which-children]))

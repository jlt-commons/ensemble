(ns ensemble.supervisor
  "The OTP supervisor behaviour.

  A supervisor is a gen-server that traps exits, starts its children in
  order, links to each, and restarts them when they exit.  What to restart is
  decided by ensemble.order: whether the exit earns a restart (the child's
  restart type), which siblings the strategy stops and starts, and whether
  the restart intensity has been exceeded.

  A child spec is a map:

      {:id       any, unique within the supervisor
       :start    (fn [] actor)  run in the supervisor; returns the child,
                                normally started with start-link, or nil to
                                ignore (the spec is kept, with no child)
       :restart  :permanent | :transient | :temporary      (:permanent)
       :shutdown ms | :brutal-kill | :infinity    (5000, :infinity for a
                                                   supervisor)
       :type     :worker | :supervisor                      (:worker)}

  Flags: :strategy (:one-for-one, :one-for-all, :rest-for-one), :intensity and
  :period -- more than intensity restarts within period seconds and the
  supervisor gives up: it stops its children and exits with :shutdown, which
  its own supervisor then sees.  OTP's defaults: one-for-one, 1 in 5.

  A child is stopped as OTP's supervisor does: unlinked, sent an exit signal
  :shutdown, and given :shutdown ms to exit before it is killed.  A child that
  traps exits (a gen-server started with {:trap true}) runs its terminate
  first; one that does not simply dies.  Children stop in reverse start
  order."
  (:require [ensemble.actor :as act :refer [receive]]
            [ensemble.gen-server :as gs]
            [ensemble.order :as ord]))

(defn- child-of [spec]
  {:id (:id spec)
   :spec spec
   :restart (:restart spec :permanent)
   :type (:type spec :worker)
   :shutdown (:shutdown spec (if (= :supervisor (:type spec)) :infinity 5000))
   :actor nil})

(defn- by-id [children id]
  (first (filter (fn [c] (= id (:id c))) children)))

(defn- by-actor [children a]
  (first (filter (fn [c] (= a (:actor c))) children)))

(defn- put-child [children child]
  (mapv (fn [c] (if (= (:id c) (:id child)) child c)) children))

(defn- drop-child [children id]
  (filterv (fn [c] (not= id (:id c))) children))

(defn- start-child
  "Start child and link it.  [:ok child] or [:error reason]."
  [child]
  (try
    (let [a ((:start (:spec child)))]
      (when a (act/link! a))
      [:ok (assoc child :actor a)])
    (catch Throwable e [:error (act/reason-of e)])))

(defn- shutdown-child
  "Stop a running child as OTP does, and return the child with no actor."
  [child]
  (when-let [a (:actor child)]
    (let [ref (act/monitor! a)
          how (:shutdown child)]
      (act/unlink! a)
      (receive [[:EXIT a _] nil] [:after 0 nil])
      (act/exit! a (if (= :brutal-kill how) :kill :shutdown))
      (receive
       [[:DOWN ref :process _ _] nil]
       [:after (when (int? how) how)
        (act/exit! a :kill)
        (receive [[:DOWN ref :process _ _] nil])])))
  (assoc child :actor nil))

(defn- stop-all [children]
  (reduce (fn [cs id] (put-child cs (shutdown-child (by-id cs id))))
          children
          (ord/stop-order (mapv :id children))))

(defn- restart
  "Restart after child id exited: stop and start what the strategy's plan says.
  A child that fails to start is retried through the same path, so the retry
  counts against the intensity."
  [strategy children id]
  (let [[_ stop start] (ord/restart-plan strategy (mapv (juxt :id :restart) children) id)
        stopped (reduce (fn [cs sid] (put-child cs (shutdown-child (by-id cs sid))))
                        children stop)
        kept (reduce (fn [cs sid]
                       (if (some #{sid} start) cs (drop-child cs sid)))
                     stopped stop)]
    (reduce (fn [cs sid]
              (let [r (start-child (by-id cs sid))]
                (if (= :ok (first r))
                  (put-child cs (second r))
                  (do (act/! (act/self) [::retry sid]) cs))))
            kept start)))

(defn- child-exited
  "Handle the exit of child with reason: restart, give up, or forget it."
  [{:keys [strategy intensity period]} st child reason]
  (let [cs (:children st)]
    (if (ord/restart? (:restart child) reason)
      (let [[verdict times] (ord/intensity (:restarts st) (System/currentTimeMillis)
                                           (* 1000 period) intensity)
            st (assoc st :restarts times)]
        (if (= :Exceed verdict)
          [:stop :shutdown (assoc st :children (put-child cs (assoc child :actor nil)))]
          [:noreply (assoc st :children
                           (restart strategy (put-child cs (assoc child :actor nil)) (:id child)))]))
      [:noreply (assoc st :children
                       (if (= :temporary (:restart child))
                         (drop-child cs (:id child))
                         (put-child cs (assoc child :actor nil))))])))

(defn- info [c] (select-keys c [:id :actor :type :restart]))

(defrecord Supervisor [flags specs]
  gs/Server
  (init [_]
    (let [cs (reduce (fn [cs spec]
                       (let [r (start-child (child-of spec))]
                         (if (= :ok (first r))
                           (conj cs (second r))
                           (do (stop-all cs)
                               (throw (ex-info "supervisor failed to start a child"
                                               {:reason [:shutdown [:failed-to-start-child (:id spec) (second r)]]}))))))
                     [] specs)]
      {:children cs :restarts []}))
  (handle-call [_ req _ st]
    (let [cs (:children st)
          id (second req)
          c (by-id cs id)]
      (case (first req)
        :start-child
        (let [spec (second req)]
          (if (by-id cs (:id spec))
            [:reply [:error (if (:actor (by-id cs (:id spec))) :already-started :already-present)] st]
            (let [r (start-child (child-of spec))]
              (if (= :ok (first r))
                [:reply [:ok (:actor (second r))] (update st :children conj (second r))]
                [:reply r st]))))
        :terminate-child
        (cond
          (nil? c) [:reply [:error :not-found] st]
          (= :temporary (:restart c)) (do (shutdown-child c) [:reply [:ok nil] (assoc st :children (drop-child cs id))])
          :else [:reply [:ok nil] (assoc st :children (put-child cs (shutdown-child c)))])
        :restart-child
        (cond
          (nil? c) [:reply [:error :not-found] st]
          (:actor c) [:reply [:error :running] st]
          :else (let [r (start-child c)]
                  (if (= :ok (first r))
                    [:reply [:ok (:actor (second r))] (assoc st :children (put-child cs (second r)))]
                    [:reply r st])))
        :delete-child
        (cond
          (nil? c) [:reply [:error :not-found] st]
          (:actor c) [:reply [:error :running] st]
          :else [:reply [:ok nil] (assoc st :children (drop-child cs id))])
        :which-children [:reply [:ok (mapv info cs)] st]
        :count-children [:reply [:ok {:specs (count cs)
                                      :active (count (filter :actor cs))
                                      :supervisors (count (filter #(= :supervisor (:type %)) cs))
                                      :workers (count (filter #(= :worker (:type %)) cs))}]
                         st])))
  (handle-cast [_ _ st] [:noreply st])
  (handle-info [_ msg st]
    (cond
      (and (vector? msg) (= :EXIT (first msg)))
      (if-let [c (by-actor (:children st) (second msg))]
        (child-exited flags st c (nth msg 2))
        [:noreply st])

      (and (vector? msg) (= ::retry (first msg)))
      (if-let [c (by-id (:children st) (second msg))]
        (if (:actor c) [:noreply st] (child-exited flags st c :start-failed))
        [:noreply st])

      :else [:noreply st]))
  (handle-timeout [_ st] [:noreply st])
  (terminate [_ _ st] (stop-all (:children st))))

(defn- start* [start-fn flags specs opts]
  (let [flags (merge {:strategy :one-for-one :intensity 1 :period 5} flags)]
    (start-fn (->Supervisor flags (vec specs)) (assoc opts :trap true))))

(defn start
  "Start a supervisor with flags {:strategy :intensity :period} and child
  specs, started in order before this returns.  Options as gen-server start
  (:name).  Throws if a child fails to start."
  ([flags specs] (start flags specs {}))
  ([flags specs opts] (start* gs/start flags specs opts)))

(defn start-link
  "start, linked to the calling actor -- how a supervisor is started as the
  child of another."
  ([flags specs] (start-link flags specs {}))
  ([flags specs opts] (start* gs/start-link flags specs opts)))

(defn- result [r]
  (if (= :ok (first r))
    (second r)
    (throw (ex-info (str "supervisor: " (pr-str (second r))) {:reason (second r)}))))

(defn start-child!
  "Start a child from spec and add it.  Returns the child actor (nil if its
  start ignored).  Throws if the id is taken or the start fails."
  [sup spec]
  (result (gs/call! sup [:start-child spec] nil)))

(defn terminate-child!
  "Stop the child id.  Its spec stays, so restart-child! can start it again
  (a :temporary child's spec is dropped).  Throws if there is no such child."
  [sup id]
  (result (gs/call! sup [:terminate-child id] nil))
  :ok)

(defn restart-child!
  "Start again the stopped child id.  Returns the child actor."
  [sup id]
  (result (gs/call! sup [:restart-child id] nil)))

(defn delete-child!
  "Remove the stopped child id's spec."
  [sup id]
  (result (gs/call! sup [:delete-child id] nil))
  :ok)

(defn which-children
  "The children, in start order: [{:id :actor :type :restart}], :actor nil
  for a child that is not running."
  [sup]
  (result (gs/call! sup [:which-children])))

(defn count-children
  "{:specs :active :supervisors :workers}."
  [sup]
  (result (gs/call! sup [:count-children])))

(defn child
  "The running actor of child id, or nil."
  [sup id]
  (:actor (first (filter #(= id (:id %)) (which-children sup)))))

(defn stop!
  "Stop the supervisor: its children stop in reverse start order, then it
  exits with reason (default :normal).  Waits until it has."
  ([sup] (gs/stop! sup :normal))
  ([sup reason] (gs/stop! sup reason)))

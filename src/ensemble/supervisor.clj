(ns ensemble.supervisor
  "The OTP supervisor behaviour.

  A supervisor is a gen-server that traps exits, starts its children in
  order, links to each, and restarts them when they exit.  What it accepts
  is decided by ensemble.childspec, and what to restart by ensemble.order:
  whether the exit earns a restart (the child's restart type), which
  siblings the strategy stops and starts, whether the restart intensity has
  been exceeded, and whether a significant child's end shuts the supervisor
  down.

  A child spec is a map:

      {:id          any, unique within the supervisor
       :start       (fn [& args] actor)  run in the supervisor; returns the
                    child, normally started with start-link, or nil to
                    ignore (the spec is kept, with no child)
       :restart     :permanent | :transient | :temporary     (:permanent)
       :shutdown    ms | :brutal-kill | :infinity    (5000, :infinity for a
                                                      supervisor)
       :type        :worker | :supervisor                     (:worker)
       :significant true | false: does its end shut the supervisor down
                    (false; only with :auto-shutdown, never :permanent)}

  Flags: :strategy (:one-for-one, :one-for-all, :rest-for-one,
  :simple-one-for-one), :intensity and :period -- more than intensity
  restarts within period seconds and the supervisor gives up: it stops its
  children and exits with :shutdown, which its own supervisor then sees --
  and :auto-shutdown (:never, :any-significant, :all-significant).  OTP's
  defaults: one-for-one, 1 in 5, never.

  A :simple-one-for-one supervisor has one spec, a template: it starts no
  child itself, and start-child! starts one from the template with extra
  args, (apply start args).  Its children are known by their actor, not
  an id, and each is restarted alone.

  A child is stopped as OTP's supervisor does: unlinked, sent an exit signal
  :shutdown, and given :shutdown ms to exit before it is killed.  A child that
  traps exits (a gen-server started with {:trap true}) runs its terminate
  first; one that does not simply dies.  Children stop in reverse start
  order."
  (:require [ensemble.actor :as act :refer [receive]]
            [ensemble.childspec :as cs]
            [ensemble.gen-server :as gs]
            [ensemble.order :as ord]))

(defn- child-of
  "A running child from a checked spec: key is how the supervisor knows it,
  its id, or a fresh key for a dynamic child."
  [spec key args]
  (assoc spec :key key :args args :actor nil))

(defn- by-key [children k]
  (first (filter (fn [c] (= k (:key c))) children)))

(defn- by-actor [children a]
  (first (filter (fn [c] (= a (:actor c))) children)))

(defn- put-child [children child]
  (mapv (fn [c] (if (= (:key c) (:key child)) child c)) children))

(defn- drop-child [children k]
  (filterv (fn [c] (not= k (:key c))) children))

(defn- start-child
  "Start child and link it.  [:ok child] or [:error reason]."
  [child]
  (try
    (let [a (apply (:start child) (:args child))]
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
  (reduce (fn [cs k] (put-child cs (shutdown-child (by-key cs k))))
          children
          (ord/stop-order (mapv :key children))))

(defn- simple? [flags] (= :simple-one-for-one (:strategy flags)))

(defn- restart
  "Restart after child k exited: stop and start what the strategy's plan says.
  A child that fails to start is retried through the same path, so the retry
  counts against the intensity."
  [strategy children k]
  (let [[_ stop start] (ord/restart-plan strategy (mapv (juxt :key :restart) children) k)
        stopped (reduce (fn [cs sk] (put-child cs (shutdown-child (by-key cs sk))))
                        children stop)
        kept (reduce (fn [cs sk]
                       (if (some #{sk} start) cs (drop-child cs sk)))
                     stopped stop)]
    (reduce (fn [cs sk]
              (let [r (start-child (by-key cs sk))]
                (if (= :ok (first r))
                  (put-child cs (second r))
                  (do (act/! (act/self) [::retry sk]) cs))))
            kept start)))

(defn- significant-left
  "How many significant children other than k are still running."
  [children k]
  (count (filter (fn [c] (and (:significant c) (:actor c) (not= k (:key c)))) children)))

(defn- child-exited
  "Handle the exit of child with reason: restart, give up, shut down on its
  own, or forget it."
  [{:keys [strategy intensity period auto-shutdown]} st child reason]
  (let [cs (:children st)
        k (:key child)]
    (if (ord/restart? (:restart child) reason)
      (let [[verdict times] (ord/intensity (:restarts st) (System/currentTimeMillis)
                                           (* 1000 period) intensity)
            st (assoc st :restarts times)]
        (if (= :Exceed verdict)
          [:stop :shutdown (assoc st :children (put-child cs (assoc child :actor nil)))]
          [:noreply (assoc st :children
                           (restart strategy (put-child cs (assoc child :actor nil)) k))]))
      (let [cs (if (or (= :temporary (:restart child)) (= :simple-one-for-one strategy))
                 (drop-child cs k)
                 (put-child cs (assoc child :actor nil)))
            st (assoc st :children cs)]
        (if (ord/auto-shutdown? auto-shutdown (:significant child) (:restart child) reason
                                (significant-left cs k))
          [:stop :shutdown st]
          [:noreply st])))))

(defn- handle-start-child
  "start-child!: under a :simple-one-for-one supervisor, a child from the
  template with extra args; otherwise a new child from spec, checked as the
  specs at start are."
  [flags st arg]
  (let [cs (:children st)]
    (if (simple? flags)
      (let [k [::dynamic (:next st 0)]
            r (start-child (child-of (:template st) k (vec arg)))]
        (if (= :ok (first r))
          [:reply [:ok (:actor (second r))]
           (-> st (update :children conj (second r)) (update :next (fnil inc 0)))]
          [:reply r st]))
      (let [checked (cs/check-child arg (:auto-shutdown flags))]
        (case (first checked)
          :Error [:reply [:error (second checked)] st]
          :Ok (let [[_ spec] checked]
                (if-let [c (by-key cs (:id spec))]
                  [:reply [:error (if (:actor c) :already-started :already-present)] st]
                  (let [r (start-child (child-of spec (:id spec) []))]
                    (if (= :ok (first r))
                      [:reply [:ok (:actor (second r))] (update st :children conj (second r))]
                      [:reply r st])))))))))

(defn- spec-of [c] (select-keys c [:id :start :restart :shutdown :type :significant]))

(defn- info [c] (assoc (select-keys c [:id :actor :type :restart]) :id (when-not (vector? (:key c)) (:id c))))

(defn- find-child
  "The child a call names: by actor under a :simple-one-for-one supervisor,
  by id otherwise."
  [flags cs x]
  (if (simple? flags) (by-actor cs x) (by-key cs x)))

(defrecord Supervisor [flags specs]
  gs/Server
  (init [_]
    (if (simple? flags)
      {:children [] :restarts [] :template (first specs) :next 0}
      (let [cs (reduce (fn [cs spec]
                         (let [r (start-child (child-of spec (:id spec) []))]
                           (if (= :ok (first r))
                             (conj cs (second r))
                             (do (stop-all cs)
                                 (throw (ex-info "supervisor failed to start a child"
                                                 {:reason [:shutdown [:failed-to-start-child (:id spec) (second r)]]}))))))
                       [] specs)]
        {:children cs :restarts []})))
  (handle-call [_ req _ st]
    (let [cs (:children st)
          x (second req)
          c (find-child flags cs x)]
      (case (first req)
        :start-child (handle-start-child flags st x)
        :terminate-child
        (cond
          (nil? c) [:reply [:error :not-found] st]
          (or (= :temporary (:restart c)) (simple? flags))
          (do (shutdown-child c) [:reply [:ok nil] (assoc st :children (drop-child cs (:key c)))])
          :else [:reply [:ok nil] (assoc st :children (put-child cs (shutdown-child c)))])
        :restart-child
        (cond
          (simple? flags) [:reply [:error :simple-one-for-one] st]
          (nil? c) [:reply [:error :not-found] st]
          (:actor c) [:reply [:error :running] st]
          :else (let [r (start-child c)]
                  (if (= :ok (first r))
                    [:reply [:ok (:actor (second r))] (assoc st :children (put-child cs (second r)))]
                    [:reply r st])))
        :delete-child
        (cond
          (simple? flags) [:reply [:error :simple-one-for-one] st]
          (nil? c) [:reply [:error :not-found] st]
          (:actor c) [:reply [:error :running] st]
          :else [:reply [:ok nil] (assoc st :children (drop-child cs (:key c)))])
        :get-childspec
        (cond
          (and (simple? flags) c) [:reply [:ok (spec-of (:template st))] st]
          (nil? c) [:reply [:error :not-found] st]
          :else [:reply [:ok (spec-of c)] st])
        :which-children [:reply [:ok (mapv info cs)] st]
        :count-children [:reply [:ok {:specs (if (simple? flags) 1 (count cs))
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
      (if-let [c (by-key (:children st) (second msg))]
        (if (:actor c) [:noreply st] (child-exited flags st c :start-failed))
        [:noreply st])

      :else [:noreply st]))
  (handle-timeout [_ st] [:noreply st])
  (terminate [_ _ st] (stop-all (:children st))))

(defn- start*
  "Check the flags and specs, then start the supervisor; a bad one is an
  error before anything runs, as OTP's {error, {start_spec, Reason}}."
  [start-fn flags specs opts]
  (let [f (cs/check-flags flags)]
    (case (first f)
      :Error (throw (ex-info (str "supervisor: bad flags " (pr-str (second f)))
                             {:reason [:supervisor-data (second f)]}))
      :Ok (let [[_ flags] f
                c (cs/check-specs flags (vec specs))]
            (case (first c)
              :Error (throw (ex-info (str "supervisor: bad child spec " (pr-str (second c)))
                                     {:reason [:start-spec (second c)]}))
              :Ok (let [[_ children] c]
                    (start-fn (->Supervisor flags children) (assoc opts :trap true))))))))

(defn start
  "Start a supervisor with flags {:strategy :intensity :period
  :auto-shutdown} and child specs, started in order before this returns.
  Options as gen-server start (:name).  Throws if the flags or a spec are
  bad, or a child fails to start."
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
  "Start a child from spec and add it, or, under a :simple-one-for-one
  supervisor, from its template with the extra args (a vector).  Returns
  the child actor (nil if its start ignored).  Throws if the spec is bad,
  the id is taken or the start fails."
  [sup spec-or-args]
  (result (gs/call! sup [:start-child spec-or-args] nil)))

(defn terminate-child!
  "Stop the child id -- the child actor, under a :simple-one-for-one
  supervisor.  Its spec stays, so restart-child! can start it again (a
  :temporary or dynamic child's is dropped).  Throws if there is no such
  child."
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

(defn get-childspec
  "The spec of child id (or, under :simple-one-for-one, of child actor:
  the template), with the defaults filled in."
  [sup id]
  (result (gs/call! sup [:get-childspec id])))

(defn which-children
  "The children, in start order: [{:id :actor :type :restart}], :actor nil
  for a child that is not running, :id nil for a dynamic child."
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

(ns ensemble.childspec
  "Which child specs and supervisor flags a supervisor accepts, as OTP's
  supervisor checks them.  The supervisor reads every spec and its flags
  through these, so a spec it runs is one they accepted, with the defaults
  filled in.

  A child spec is a map with a mandatory :id and :start (a fn), and
  optional :restart, :shutdown, :type and :significant.  Flags are
  :strategy, :intensity, :period and :auto-shutdown.  Each check returns
  [:Ok value] or [:Error reason], the reason as OTP names it.")

(def ^:private restarts #{:permanent :transient :temporary})
(def ^:private types #{:worker :supervisor})
(def ^:private strategies #{:one-for-one :one-for-all :rest-for-one :simple-one-for-one})
(def ^:private auto-shutdowns #{:never :any-significant :all-significant})

(defn- shutdown? [s]
  (or (= :brutal-kill s) (= :infinity s) (and (integer? s) (<= 0 s))))

(defn check-child
  "Accept child spec, under a supervisor whose :auto-shutdown is auto:
  [:Ok child] with the defaults filled in, or [:Error reason]."
  [spec auto]
  (cond
    (not (map? spec)) [:Error [:invalid-child-spec spec]]
    (not (contains? spec :id)) [:Error :missing-id]
    (not (contains? spec :start)) [:Error :missing-start]
    (not (fn? (get spec :start))) [:Error [:invalid-mfa (get spec :start)]]
    :else
    (let [restart (get spec :restart :permanent)
          type (get spec :type :worker)
          shutdown (get spec :shutdown (if (= :supervisor type) :infinity 5000))
          significant (get spec :significant false)]
      (cond
        (not (contains? restarts restart)) [:Error [:invalid-restart-type restart]]
        (not (contains? types type)) [:Error [:invalid-child-type type]]
        (not (shutdown? shutdown)) [:Error [:invalid-shutdown shutdown]]
        (not (boolean? significant)) [:Error [:invalid-significant significant]]
        (and significant (= :never auto))
        [:Error [:bad-combination [[:auto-shutdown :never] [:significant true]]]]
        (and significant (= :permanent restart))
        [:Error [:bad-combination [[:restart :permanent] [:significant true]]]]
        :else [:Ok {:id (get spec :id) :start (get spec :start) :restart restart
                    :shutdown shutdown :type type :significant significant}]))))

(defn check-flags
  "Accept supervisor flags: [:Ok flags] with the defaults filled in, or
  [:Error reason]."
  [flags]
  (if-not (map? flags)
    [:Error [:invalid-flags flags]]
    (let [strategy (get flags :strategy :one-for-one)
          intensity (get flags :intensity 1)
          period (get flags :period 5)
          auto (get flags :auto-shutdown :never)]
      (cond
        (not (contains? strategies strategy)) [:Error [:invalid-strategy strategy]]
        (not (and (integer? intensity) (<= 0 intensity))) [:Error [:invalid-intensity intensity]]
        (not (and (integer? period) (< 0 period))) [:Error [:invalid-period period]]
        (not (contains? auto-shutdowns auto)) [:Error [:invalid-auto-shutdown auto]]
        :else [:Ok {:strategy strategy :intensity intensity :period period :auto-shutdown auto}]))))

(defn check-specs
  "Accept a supervisor's child specs under its checked flags: [:Ok
  children] in order, or [:Error reason] for the first that fails.  Ids
  are unique; a :simple-one-for-one supervisor has exactly one spec, its
  children's template."
  [flags specs]
  (if (and (= :simple-one-for-one (get flags :strategy)) (not= 1 (count specs)))
    [:Error [:bad-start-spec specs]]
    (loop [todo (seq specs), out []]
      (if-let [s (first todo)]
        (let [r (check-child s (get flags :auto-shutdown))]
          (case (first r)
            :Error r
            :Ok (let [[_ c] r]
                  (if (some (fn [d] (= (:id c) (:id d))) out)
                    [:Error [:duplicate-child-name (:id c)]]
                    (recur (next todo) (concat out [c]))))))
        [:Ok (vec out)]))))

;; --- what a supervisor does with a child's start, and which operations a
;; --- child allows

(defn after-start
  "What the supervisor keeps once a child's start gave outcome:
  [:Started actor], [:Ignored] or [:Failed reason].  [:Add actor] keeps
  the child (actor nil: kept without a process, OTP's undefined), [:Skip]
  keeps nothing, [:Refuse reason] is a failed start.  An ignored
  :temporary child, or one of a :simple-one-for-one supervisor, is not
  kept."
  [strategy restart outcome]
  (case (first outcome)
    :Started (let [[_ a] outcome] [:Add a])
    :Ignored (if (or (= :temporary restart) (= :simple-one-for-one strategy))
               [:Skip]
               [:Add nil])
    :Failed (let [[_ e] outcome] [:Refuse e])))

(defn start-reply
  "What start_child answers for outcome: [:ok actor], [:ok nil] for an
  ignored start, or [:error reason]."
  [outcome]
  (case (first outcome)
    :Started (let [[_ a] outcome] [:ok a])
    :Ignored [:ok nil]
    :Failed (let [[_ e] outcome] [:error e])))

(defn may
  "May op ([:Restart], [:Delete] or [:Terminate]) run on a child of status
  [:Running], [:Restarting], [:Stopped] or [:Absent]?  [:Go], or [:No
  reason].  simple? is a :simple-one-for-one supervisor, which refuses
  restart and delete."
  [op simple? status]
  (let [absent? (case (first status) :Absent true false)]
    (case (first op)
      :Terminate (if absent? [:No :not-found] [:Go])
      (:Restart :Delete)
      (cond
        simple? [:No :simple-one-for-one]
        absent? [:No :not-found]
        :else (case (first status)
                :Running [:No :running]
                :Restarting [:No :restarting]
                [:Go])))))

(defn shown
  "What which_children shows as the child of a child of status: its actor
  a, :restarting, or nil (OTP's undefined)."
  [status a]
  (case (first status)
    :Running a
    :Restarting :restarting
    nil))

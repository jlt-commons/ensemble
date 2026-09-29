(ns ensemble.childspec-spec
  "Contract for ensemble.childspec: which child specs and supervisor flags
  a supervisor accepts, as the OTP supervisor docs state them.

  A child spec is a map.  :id and :start are mandatory; :start is a fn
  (OTP's {M,F,A}).  :restart is :permanent (the default), :transient or
  :temporary; :shutdown is :brutal-kill, :infinity or a timeout of 0 ms or
  more, by default 5000 for a worker and :infinity for a supervisor; :type
  is :worker (the default) or :supervisor; :significant is a boolean,
  false by default.  A significant child may not be :permanent, and a
  supervisor whose :auto-shutdown is :never may have no significant child.

  Flags: :strategy is :one-for-one (the default), :one-for-all,
  :rest-for-one or :simple-one-for-one; :intensity is an integer of 0 or
  more (1); :period a positive integer of seconds (5); :auto-shutdown is
  :never (the default), :any-significant or :all-significant.  A
  :simple-one-for-one supervisor has exactly one child spec, the template
  its children are started from; ids are unique in any other.

  Each rejection names the fault as OTP's does: [:invalid-restart-type r],
  :missing-id, [:duplicate-child-name id], and so on.

  A child's start returns the child, ignore, or {error, Error}.  On ignore
  the supervisor keeps the spec with no process (pid undefined), unless the
  child is :temporary; a :simple-one-for-one supervisor adds no child.
  start_child then answers {ok, undefined}.

  restart_child needs the spec and no running process: {error, not_found},
  {error, running}, or {error, restarting} for a child being restarted.
  delete_child the same.  terminate_child needs only the spec.  A
  :simple-one-for-one supervisor refuses restart_child and delete_child.
  which_children shows a child's pid, restarting, or undefined."
  (:require [writ.spec :refer [spec data ann refine graph law calls]]
            [ensemble.childspec :as cs]
            [ensemble.supervisor :as sup]))

(spec ensemble.childspec {:require :proved})

(data Checked (Ok Any) (Error Any))

(refine Accepted [r Checked] (= :Ok (first r)))
(refine Rejected [r Checked] (= :Error (first r)))

(refine Restart [r Keyword] (contains? #{:permanent :transient :temporary} r))
(refine AutoShutdown [a Keyword] (contains? #{:never :any-significant :all-significant} a))

;; what a child's start gave, and what the supervisor does with it
(data Outcome (Started Any) (Ignored) (Failed Any))
(data Kept (Add Any) (Skip) (Refuse Any))
;; a child as a guard sees it
(data Status (Running) (Restarting) (Stopped) (Absent))
(data Op (Restart) (Delete) (Terminate))
(data Go (Go) (No Any))

(refine Strategy [s Keyword] (contains? #{:one-for-one :one-for-all :rest-for-one :simple-one-for-one} s))

(ann check-child [Any AutoShutdown -> Checked])
(ann after-start [Strategy Restart Outcome -> Kept])
(ann start-reply [Outcome -> Any])
(ann may [Op Bool Status -> Go])
(ann shown [Status Any -> Any])
(ann check-flags [Any -> Checked])
(ann check-specs [(Map Keyword Any) (Vec Any) -> Checked])

;; --- the state graph ----------------------------------------------------

;; flags from a user are accepted, normalised, or rejected with why.  A
;; child spec's :start is a fn, which no generated value is, so its step is
;; stated by the laws below and not by the graph
(graph validate
  {:states {:flags (Map Keyword Any), :ok Accepted, :error Rejected}
   :edges  {:flags {[check-flags] #{:ok :error}}}})

;; --- the spec's vocabulary ----------------------------------------------

(defn start [] nil)

(defn worker [] {:id :w :start start})

(defn rejected-with [r reason] (= [:Error reason] r))

(defn child-of [r] (second r))

;; --- child specs --------------------------------------------------------

(law the-defaults-are-otps
  (= (check-child (worker) :never)
     [:Ok {:id :w :start start :restart :permanent :shutdown 5000 :type :worker :significant false}]))

(law a-supervisors-shutdown-defaults-to-infinity
  (= :infinity (:shutdown (child-of (check-child (assoc (worker) :type :supervisor) :never)))))

(law what-is-given-is-kept
  (forall [r Restart, n Nat, a AutoShutdown]
    (let [c (child-of (check-child (assoc (worker) :restart r :shutdown n) a))]
      (and (= r (:restart c)) (= n (:shutdown c))))))

(law an-id-is-mandatory
  (forall [a AutoShutdown] (rejected-with (check-child {:start start} a) :missing-id)))

(law a-start-is-mandatory
  (forall [a AutoShutdown] (rejected-with (check-child {:id :w} a) :missing-start)))

(law a-start-must-be-a-fn
  (forall [x Int, a AutoShutdown]
    (rejected-with (check-child (assoc (worker) :start x) a) [:invalid-mfa x])))

(law an-unknown-restart-type-is-rejected
  (forall [r Keyword, a AutoShutdown]
    (=> (not (contains? #{:permanent :transient :temporary} r))
        (rejected-with (check-child (assoc (worker) :restart r) a) [:invalid-restart-type r]))))

(law a-negative-shutdown-is-rejected
  (forall [n Nat, a AutoShutdown]
    (rejected-with (check-child (assoc (worker) :shutdown (- -1 n)) a) [:invalid-shutdown (- -1 n)])))

(law a-shutdown-that-is-no-time-is-rejected
  (forall [k Keyword, a AutoShutdown]
    (=> (not (contains? #{:brutal-kill :infinity} k))
        (rejected-with (check-child (assoc (worker) :shutdown k) a) [:invalid-shutdown k]))))

(law an-unknown-type-is-rejected
  (forall [t Keyword, a AutoShutdown]
    (=> (not (contains? #{:worker :supervisor} t))
        (rejected-with (check-child (assoc (worker) :type t) a) [:invalid-child-type t]))))

(law significant-is-a-boolean
  (forall [x Int, a AutoShutdown]
    (rejected-with (check-child (assoc (worker) :significant x) a) [:invalid-significant x])))

(law a-permanent-child-is-never-significant
  (forall [a AutoShutdown]
    (=> (not= :never a)
        (rejected-with (check-child (assoc (worker) :significant true) a)
                       [:bad-combination [[:restart :permanent] [:significant true]]]))))

(law no-child-is-significant-to-a-supervisor-that-never-shuts-itself-down
  (forall [r Restart]
    (rejected-with (check-child (assoc (worker) :restart r :significant true) :never)
                   [:bad-combination [[:auto-shutdown :never] [:significant true]]])))

(law a-transient-or-temporary-child-may-be-significant
  (forall [a AutoShutdown]
    (=> (not= :never a)
        (and (:significant (child-of (check-child (assoc (worker) :restart :transient :significant true) a)))
             (:significant (child-of (check-child (assoc (worker) :restart :temporary :significant true) a)))))))

(law a-spec-that-is-no-map-is-rejected
  (forall [x Int, a AutoShutdown] (rejected-with (check-child x a) [:invalid-child-spec x])))

;; --- flags --------------------------------------------------------------

(law the-flag-defaults-are-otps
  (= (check-flags {})
     [:Ok {:strategy :one-for-one :intensity 1 :period 5 :auto-shutdown :never}]))

(law an-unknown-strategy-is-rejected
  (forall [s Keyword]
    (=> (not (contains? #{:one-for-one :one-for-all :rest-for-one :simple-one-for-one} s))
        (rejected-with (check-flags {:strategy s}) [:invalid-strategy s]))))

(law intensity-is-never-negative
  (forall [n Nat] (rejected-with (check-flags {:intensity (- -1 n)}) [:invalid-intensity (- -1 n)])))

(law a-period-is-positive
  (forall [n Nat] (rejected-with (check-flags {:period (- n)}) [:invalid-period (- n)])))

(law an-unknown-auto-shutdown-is-rejected
  (forall [a Keyword]
    (=> (not (contains? #{:never :any-significant :all-significant} a))
        (rejected-with (check-flags {:auto-shutdown a}) [:invalid-auto-shutdown a]))))

(law flags-that-are-no-map-are-rejected
  (forall [x Int] (rejected-with (check-flags x) [:invalid-flags x])))

(law given-flags-are-kept
  (forall [n Nat, p Nat]
    (= (check-flags {:strategy :rest-for-one :intensity n :period (inc p)})
       [:Ok {:strategy :rest-for-one :intensity n :period (inc p) :auto-shutdown :never}])))

;; --- a supervisor's specs -----------------------------------------------

(def flags {:strategy :one-for-one :intensity 1 :period 5 :auto-shutdown :never})

(law specs-are-checked-in-order
  (= (check-specs flags [(worker) {:id :v :start start :restart :transient}])
     [:Ok [(child-of (check-child (worker) :never))
           (child-of (check-child {:id :v :start start :restart :transient} :never))]]))

(law the-first-bad-spec-is-the-error
  (= (check-specs flags [(worker) {:id :v} {:start start}]) [:Error :missing-start]))

(law ids-are-unique
  (forall [x Keyword]
    (rejected-with (check-specs flags [(assoc (worker) :id x) {:id x :start start}])
                   [:duplicate-child-name x])))

(law a-simple-one-for-one-supervisor-has-one-template
  (and (= :Ok (first (check-specs (assoc flags :strategy :simple-one-for-one) [(worker)])))
       (rejected-with (check-specs (assoc flags :strategy :simple-one-for-one) [])
                      [:bad-start-spec []])
       (rejected-with (check-specs (assoc flags :strategy :simple-one-for-one) [(worker) (worker)])
                      [:bad-start-spec [(worker) (worker)]])))

(law significance-is-read-against-the-supervisors-auto-shutdown
  (and (= :Ok (first (check-specs (assoc flags :auto-shutdown :any-significant)
                                  [(assoc (worker) :restart :transient :significant true)])))
       (= :Error (first (check-specs flags [(assoc (worker) :restart :transient :significant true)])))))

;; --- a child's start ---------------------------------------------------------

(law a-started-child-is-added-running
  (forall [st Strategy, r Restart, a Any]
    (= (after-start st r [:Started a]) [:Add a])))

(law an-ignored-child-is-kept-without-a-process
  (forall [st Strategy, r Restart]
    (=> (and (not= :temporary r) (not= :simple-one-for-one st))
        (= (after-start st r [:Ignored]) [:Add nil]))))

(law an-ignored-temporary-child-is-not-kept
  (forall [st Strategy] (= (after-start st :temporary [:Ignored]) [:Skip])))

(law an-ignored-dynamic-child-is-not-added
  (forall [r Restart] (= (after-start :simple-one-for-one r [:Ignored]) [:Skip])))

(law a-failed-start-is-refused
  (forall [st Strategy, r Restart, e Any]
    (= (after-start st r [:Failed e]) [:Refuse e])))

(law start-child-answers-ok-undefined-for-ignore
  (forall [a Any, e Any]
    (and (= (start-reply [:Started a]) [:ok a])
         (= (start-reply [:Ignored]) [:ok nil])
         (= (start-reply [:Failed e]) [:error e]))))

;; --- what a child operation needs ---------------------------------------------

(law an-absent-child-is-not-found
  (forall [op Op] (= (may op false [:Absent]) [:No :not-found])))

(law restart-and-delete-need-a-child-that-is-not-running
  (forall [op Op]
    (=> (not= [:Terminate] op)
        (and (= (may op false [:Running]) [:No :running])
             (= (may op false [:Restarting]) [:No :restarting])
             (= (may op false [:Stopped]) [:Go])))))

(law terminate-needs-only-the-spec
  (forall [s Status]
    (=> (not= [:Absent] s) (= (may [:Terminate] false s) [:Go]))))

(law simple-one-for-one-refuses-restart-and-delete
  (forall [s Status]
    (and (= (may [:Restart] true s) [:No :simple-one-for-one])
         (= (may [:Delete] true s) [:No :simple-one-for-one]))))

(law which-children-shows-pid-restarting-or-undefined
  (forall [a Any]
    (and (= (shown [:Running] a) a)
         (= (shown [:Restarting] a) :restarting)
         (= (shown [:Stopped] a) nil))))

;; --- the supervisor checks what it is given -----------------------------

(calls sup/start* {:through [cs/check-flags cs/check-specs]})
(calls sup/handle-start-child {:through [cs/check-child cs/after-start cs/start-reply]})
(calls sup/restart {:through [cs/after-start]})
(calls sup/start-all {:through [cs/after-start]})
(calls sup/child-call {:through [cs/may cs/shown]})

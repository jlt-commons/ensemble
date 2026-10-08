(ns ensemble.application
  "OTP applications: a named component with a supervision tree, started and
  stopped as a unit, that may depend on other applications.

  An application is a map, registered with load!:

      {:name          keyword, unique
       :start         (fn [] top-supervisor) or (fn [] [top-supervisor state])
       :stop          (fn [state])          run after the tree has stopped
       :prep-stop     (fn [state] state')   run before the tree stops; :stop
                                            gets what it returns
       :applications  [name ...]            must be running first
       :optional-applications [name ...]    of those, the ones that may be
                                            absent: one not loaded is skipped
       :included-applications [name ...]    started by this one's own tree,
                                            never by the controller
       :env           {key value}           its configuration (get-env)
       :start-phases  [[phase args] ...]    run in order after :start, each
                                            as (start-phase phase :normal args)
       :start-phase   (fn [phase type args]) -> :ok
       :type          :temporary | :transient | :permanent    (:temporary)}

  start! runs :start and has an application master monitor the tree it
  returns, then runs the start phases; one that does not answer :ok stops
  the tree and fails the start.  stop! runs :prep-stop, stops the tree (as
  a supervisor stops: children in reverse order) and then runs :stop.  If the tree exits by itself the application
  has stopped, and its type decides the rest, as OTP's does: a :temporary
  application is only reported; a :permanent one, or a :transient one that
  exited abnormally, takes every other running application down with it --
  OTP would stop the node."
  (:require [ensemble.actor :as act :refer [receive]]
            [ensemble.gen-server :as gs]
            [ensemble.logger :as logger]
            [ensemble.signal :as sig]))

(defonce ^:private loaded (atom {}))

(defonce ^:private running (atom {}))

(defonce ^:private exit-log (atom []))

(defonce ^:private envs
  ;; name -> {key value}: each application's configuration
  (atom {}))

(defonce ^:private started-order
  ;; the names of the running applications, in the order they started
  (atom []))

(defn load!
  "Register an application spec.  Its :env is the application's
  configuration, under any value set-env! has given a key already.
  Returns its name."
  [spec]
  (let [old (:env (get @loaded (:name spec)))
        ;; the values a spec loaded before gave, as opposed to set-env!'s
        set-by-hand (fn [env] (into {} (remove (fn [[k v]] (and (contains? old k) (= v (get old k))))) env))]
    (swap! loaded assoc (:name spec) spec)
    (swap! envs update (:name spec) #(merge (:env spec) (set-by-hand %))))
  (:name spec))

(defn get-key
  "The value of key k in the loaded spec of application name."
  [name k]
  (get-in @loaded [name k]))

(defn get-env
  "Application name's configuration value for k, or default (nil)."
  ([name k] (get-env name k nil))
  ([name k default] (get-in @envs [name k] default)))

(defn get-all-env
  "Application name's whole configuration, a map."
  [name]
  (get @envs name {}))

(defn set-env!
  "Set application name's configuration value for k.  Returns :ok."
  [name k v]
  (swap! envs assoc-in [name k] v)
  :ok)

(defn unset-env!
  "Remove k from application name's configuration.  Returns :ok."
  [name k]
  (swap! envs update name dissoc k)
  :ok)

(defn- fail [reason] (throw (ex-info (str "application: " (pr-str reason)) {:reason reason})))

(defn- required
  "The dependencies of spec that must run first: its :applications,
  less the optional ones that are not loaded."
  [spec]
  (let [optional (set (:optional-applications spec))]
    (remove #(and (contains? optional %) (not (contains? @loaded %))) (:applications spec))))

(defn- includer
  "The running application that includes name, or nil."
  [name]
  (some (fn [[n a]] (when (and (map? a) (some #{name} (:included-applications (:spec a)))) n))
        @running))

(declare stop-all!)

(defn- master
  "The application master: once told ::go, monitor the tree; when it exits
  without stop!, forget the application and act on its type."
  [name top kind]
  (act/spawn
   (fn []
     ;; start! records the application before it says go, so a tree dead
     ;; already is not mistaken for one still starting
     (receive [::go nil])
     (let [ref (act/monitor! top)]
       (receive
        [[:DOWN ref :process _ reason]
         (when (= top (:top (get @running name)))
           (swap! running dissoc name)
           (swap! started-order (fn [o] (filterv #(not= name %) o)))
           (swap! exit-log conj [name reason])
           (when (or (= :permanent kind)
                     (and (= :transient kind) (not (sig/shutdown? reason))))
             (stop-all!)))]
        [::stop (act/demonitor! ref {:flush true})])))))

(declare stop!)

(defn- run-phases!
  "Run spec's start phases in order; throws for the first that does not
  answer :ok."
  [spec]
  (doseq [[phase args] (:start-phases spec)]
    (let [r ((:start-phase spec) phase :normal args)]
      (when-not (= :ok r) (fail [:bad-start-phase phase r])))))

(defn start!
  "Start the loaded application name.  Its :applications must be running,
  less any optional one that is not loaded, and its included applications
  loaded.  Throws if it is already running, not loaded, included by a
  running application, a dependency is not running, or its :start or a
  start phase fails.  Returns :ok."
  [name]
  (let [spec (or (get @loaded name) (fail [:not-loaded name]))]
    (when-let [by (includer name)] (fail [:included name by]))
    (doseq [inc (:included-applications spec)]
      (when-not (contains? @loaded inc) (fail [:not-loaded inc])))
    (doseq [dep (required spec)]
      (when-not (contains? @running dep) (fail [:not-started dep])))
    ;; claim the name first, so two starts of one application cannot both run
    (let [[before _] (swap-vals! running (fn [r] (if (contains? r name) r (assoc r name ::starting))))]
      (when (contains? before name) (fail [:already-started name])))
    (let [r (try ((:start spec)) (catch Throwable e (swap! running dissoc name) (throw e)))
          [top state] (if (vector? r) r [r nil])]
      (when-not (act/actor? top)
        (swap! running dissoc name)
        (fail [:bad-return r]))
      (swap! started-order (fn [o] (conj (filterv #(not= name %) o) name)))
      (let [m (master name top (:type spec :temporary))]
        (swap! running assoc name {:spec spec :top top :state state :master m})
        (act/! m ::go))
      (try (run-phases! spec)
           (catch Throwable e (stop! name) (throw e)))
      :ok)))

(defn ensure-all-started!
  "Start name and, first, every application it depends on, transitively,
  that is not already running.  Returns the names started, in order.  If
  one fails to start, those it started are stopped again, the last first,
  and it throws, as OTP 26's does."
  [name]
  ;; depth first: an application goes after everything it depends on
  (let [ordered (loop [todo [name], out [], steps 0]
                  (if-let [n (first todo)]
                    (let [spec (or (get @loaded n) (fail [:not-loaded n]))
                          deps (remove (set out) (required spec))]
                      (when (> steps 10000) (fail [:circular-dependencies name]))
                      (if (seq deps)
                        (recur (concat deps todo) out (inc steps))
                        (recur (rest todo) (if (some #{n} out) out (conj out n)) (inc steps))))
                    out))]
    (loop [todo (remove #(contains? @running %) ordered), started []]
      (if-let [n (first todo)]
        (do (try (start! n)
                 (catch Throwable e
                   (doseq [s (rseq started)] (stop! s))
                   (throw e)))
            (recur (rest todo) (conj started n)))
        started))))

(defn stop!
  "Stop the running application name: run :prep-stop on its state, stop
  its tree, then run :stop on what :prep-stop returned.  Returns :ok, or
  nil if it is not running."
  [name]
  ;; claimed first, so of two stops only one runs the callbacks
  (let [[before _] (swap-vals! running (fn [r] (if (map? (get r name)) (dissoc r name) r)))
        a (get before name)]
    (when (map? a)
      (let [{:keys [spec top state master]} a]
        (swap! started-order (fn [o] (filterv #(not= name %) o)))
        ;; the master stops watching, so the tree's exit is not a crash
        (act/! master ::stop)
        ;; a prep-stop that throws does not keep the tree up: it stops, :stop
        ;; gets the state prep-stop was given, and then stop! throws
        (let [[state failed] (if-let [prep (:prep-stop spec)]
                               (try [(prep state)] (catch Throwable e [state e]))
                               [state])]
          (when (act/alive? top)
            (try (gs/stop! top :shutdown) (catch Throwable _ (act/exit! top :kill))))
          (when-let [stop (:stop spec)] (stop state))
          (when failed (throw failed))
          :ok)))))

(defn- stop-all!
  "Stop every running application, the last started first, as OTP stops
  them with the node: an application stops before those it depends on."
  []
  (doseq [n (rseq @started-order)]
    (try (stop! n)
         (catch Throwable e
           (logger/report! {:level :error :kind :application-stop-failed :application n :reason e})))))

(defn which-applications
  "The names of the running applications, in the order they started."
  []
  @started-order)

(defn started?
  "True while name is running."
  [name]
  (contains? @running name))

(defn exits
  "[name reason] for each application whose tree exited by itself, oldest
  first (OTP reports these; here they are kept to be read)."
  []
  @exit-log)

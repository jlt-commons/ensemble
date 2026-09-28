(ns ensemble.application
  "OTP applications: a named component with a supervision tree, started and
  stopped as a unit, that may depend on other applications.

  An application is a map, registered with load!:

      {:name          keyword, unique
       :start         (fn [] top-supervisor) or (fn [] [top-supervisor state])
       :stop          (fn [state])          run after the tree has stopped
       :applications  [name ...]            must be running first
       :type          :temporary | :transient | :permanent    (:temporary)}

  start! runs :start and has an application master monitor the tree it
  returns.  stop! stops the tree (as a supervisor stops: children in reverse
  order) and then runs :stop.  If the tree exits by itself the application
  has stopped, and its type decides the rest, as OTP's does: a :temporary
  application is only reported; a :permanent one, or a :transient one that
  exited abnormally, takes every other running application down with it --
  OTP would stop the node."
  (:require [ensemble.actor :as act :refer [receive]]
            [ensemble.gen-server :as gs]
            [ensemble.signal :as sig]))

(defonce ^:private loaded (atom {}))

(defonce ^:private running (atom {}))

(defonce ^:private exit-log (atom []))

(defn load!
  "Register an application spec.  Returns its name."
  [spec]
  (swap! loaded assoc (:name spec) spec)
  (:name spec))

(defn- fail [reason] (throw (ex-info (str "application: " (pr-str reason)) {:reason reason})))

(declare stop-all!)

(defn- master
  "The application master: monitor the tree; when it exits without stop!,
  forget the application and act on its type."
  [name top kind]
  (act/spawn
   (fn []
     (let [ref (act/monitor! top)]
       (receive
        [[:DOWN ref :process _ reason]
         (when (get @running name)
           (swap! running dissoc name)
           (swap! exit-log conj [name reason])
           (when (or (= :permanent kind)
                     (and (= :transient kind) (not (sig/shutdown? reason))))
             (stop-all!)))]
        [::stop (act/demonitor! ref {:flush true})])))))

(defn start!
  "Start the loaded application name.  Its :applications must be running.
  Throws if it is already running, not loaded, a dependency is not running,
  or its :start throws.  Returns :ok."
  [name]
  (let [spec (or (get @loaded name) (fail [:not-loaded name]))]
    (when (contains? @running name) (fail [:already-started name]))
    (doseq [dep (:applications spec)]
      (when-not (contains? @running dep) (fail [:not-started dep])))
    (let [r ((:start spec))
          [top state] (if (vector? r) r [r nil])]
      (when-not (act/actor? top) (fail [:bad-return r]))
      (swap! running assoc name {:spec spec :top top :state state
                                 :master (master name top (:type spec :temporary))})
      :ok)))

(defn ensure-all-started!
  "Start name and, first, every application it depends on, transitively,
  that is not already running.  Returns the names started, in order."
  [name]
  ;; depth first: an application goes after everything it depends on
  (let [ordered (loop [todo [name], out [], steps 0]
                  (if-let [n (first todo)]
                    (let [spec (or (get @loaded n) (fail [:not-loaded n]))
                          deps (remove (set out) (:applications spec))]
                      (when (> steps 10000) (fail [:circular-dependencies name]))
                      (if (seq deps)
                        (recur (concat deps todo) out (inc steps))
                        (recur (rest todo) (if (some #{n} out) out (conj out n)) (inc steps))))
                    out))]
    (vec (for [n ordered :when (not (contains? @running n))]
           (do (start! n) n)))))

(defn stop!
  "Stop the running application name: stop its tree, then run :stop on its
  state.  Returns :ok, or nil if it is not running."
  [name]
  (when-let [{:keys [spec top state master]} (get @running name)]
    (swap! running dissoc name)
    (act/! master ::stop)
    (when (act/alive? top)
      (try (gs/stop! top :shutdown) (catch Throwable _ (act/exit! top :kill))))
    (when-let [stop (:stop spec)] (stop state))
    :ok))

(defn- stop-all! []
  (doseq [n (keys @running)] (stop! n)))

(defn which-applications
  "The names of the running applications."
  []
  (vec (keys @running)))

(defn started?
  "True while name is running."
  [name]
  (contains? @running name))

(defn exits
  "[name reason] for each application whose tree exited by itself, oldest
  first (OTP reports these; here they are kept to be read)."
  []
  @exit-log)

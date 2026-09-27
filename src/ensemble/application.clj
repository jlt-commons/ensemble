(ns ensemble.application
  "OTP applications: a named unit with a start and a stop.

  A spec is a map:

      {:name  any                     identity, unique among running apps
       :start (fn [] state)           run once on start, returns state
       :stop  (fn [state])            run once on stop, given that state}

  Application names are tracked in a process-wide registry, so an application
  can be started, queried and stopped by name.  Starting an already-running
  application throws; stopping an unknown application is a no-op.")

(defonce ^:private running (atom {}))

(defn start-application
  "Start the application described by spec.  Runs :start once and remembers the
  state it returns, for :stop.  Throws if an application of the same :name is
  already running.  Returns the name."
  [{:keys [name start] :as spec}]
  (when (contains? @running name)
    (throw (ex-info "application already started" {:name name})))
  (let [state (start)]
    (swap! running assoc name {:spec spec :state state})
    name))

(defn stop-application
  "Stop the running application named name: run its :stop on the state its
  :start returned, then forget it.  Returns the name, or nil if name is not
  running."
  [name]
  (when-let [{:keys [spec state]} (get @running name)]
    (when-let [stop (:stop spec)] (stop state))
    (swap! running dissoc name)
    name))

(defn started?
  "True while an application named name is running."
  [name]
  (contains? @running name))

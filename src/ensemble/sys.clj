(ns ensemble.sys
  "OTP's sys: look into and steer a running behaviour -- a gen-server, a
  gen-statem, and so a supervisor or a gen-event manager -- through system
  messages it answers between its own.

  What a behaviour's state is to sys: a gen-server's state, a gen-statem's
  [state data].  A suspended process takes only system messages (and a
  stop, and its parent's exit) until it is resumed; everything else waits
  in its mailbox.

  Each fn takes an optional timeout in ms (default 5000; nil or :infinity
  waits forever) and throws as a gen-server call does when the process is
  not there or does not answer."
  (:require [ensemble.gen-server :as gs]))

(def ^:private default-timeout 5000)

(defn- ask
  [srv req timeout]
  (let [[tag v] (gs/system-call! srv req timeout)]
    (if (= :ok tag)
      v
      (throw (ex-info (str "sys request failed: " (pr-str v))
                      {:reason v :request req}
                      (when (instance? Throwable v) v))))))

(defn get-state
  "The behaviour's state."
  ([srv] (get-state srv default-timeout))
  ([srv timeout] (ask srv :get-state timeout)))

(defn replace-state!
  "Replace the behaviour's state with (f state) and return it.  When f
  throws, the state is left and this throws."
  ([srv f] (replace-state! srv f default-timeout))
  ([srv f timeout] (ask srv [:replace-state f] timeout)))

(defn get-status
  "What the process is: {:pid :name :module :status :parent :state}, its
  :status :running or :suspended, its state as the behaviour's
  format-status shows it."
  ([srv] (get-status srv default-timeout))
  ([srv timeout] (ask srv :get-status timeout)))

(defn suspend!
  "Suspend the process: it takes only system messages until resumed."
  ([srv] (suspend! srv default-timeout))
  ([srv timeout] (ask srv :suspend timeout)))

(defn resume!
  "Resume a suspended process."
  ([srv] (resume! srv default-timeout))
  ([srv timeout] (ask srv :resume timeout)))

(defn statistics
  "flag true starts counting, false stops; :get answers {:start-time
  :current-time :messages-in} (wall-clock ms), or :no-statistics."
  ([srv flag] (statistics srv flag default-timeout))
  ([srv flag timeout] (ask srv [:statistics flag] timeout)))

(defn trace!
  "While on, report each message the process takes to ensemble.logger,
  as {:kind :sys-trace :event e}."
  ([srv on] (trace! srv on default-timeout))
  ([srv on timeout] (ask srv [:trace on] timeout)))

(defn terminate!
  "Stop the process with reason, as sys:terminate, suspended or not: its
  terminate runs and it exits.  Returns :ok."
  ([srv reason] (terminate! srv reason :infinity))
  ([srv reason timeout] (gs/stop! srv reason timeout)))

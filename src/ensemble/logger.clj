(ns ensemble.logger
  "Reports, as OTP's logger receives them from the runtime and the
  behaviours.  A report is a map with a :level and a :kind:

      :crash-report           an actor's body threw: :pid, :name, :reason
      :gen-server-terminate   a gen-server stopped abnormally: :server,
                              :name, :last-message, :state, :reason
      :gen-statem-terminate   the same for a gen-statem, with :last-event,
                              :state and :data
      :supervisor-report      a supervisor's :context -- :child-terminated,
                              :start-error, :shutdown-error or :shutdown --
                              with :supervisor, :reason and the :child

  An orderly end -- :normal, :shutdown or [:shutdown term] -- is never
  reported, nor is an actor's own (exit! reason): only a throw it did not
  catch is a crash, as the emulator reports only an uncaught error.

  Every report goes to one handler, a fn of the report, which prints it to
  *err* until set-handler! installs another.  A handler that throws is
  ignored: reporting never takes the reporter down.")

(defn print-report
  "The default handler: one line per report on *err*."
  [report]
  (binding [*out* *err*]
    (println (str "=" (name (:level report :error)) " " (name (:kind report :report)) "= "
                  (pr-str (dissoc report :level :kind))))))

(defonce ^:private handler (atom print-report))

(defn set-handler!
  "Send every report to (f report) from now on.  Returns the handler it
  replaced."
  [f]
  (let [[old _] (swap-vals! handler (constantly f))] old))

(defn report!
  "Hand report to the handler.  Returns nil."
  [report]
  (try (@handler report) (catch Throwable _ nil))
  nil)

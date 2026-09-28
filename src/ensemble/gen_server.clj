(ns ensemble.gen-server
  "The OTP gen_server behaviour.  A server is a record implementing Server;
  start or start-link runs it as an actor, and clients use call! and cast!.

  Starting is synchronous, as in OTP: start returns once init has returned,
  and throws if init throws (the server then exits and a linked caller is
  unlinked first, so it is not taken down).  start-link links the server to
  the calling actor, its parent.  A server that traps exits and receives an
  exit signal from its parent terminates with the parent's reason; that is how
  a supervisor shuts a child down.

  Callbacks return tagged vectors, read by ensemble.callback/interpret:

      [:reply reply state]  [:noreply state]  [:stop reason state] ...

  each with an optional trailing timeout in ms, which runs handle-timeout if
  no message arrives in time.  [:stop reason state] terminates the server:
  terminate runs, then the server exits with reason, which is what links,
  monitors and supervisors see.  A callback that throws does the same with
  the throwable as the reason.

  A call monitors the server and waits for a reply tagged with a fresh alias,
  so a call to a dead server fails at once with :noproc, a call whose server
  dies fails with the server's reason, and a reply that comes after the call
  timed out is dropped.  A failed call throws ex-info whose data carries the
  :reason (:noproc, :timeout, :calling-self, or the server's exit reason)."
  (:require [ensemble.actor :as act :refer [receive]]
            [ensemble.callback :as cb]))

(defprotocol Server
  (init [this] "Return the server's initial state.")
  (handle-call [this request from state] "Handle a call; return a tagged vector.")
  (handle-cast [this request state] "Handle a cast; return a tagged vector.")
  (handle-info [this msg state] "Handle any other message; return a tagged vector.")
  (handle-timeout [this state] "Run when a returned timeout elapses; return a tagged vector.")
  (terminate [this reason state] "Run once as the server stops, with the reason."))

(def ^:dynamic *call-timeout*
  "How long call! waits for a reply by default, in ms (OTP's default)."
  5000)

(defn- failure [reason request]
  (ex-info (str "gen-server call failed: " (pr-str reason))
           {:reason reason :request request}
           (when (instance? Throwable reason) reason)))

(defn reply!
  "Answer a call.  from is what handle-call was given; the reply reaches the
  caller only while it is still waiting.  Returns reply."
  [from reply]
  (if-let [p (:promise from)]
    (deliver p [:reply reply])
    (act/send-alias! (:alias from) [(:alias from) reply]))
  reply)

(defn- call-from-actor [srv request timeout-ms]
  (let [ref (act/monitor! srv)
        alias (act/alias!)]
    (act/! srv [::call {:alias alias} request])
    (let [r (receive
             [[alias reply] [:ok reply]]
             [[:DOWN ref :process _ reason] [:down reason]]
             [:after timeout-ms [:down :timeout]])]
      (act/unalias! alias)
      (act/demonitor! ref {:flush true})
      (if (= :ok (first r))
        (second r)
        (throw (failure (second r) request))))))

(defn- call-from-outside [srv request timeout-ms]
  (let [p (promise)
        hook (act/on-exit! srv (fn [reason] (deliver p [:down reason])))]
    (act/! srv [::call {:promise p} request])
    (let [r (if timeout-ms (deref p timeout-ms [:down :timeout]) @p)]
      (act/cancel-exit! srv hook)
      (if (= :reply (first r))
        (second r)
        (throw (failure (second r) request))))))

(defn call!
  "Send request to the server srv (an actor or a registered name) and wait for
  its reply, up to timeout-ms (default *call-timeout*; nil waits forever).
  Throws if the server is not running, exits before replying, or the timeout
  elapses; the ex-data :reason says which."
  ([srv request] (call! srv request *call-timeout*))
  ([srv request timeout-ms]
   (let [target (try (act/resolve-dest srv)
                     (catch Throwable _ (throw (failure :noproc request))))]
     (cond
       (= target (act/self)) (throw (failure :calling-self request))
       (act/self)            (call-from-actor target request timeout-ms)
       ;; a reply from another node comes to an actor's alias: a promise
       ;; cannot cross, so a short-lived actor makes the call
       (act/remote? target)  (let [p (promise)]
                               (act/spawn (fn [] (deliver p (try [:ok (call-from-actor target request timeout-ms)]
                                                                 (catch Throwable e [:err e])))))
                               (let [[k v] @p] (if (= :ok k) v (throw v))))
       :else                 (call-from-outside target request timeout-ms)))))

(defn cast!
  "Send request to srv without waiting.  Never fails, even when the server is
  not running (as OTP's cast).  Returns :ok."
  [srv request]
  (try (act/! srv [::cast request]) (catch Throwable _ nil))
  :ok)

(defn- finish
  "Run terminate with reason and exit with it.  A terminate that throws makes
  its throwable the exit reason instead."
  [server reason state]
  (terminate server reason state)
  (act/exit! reason))

(defn- crash-reason [e] (act/reason-of e))

(defn- dispatch
  "Run the callback for one received message.  Returns the interpreted step,
  with the caller to answer, or [:Crash reason]."
  [server st m]
  (try
    (case (first m)
      :call    (cb/interpret :call (handle-call server (nth m 2) (nth m 1) st))
      :cast    (cb/interpret :other (handle-cast server (nth m 1) st))
      :timeout (cb/interpret :other (handle-timeout server st))
      :info    (cb/interpret :other (handle-info server (nth m 1) st)))
    (catch Throwable e [:Crash (crash-reason e)])))

(defn- run-loop [server parent st timeout]
  (loop [st st, t timeout]
    (let [m (receive
             [[::call from request] [:call from request]]
             [[::cast request] [:cast request]]
             [[::stop reason] [:stop reason]]
             [[:EXIT from reason] :when (and (some? parent) (= from parent)) [:parent reason]]
             [:after t [:timeout]]
             [msg [:info msg]])]
      (case (first m)
        :stop   (finish server (second m) st)
        :parent (finish server (second m) st)
        (let [step (dispatch server st m)]
          (case (first step)
            :Reply    (do (reply! (second m) (nth step 1))
                          (recur (nth step 2) (nth step 3)))
            :Continue (recur (nth step 1) (nth step 2))
            :Stop     (let [[_ reason reply? reply st*] step]
                        (when reply? (reply! (second m) reply))
                        (finish server reason st*))
            :Bad      (finish server [:bad-return-value (nth step 1)] st)
            :Crash    (let [reason (nth step 1)]
                        (terminate server reason st)
                        (act/exit! reason))))))))

(defn- run [server parent ack timeout]
  (let [st (try (init server)
                (catch Throwable e
                  (when parent (act/unlink! parent))
                  (deliver ack [:error (act/reason-of e)])
                  (act/exit! (act/reason-of e))))]
    (deliver ack [:ok])
    (run-loop server parent st timeout)))

(defn- start* [server {:keys [name timeout trap]} link?]
  (let [ack (promise)
        parent (when link? (act/self))
        srv (try
              (act/spawn (fn [] (run server parent ack timeout))
                         {:name name :link link? :trap trap})
              (catch Throwable e
                (if-let [holder (and name (act/whereis name))]
                  (throw (ex-info "gen-server already started"
                                  {:reason [:already-started holder]}))
                  (throw e))))
        hook (act/on-exit! srv (fn [reason] (deliver ack [:error reason])))
        r @ack]
    (act/cancel-exit! srv hook)
    (if (= :ok (first r))
      srv
      (throw (ex-info (str "gen-server init failed: " (pr-str (second r)))
                      {:reason (second r)}
                      (when (instance? Throwable (second r)) (second r)))))))

(defn start
  "Start server as a new actor, returning it once init has returned.  Throws
  if init throws, or if :name is taken ({:reason [:already-started actor]}).
  Options:

      :name     register the server under this name
      :timeout  a timeout armed before the first message, as a callback's
      :trap     trap exits from the start (a server that must terminate
                cleanly when its parent stops it traps exits)"
  ([server] (start server {}))
  ([server opts] (start* server opts false)))

(defn start-link
  "start, and link the server to the calling actor, which becomes its parent."
  ([server] (start-link server {}))
  ([server opts] (start* server opts true)))

(defn stop!
  "Stop srv with reason (default :normal) and wait, up to timeout-ms (default
  forever), until it has exited: terminate runs and the server exits with
  reason.  Throws if srv is not running, or exits with some other reason.
  Returns :ok."
  ([srv] (stop! srv :normal nil))
  ([srv reason] (stop! srv reason nil))
  ([srv reason timeout-ms]
   (let [target (try (act/resolve-dest srv)
                     (catch Throwable _ (throw (failure :noproc [:stop reason]))))
         p (promise)
         hook (act/on-exit! target (fn [r] (deliver p r)))]
     (if (= :noproc (deref p 0 nil))
       (throw (failure :noproc [:stop reason]))
       (do
         (act/! target [::stop reason])
         (let [r (if timeout-ms (deref p timeout-ms ::timeout) @p)]
           (act/cancel-exit! target hook)
           (cond
             (= ::timeout r) (throw (failure :timeout [:stop reason]))
             (= reason r) :ok
             :else (throw (failure r [:stop reason])))))))))

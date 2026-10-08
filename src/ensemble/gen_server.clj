(ns ensemble.gen-server
  "The OTP gen_server behaviour.  A server is a record implementing Server;
  start or start-link runs it as an actor, and clients use call! and cast!.

  Starting is synchronous, as in OTP: start returns once init has returned,
  or fails with :timeout if init takes longer than the :timeout option.
  init returns [:ok state] or [:ok state action]; [:stop reason] and
  [:error reason] make start throw {:reason reason}, and :ignore makes it
  return :ignore.  A throw from init is [:stop the-throwable].  The server
  then exits (with reason for :stop, :normal otherwise), and a linked caller
  is unlinked first, so it is not taken down.  start-link links the server to
  the calling actor, its parent.  A server that traps exits and receives an
  exit signal from its parent terminates with the parent's reason; that is how
  a supervisor shuts a child down.

  Callbacks return tagged vectors, read by ensemble.callback/interpret:

      [:reply reply state]  [:noreply state]  [:stop reason state] ...

  each with an optional trailing action: a timeout in ms, which runs
  handle-timeout if no message arrives in time; :hibernate; or [:continue
  c], which runs handle-continue with c before another message is taken.  [:stop reason state] terminates the server:
  terminate runs, then the server exits with reason, which is what links,
  monitors and supervisors see.  A callback that throws does the same with
  the throwable as the reason.

  A call monitors the server and waits for a reply tagged with a fresh alias,
  so a call to a dead server fails at once with :noproc, a call whose server
  dies fails with the server's reason, and a reply that comes after the call
  timed out is dropped.  A failed call throws ex-info whose data carries the
  :reason (:noproc, :timeout, :calling-self, or the server's exit reason)."
  (:require [ensemble.actor :as act :refer [receive]]
            [ensemble.callback :as cb]
            [ensemble.logger :as logger]
            [ensemble.request :as rq]
            [ensemble.signal :as sig]))

(defprotocol Server
  (init [this] "Return [:ok state], [:ok state action], [:stop reason], [:error reason] or :ignore.")
  (handle-call [this request from state] "Handle a call; return a tagged vector.")
  (handle-cast [this request state] "Handle a cast; return a tagged vector.")
  (handle-info [this msg state] "Handle any other message; return a tagged vector.")
  (handle-timeout [this state] "Run when a returned timeout elapses; return a tagged vector.")
  (handle-continue [this c state] "Run for a returned [:continue c], before another message; return a tagged vector.")
  (terminate [this reason state] "Run once as the server stops, with the reason."))

(defprotocol FormatStatus
  (format-status [this status]
    "Optional, as OTP's format_status/1: take a map of what a status or a
    report shows -- :state, and in a report :message and :reason -- and
    return it as it should be shown, a secret left out, say."))

(defn formatted
  "status as behaviour b would have it shown."
  [b status]
  (if (satisfies? FormatStatus b) (try (format-status b status) (catch Throwable _ status)) status))

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

(defn- call-from-actor [tag srv request timeout-ms]
  (let [ref (act/monitor! srv)
        alias (act/alias!)]
    (act/! srv [tag {:alias alias} request])
    (let [r (receive
             [[alias reply] [:ok reply]]
             [[:DOWN ref :process _ reason] [:down reason]]
             [:after timeout-ms [:down :timeout]])]
      (act/unalias! alias)
      (act/demonitor! ref {:flush true})
      (if (= :ok (first r))
        (second r)
        (throw (failure (second r) request))))))

(defn- call-from-outside [tag srv request timeout-ms]
  (let [p (promise)
        hook (act/on-exit! srv (fn [reason] (deliver p [:down reason])))]
    (act/! srv [tag {:promise p} request])
    (let [r (if timeout-ms (deref p timeout-ms [:down :timeout]) @p)]
      (act/cancel-exit! srv hook)
      (if (= :reply (first r))
        (second r)
        (throw (failure (second r) request))))))

(defn- request!
  "Send [tag from request] to srv and wait for the reply, as call! does."
  [tag srv request timeout-ms]
  (let [timeout-ms (act/timeout-ms timeout-ms)
        target (try (act/resolve-dest srv)
                    (catch Throwable _ (throw (failure :noproc request))))]
    (cond
      (= target (act/self)) (throw (failure :calling-self request))
      (act/self)            (call-from-actor tag target request timeout-ms)
      ;; a reply from another node comes to an actor's alias: a promise
      ;; cannot cross, so a short-lived actor makes the call
      (act/remote? target)  (let [p (promise)]
                              (act/spawn (fn [] (deliver p (try [:ok (call-from-actor tag target request timeout-ms)]
                                                                (catch Throwable e [:err e])))))
                              (let [[k v] @p] (if (= :ok k) v (throw v))))
      :else                 (call-from-outside tag target request timeout-ms))))

(defn call!
  "Send request to the server srv (an actor or a registered name) and wait for
  its reply, up to timeout-ms (default *call-timeout*; nil or :infinity
  waits forever).
  Throws if the server is not running, exits before replying, or the timeout
  elapses; the ex-data :reason says which."
  ([srv request] (call! srv request *call-timeout*))
  ([srv request timeout-ms] (request! ::call srv request timeout-ms)))

(defn system-call!
  "Send a system message, as sys does, to a behaviour's process and wait
  for its answer.  See ensemble.sys."
  [srv request timeout-ms]
  (request! ::system srv request timeout-ms))

;; --- system messages ------------------------------------------------------------
;; A behaviour's process answers sys's requests between its own messages.
;; What it keeps for them, dbg, is {:suspended bool :trace bool :stats
;; {:start-time ms :messages-in n} or nil}.

(def no-debug {:suspended false :trace false :stats nil})

(defn handle-system
  "Answer the system request req from from, in a behaviour whose state as
  sys sees it is st and whose status (status dbg) describes.  Returns [dbg
  st]: what the process keeps from now on."
  [dbg req from st status]
  (let [answer (fn [v] (reply! from v))
        [tag arg] (if (vector? req) req [req])]
    (case tag
      :get-state (do (answer [:ok st]) [dbg st])
      :replace-state (let [r (try [:ok (arg st)] (catch Throwable e [:error e]))]
                       (answer r)
                       [dbg (if (= :ok (first r)) (second r) st)])
      :get-status (do (answer [:ok (status dbg)]) [dbg st])
      :suspend (do (answer [:ok :ok]) [(assoc dbg :suspended true) st])
      :resume (do (answer [:ok :ok]) [(assoc dbg :suspended false) st])
      :trace (do (answer [:ok :ok]) [(assoc dbg :trace (boolean arg)) st])
      :statistics (case arg
                    true (do (answer [:ok :ok])
                             [(assoc dbg :stats {:start-time (System/currentTimeMillis) :messages-in 0}) st])
                    false (do (answer [:ok :ok]) [(assoc dbg :stats nil) st])
                    (do (answer [:ok (if-let [s (:stats dbg)]
                                       (assoc s :current-time (System/currentTimeMillis))
                                       :no-statistics)])
                        [dbg st]))
      (do (answer [:error [:unknown-system-msg req]]) [dbg st]))))

(defn observed
  "dbg once the process has taken event, an ordinary message: counted, and
  reported while tracing."
  [dbg event]
  (when (:trace dbg)
    (logger/report! {:level :debug :kind :sys-trace :pid (act/self)
                     :name (act/registered-name (act/self)) :event event}))
  (if (:stats dbg) (update-in dbg [:stats :messages-in] inc) dbg))

;; --- asynchronous calls ------------------------------------------------------

(defn send-request
  "Send request to srv without waiting, as gen_server:send_request, and
  return its request id, which check-response, wait-response and
  receive-response take.  Run it in an actor: the reply comes to an alias
  of it, and it monitors srv.  With a label and a collection (reqids-new),
  adds the request to the collection and returns the collection."
  ([srv request]
   (let [target (try (act/resolve-dest srv) (catch Throwable _ nil))
         alias (act/alias!)]
     (if target
       (let [mref (act/monitor! target)]
         (act/! target [::call {:alias alias} request])
         [:Req alias mref srv])
       ;; a name nobody holds: the request fails as a call to a dead server
       (let [mref (act/make-ref)]
         (act/! (act/self) [:DOWN mref :process srv :noproc])
         [:Req alias mref srv]))))
  ([srv request label coll] (conj coll [(send-request srv request) label])))

(defn- done! [[_ alias mref]]
  (act/unalias! alias)
  (act/demonitor! mref {:flush true}))

(defn- response
  "An answer as OTP gives it: [:reply r] or [:error [reason server]]."
  [a]
  (case (first a)
    :Reply [:reply (second a)]
    :Failed [:error [(nth a 1) (nth a 2)]]))

(defn reqids-new "An empty request id collection." [] [])
(defn reqids-add "coll with request id req under label." [req label coll] (conj coll [req label]))
(defn reqids-size "How many requests coll holds." [coll] (count coll))
(defn reqids-to-list "coll as [[req label] ...]." [coll] coll)

(defn check-response
  "What msg, a message just received, is to request req: [:reply r],
  [:error [reason server]], or :no-reply.  With a collection and delete?:
  [response label coll'], :no-request for an empty collection, or
  :no-reply."
  ([msg req]
   (let [a (rq/answer msg req)]
     (if (= :NoReply (first a)) :no-reply (do (done! req) (response a)))))
  ([msg coll delete?]
   (let [c (rq/check msg coll delete?)]
     (case (first c)
       :NoRequest :no-request
       :NotOurs :no-reply
       :Response (let [[_ a label req coll*] c]
                   (when delete? (done! req))
                   [(response a) label coll*])))))

(defn- await-answer
  "Wait up to timeout-ms (nil: for ever) for a message that answers.
  [:msg m] or [:timeout]."
  [answers? timeout-ms]
  (receive
   [m :when (answers? m) [:msg m]]
   [:after timeout-ms [:timeout]]))

(defn wait-response
  "Wait up to timeout-ms for the answer to req: [:reply r], [:error
  [reason server]], or :timeout, which keeps the request."
  ([req timeout-ms]
   (let [r (await-answer #(not= :NoReply (first (rq/answer % req))) timeout-ms)]
     (if (= :timeout (first r)) :timeout (check-response (second r) req))))
  ([coll timeout-ms delete?]
   (if (empty? coll)
     :no-request
     (let [r (await-answer #(= :Response (first (rq/check % coll delete?))) timeout-ms)]
       (if (= :timeout (first r)) :timeout (check-response (second r) coll delete?))))))

(defn receive-response
  "wait-response, but a timeout abandons the request (every request of
  the collection): a late answer is dropped."
  ([req timeout-ms]
   (let [r (wait-response req timeout-ms)]
     (when (= :timeout r) (done! req))
     r))
  ([coll timeout-ms delete?]
   (let [r (wait-response coll timeout-ms delete?)]
     (when (= :timeout r) (run! (fn [[req _]] (done! req)) coll))
     r)))

(defn cast!
  "Send request to srv without waiting.  Never fails, even when the server is
  not running (as OTP's cast).  Returns :ok."
  [srv request]
  (try (act/! srv [::cast request]) (catch Throwable _ nil))
  :ok)

(defn multi-call
  "Call the server registered as nm on each of nodes (default: this node
  and every node it is connected to) at once, as gen_server:multi_call,
  waiting up to timeout-ms (default :infinity) for all.  Returns [replies
  bad-nodes]: replies as [[node reply] ...], and the nodes whose server
  was not there, failed, or did not answer in time."
  ([nm request] (multi-call (into [(act/node)] (act/nodes)) nm request :infinity))
  ([nodes nm request] (multi-call nodes nm request :infinity))
  ([nodes nm request timeout-ms]
   (let [here (act/node)
         asks (mapv (fn [n]
                      (let [p (promise)]
                        (binding [act/*node* here]
                          (act/spawn (fn [] (deliver p (try [:ok (call! [:At nm n] request timeout-ms)]
                                                            (catch Throwable _ [:bad]))))))
                        [n p]))
                    nodes)
         rs (mapv (fn [[n p]] [n @p]) asks)]
     [(vec (for [[n [k v]] rs :when (= :ok k)] [n v]))
      (vec (for [[n [k]] rs :when (= :bad k)] n))])))

(defn abcast
  "Cast request to the server registered as nm on each of nodes (default:
  this node and its peers), as gen_server:abcast.  Returns :abcast."
  ([nm request] (abcast (into [(act/node)] (act/nodes)) nm request))
  ([nodes nm request]
   (doseq [n nodes] (cast! [:At nm n] request))
   :abcast))

(defn- last-message
  "The message a server was handling as it stopped, as a report shows it."
  [m]
  (case (first m)
    :call [:call (nth m 2)]
    m))

(defn- report-terminate!
  "Report a server stopping with reason after m, unless the stop is orderly."
  [server reason m state]
  (when-not (sig/shutdown? reason)
    (let [shown (formatted server {:state state :message (last-message m) :reason reason})]
      (logger/report! {:level :error :kind :gen-server-terminate :server (act/self)
                       :name (act/registered-name (act/self)) :last-message (:message shown)
                       :state (:state shown) :reason (:reason shown)}))))

(defn- finish
  "Run terminate with reason and exit with it, reporting an abnormal stop
  after message m.  A terminate that throws makes its throwable the exit
  reason instead."
  [server reason state m]
  (let [reason (try (terminate server reason state) reason
                    (catch Throwable e (act/reason-of e)))]
    (report-terminate! server reason m state)
    (act/exit! reason)))

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
      :continue (cb/interpret :other (handle-continue server (nth m 1) st))
      :info    (cb/interpret :other (handle-info server (nth m 1) st)))
    (catch Throwable e [:Crash (crash-reason e)])))

(defn system-message
  "While suspended, the next message a behaviour may take: a system
  message, a stop, or its parent's exit, as [:system from req], [:stop
  reason] or [:parent reason]."
  [parent]
  (receive
   [[::system from req] [:system from req]]
   [[::stop reason] [:stop reason]]
   [[:EXIT from reason] :when (and (some? parent) (= from parent)) [:parent reason]]))

(defn- next-message
  "What the server handles next, once a callback asked for [:Wait t] or
  [:Cont c]: a pending continue before anything in the mailbox, else the
  next message, or [:timeout] when t elapses first.  With no t and a
  hibernate-after, [:idle] when that long passes with no message.  A
  suspended server takes only system-message's."
  [parent next hibernate-after suspended?]
  (cond
    suspended? (system-message parent)
    (= :Cont (first next)) [:continue (second next)]
    :else
    (let [t (second next)]
      (receive
       [[::call from request] [:call from request]]
       [[::cast request] [:cast request]]
       [[::stop reason] [:stop reason]]
       [[::system from req] [:system from req]]
       [[:EXIT from reason] :when (and (some? parent) (= from parent)) [:parent reason]]
       [:after (or t hibernate-after) (if t [:timeout] [:idle])]
       [msg [:info msg]]))))

(defn- status-of
  "What sys/get-status shows of a server."
  [server parent st dbg]
  {:pid (act/self) :name (act/registered-name (act/self)) :module (type server)
   :status (if (:suspended dbg) :suspended :running) :parent parent
   :state (:state (formatted server {:state st}))})

(defn- run-loop
  "Serve until the server stops.  A :hibernate action, or idle ms with no
  message, hibernates the process: its stack goes, and the next message
  runs this again.  idle is {:after ms :disk? bool}; with :disk? an idle
  server is passivated, kept on disk until its next message.  dbg is what
  it keeps for system messages."
  ([server parent st next idle] (run-loop server parent st next idle no-debug))
  ([server parent st next idle dbg]
   (loop [st st, next next, dbg dbg]
     (when (= :Hibernate (first next))
       (act/hibernate! run-loop server parent st [:Wait nil] idle dbg))
     (let [m (next-message parent next (:after idle) (:suspended dbg))]
       (case (first m)
         :idle   (if (:disk? idle)
                   (act/passivate! run-loop server parent st [:Wait nil] idle dbg)
                   (act/hibernate! run-loop server parent st [:Wait nil] idle dbg))
         :stop   (finish server (second m) st m)
         :parent (finish server (second m) st [:EXIT parent (second m)])
         :system (let [[_ from req] m
                       [dbg st] (handle-system dbg req from st #(status-of server parent st %))]
                   ;; a pending continue or timeout is kept across a system message
                   (recur st next dbg))
         (let [dbg (observed dbg (last-message m))
               step (dispatch server st m)]
           (case (first step)
             :Reply   (do (reply! (second m) (nth step 1))
                          (recur (nth step 2) (nth step 3) dbg))
             :NoReply (recur (nth step 1) (nth step 2) dbg)
             :Stop    (let [[_ reason reply? reply st*] step]
                        (when reply? (reply! (second m) reply))
                        (finish server reason st* m))
             :Bad     (finish server [:bad-return-value (nth step 1)] st m)
             :Crash   (finish server (nth step 1) st m))))))))

(defn- run [server parent ack idle]
  (let [r (try (cb/interpret-init (init server))
               (catch Throwable e (let [why (act/reason-of e)] [:Fail why why])))
        quit! (fn [answer exit]
                (when parent (act/unlink! parent))
                (deliver ack answer)
                (act/exit! exit))]
    (case (first r)
      :Start  (let [[_ st next] r]
                (deliver ack [:ok])
                (run-loop server parent st next idle))
      :Ignore (quit! [:ignore] :normal)
      :Fail   (let [[_ why exit] r] (quit! [:error why] exit)))))

(defn enter-loop
  "Make the calling actor server, with state, as gen_server:enter_loop:
  init is not called.  action is one init could return with its state (a
  timeout, :hibernate or [:continue c]).  opts: :name, which the actor must
  already be registered under (else it exits :process-not-registered),
  :hibernate-after, and :parent, the actor whose exit stops it.  Never
  returns."
  ([server opts state] (enter-loop server opts state nil))
  ([server {:keys [name parent hibernate-after]} state action]
   (when (and name (not= (act/self) (act/whereis name)))
     (act/exit! :process-not-registered))
   (let [r (cb/interpret-init (if (some? action) [:ok state action] [:ok state]))]
     (when-not (= :Start (first r)) (act/exit! [:bad-return-value action]))
     (run-loop server parent state (nth r 2) (when hibernate-after {:after hibernate-after :disk? false})))))

(defn await-init
  "What a behaviour's start answers once its process srv has run init, which
  delivers [:ok], [:ignore] or [:error reason] to ack: srv, :ignore, or a
  throw of {:reason reason}.  With timeout ms (nil or :infinity: none), an
  init still running then is killed -- unlinked first, so a linked starter
  is not taken down -- and the start fails with :timeout, as OTP's
  {timeout, T} start option.  what names the behaviour in the message."
  [srv ack timeout what]
  (let [hook (act/on-exit! srv (fn [reason] (deliver ack [:error reason])))
        t (when-not (= :infinity timeout) timeout)
        r (if t (deref ack t [:timeout]) @ack)]
    (act/cancel-exit! srv hook)
    (case (first r)
      :ok srv
      :ignore :ignore
      :timeout (do (when (act/self) (act/unlink! srv))
                   (act/exit! srv :kill)
                   (act/exit-reason srv)
                   (throw (ex-info (str what " init timed out") {:reason :timeout})))
      (throw (ex-info (str what " init failed: " (pr-str (second r)))
                      {:reason (second r)}
                      (when (instance? Throwable (second r)) (second r)))))))

(defn- start* [server {:keys [name timeout trap hibernate-after passivate-after]} link?]
  (let [ack (promise)
        parent (when link? (act/self))
        srv (try
              (act/spawn (fn [] (run server parent ack
                                     (cond passivate-after {:after passivate-after :disk? true}
                                           hibernate-after {:after hibernate-after :disk? false})))
                         {:name name :link link? :trap trap :initial-call [(type server) :init]})
              (catch Throwable e
                (if-let [holder (and name (act/whereis name))]
                  (throw (ex-info "gen-server already started"
                                  {:reason [:already-started holder]}))
                  (throw e))))]
    (await-init srv ack timeout "gen-server")))

(defn start
  "Start server as a new actor, returning it once init has returned, or
  :ignore when init returned :ignore.  Throws {:reason r} if init failed
  with r, or if :name is taken ({:reason [:already-started actor]}).
  Options:

      :name     register the server under this name
      :timeout  ms init may take (default :infinity); a slower init is
                killed and start throws {:reason :timeout}, as OTP's
      :trap     trap exits from the start (a server that must terminate
                cleanly when its parent stops it traps exits)
      :hibernate-after  hibernate after this many ms with no message, as
                OTP's hibernate_after
      :passivate-after  the same, to disk (see ensemble.actor/passivate!)"
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
   (let [timeout-ms (act/timeout-ms timeout-ms)
         target (try (act/resolve-dest srv)
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

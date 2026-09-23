(ns ensemble.gen-server
  "The OTP gen_server behaviour: a server is a record implementing Server, an
  ensemble.actor runs the loop, and callers use call! / cast!.

  A handler returns a tagged vector:

      [:reply reply new-state]        [:reply reply new-state timeout-ms]
      [:noreply new-state]            [:noreply new-state timeout-ms]
      [:stop reason new-state]

  A timeout-ms arms handle-timeout: if no call, cast or info arrives within
  that many milliseconds, the loop runs handle-timeout.  A bare value is read
  as [:noreply value]."
  (:require [clojure.core.async :as a]
            [ensemble.actor :as act]))

(defprotocol Server
  (init [this] "Return the server's initial state.")
  (handle-call [this from msg state] "Handle a synchronous call; return a tagged vector.")
  (handle-cast [this msg state] "Handle an asynchronous cast; return a tagged vector.")
  (handle-info [this msg state] "Handle a plain message; return a tagged vector.")
  (handle-timeout [this state] "Run when a handler's timeout elapses; return a tagged vector.")
  (terminate [this reason state] "Run once on :stop, before the server exits."))

(defn reply!
  "Send val to a caller's reply channel, as carried by the from passed to
  handle-call."
  [from val]
  (a/>!! (:chan from) [:ensemble/reply val]))

(def ^:dynamic *call-timeout*
  "How long call! waits for a reply before throwing."
  5000)

(defn reply-error!
  "Send an error to a caller's reply channel so its call! rethrows it."
  [from e]
  (when (and from (:chan from)) (a/>!! (:chan from) [:ensemble/error e])))

(defn- do-call [srv msg timeout-ms]
  (let [c (a/chan 1)]
    (act/! srv [:call {:chan c} msg])
    (let [[v _] (a/alts!! [c (a/timeout timeout-ms)])]
      (cond
        (nil? v) (throw (ex-info "gen-server call timed out" {:msg msg}))
        (and (vector? v) (= :ensemble/error (first v))) (throw (nth v 1))
        (and (vector? v) (= :ensemble/reply (first v))) (nth v 1)
        :else (throw (ex-info "gen-server reply was not tagged" {:reply v}))))))

(defn call!
  "Synchronously call a gen-server.  Blocks the caller until it replies, up to
  *call-timeout* milliseconds.  Extra arguments pack into a vector like !:
  (call! s :add 5) sends [:add 5].  If the handler throws, call! rethrows it."
  ([srv msg] (do-call srv msg *call-timeout*))
  ([srv msg arg & args] (do-call srv (into [msg arg] args) *call-timeout*)))

(defn call-timed!
  "Like call! but with an explicit timeout in milliseconds, overriding
  *call-timeout*."
  ([srv timeout-ms msg] (do-call srv msg timeout-ms))
  ([srv timeout-ms msg arg & args] (do-call srv (into [msg arg] args) timeout-ms)))

(defn cast!
  "Asynchronously send msg to a gen-server.  Returns the server.  Extra
  arguments pack into a vector like !."
  ([srv msg] (act/! srv [:cast msg]) srv)
  ([srv msg arg & args] (act/! srv [:cast (into [msg arg] args)]) srv))

(defn shutdown!
  "Stop the server from outside, as if it had returned [:stop reason st].
  terminate runs with reason (default nil, a normal termination) and the
  server's current state; join returns reason."
  ([srv] (shutdown! srv nil))
  ([srv reason]
   (act/! srv [:ensemble/shutdown reason])
   srv))

(defn- normalize [ret]
  (if (vector? ret) ret [:noreply ret]))

(defn- run-server [server tx]
  (loop [st (init server)
         tx tx]
    (let [msg (act/receive
               [[:call from msg] (try
                                   {:from from :ret (handle-call server from msg st)}
                                   (catch Throwable e {:from from :crash e}))]
               [[:cast msg] (try {:ret (handle-cast server msg st)}
                                 (catch Throwable e {:crash e}))]
               [[:info msg] (try {:ret (handle-info server msg st)}
                                 (catch Throwable e {:crash e}))]
               [[:ensemble/shutdown reason] {:ret [:stop reason st]}]
               [:after tx (try {:ret (handle-timeout server st)}
                               (catch Throwable e {:crash e}))]
               [other (try {:ret (handle-info server other st)}
                           (catch Throwable e {:crash e}))])]
      (if (:crash msg)
        (do (reply-error! (:from msg) (:crash msg))
            (terminate server (:crash msg) st)
            (throw (:crash msg)))
        (let [ret (normalize (:ret msg))]
          (case (first ret)
            :stop (do (terminate server (nth ret 1) (nth ret 2))
                      (nth ret 1))
            :reply (do (reply! (:from msg) (nth ret 1))
                       (recur (nth ret 2) (nth ret 3 nil)))
            :noreply (recur (nth ret 1) (nth ret 2 nil))))))))

(defn gen-server
  "Start a gen-server on a fiber.  Returns its actor.  Options are actor/spawn
  options (:name, :state) plus :timeout, an initial timeout-ms armed before the
  first message."
  ([server] (gen-server server {}))
  ([server opts]
   (act/spawn (fn [] (run-server server (:timeout opts nil))) opts)))

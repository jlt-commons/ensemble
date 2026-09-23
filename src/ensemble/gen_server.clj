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
  (a/>!! (:chan from) val))

(def ^:dynamic *call-timeout*
  "How long call! waits for a reply before throwing."
  5000)

(defn call!
  "Synchronously call a gen-server.  Blocks the caller until it replies."
  [srv msg]
  (let [c (a/chan 1)]
    (act/! srv [:call {:chan c} msg])
    (let [[v _] (a/alts!! [c (a/timeout *call-timeout*)])]
      (if (nil? v)
        (throw (ex-info "gen-server call timed out" {:msg msg}))
        v))))

(defn cast!
  "Asynchronously send msg to a gen-server.  Returns the server."
  [srv msg]
  (act/! srv [:cast msg])
  srv)

(defn- normalize [ret]
  (if (vector? ret) ret [:noreply ret]))

(defn- run-server [server tx]
  (loop [st (init server)
         tx tx]
    (let [msg (act/receive
               [[:call from msg] {:from from :ret (handle-call server from msg st)}]
               [[:cast msg] {:ret (handle-cast server msg st)}]
               [[:info msg] {:ret (handle-info server msg st)}]
               [:after tx {:ret (handle-timeout server st)}])
          ret (normalize (:ret msg))]
      (case (first ret)
        :stop (do (terminate server (nth ret 1) (nth ret 2))
                  (nth ret 1))
        :reply (do (reply! (:from msg) (nth ret 1))
                   (recur (nth ret 2) (nth ret 3 nil)))
        :noreply (recur (nth ret 1) (nth ret 2 nil))))))

(defn gen-server
  "Start a gen-server on a fiber.  Returns its actor.  Options are actor/spawn
  options (:name, :state) plus :timeout, an initial timeout-ms armed before the
  first message."
  ([server] (gen-server server {}))
  ([server opts]
   (act/spawn (fn [] (run-server server (:timeout opts nil))) opts)))

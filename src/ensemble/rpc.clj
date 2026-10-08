(ns ensemble.rpc
  "Run a fn on another node, as OTP's erpc: each call runs in a process of
  its own there, started by spawn-on, so the fn is one the node allows
  (ensemble.node/start!'s :spawn) and named by a symbol.

  A failed call throws ex-info whose :reason is :noconnection (the node
  cannot be reached, or went down during the call), :timeout (the
  process running it is then killed), [:not-allowed sym], [:exception e]
  for a throw (e as data: {:ensemble.node/exception message ...}), or
  [:exit reason] for an exit."
  (:require [ensemble.actor :as act :refer [receive]]
            [ensemble.node :as node]))

(defn- fail [reason]
  (throw (ex-info (str "rpc failed: " (pr-str reason)) {:reason reason})))

(defn- in-actor
  "(f) run in an actor: this one, or a fresh one when there is none."
  [f]
  (if (act/self)
    (f)
    (let [p (promise)]
      (act/spawn (fn [] (deliver p (node/outcome f))))
      (let [[k v] @p]
        (case k :ok v :exit (act/exit! v) :exception (throw v))))))

(defn- start
  "Start (apply f args) on node n, answering to the current actor under
  ref; its pid."
  [n f-sym args ref]
  (if (= n (act/node))
    (let [me (act/self)
          f (requiring-resolve f-sym)]
      (act/spawn (fn [] (act/! me [::node/result ref (node/outcome #(apply f args))]))))
    (try (node/spawn-on n f-sym args {:reply ref})
         (catch Throwable e
           (let [r (:reason (ex-data e))]
             (fail (if (= :timeout r) :noconnection r)))))))

(defn call
  "(apply f args) on node n, f named by f-sym, and its value, waiting up to
  timeout-ms (default :infinity).  Throws as the ns doc says."
  ([n f-sym args] (call n f-sym args :infinity))
  ([n f-sym args timeout-ms]
   (in-actor
    (fn []
      (let [ref (act/make-ref)
            pid (start n f-sym args ref)
            mref (act/monitor! pid)
            r (receive
               [[::node/result ref r] r]
               [[:DOWN mref :process _ reason] [:down reason]]
               [:after (act/timeout-ms timeout-ms) [:timeout]])]
        (act/demonitor! mref {:flush true})
        (case (first r)
          :ok (second r)
          :exception (fail [:exception (second r)])
          :exit (fail [:exit (second r)])
          :down (fail (if (= :noconnection (second r)) :noconnection [:exit (second r)]))
          :timeout (do (act/exit! pid :kill) (fail :timeout))))))))

(defn cast
  "Start (apply f args) on node n and do not wait, as erpc:cast.  Returns
  true, whether or not the node could be reached."
  [n f-sym args]
  (try
    (if (= n (act/node))
      (let [f (requiring-resolve f-sym)] (act/spawn #(apply f args)))
      (node/spawn-on n f-sym args))
    (catch Throwable _ nil))
  true)

(defn multicall
  "call on each of nodes at once, waiting up to timeout-ms (default
  :infinity) for all.  A result per node, in order: [:ok value] or
  [:error reason]."
  ([nodes f-sym args] (multicall nodes f-sym args :infinity))
  ([nodes f-sym args timeout-ms]
   (let [here (act/node)
         ps (mapv (fn [n]
                    (let [p (promise)]
                      (binding [act/*node* here]
                        (act/spawn (fn [] (deliver p (try [:ok (call n f-sym args timeout-ms)]
                                                          (catch Throwable e
                                                            [:error (or (:reason (ex-data e)) e)]))))))
                      p))
                  nodes)]
     (mapv deref ps))))

(ns ensemble.node
  "Nodes: ensemble processes on other runtimes, as Erlang's distribution.

  A node is a named runtime, its name a keyword that reads back as itself
  (:shop.host, not Erlang's shop@host: @ ends a token in Clojure's reader,
  and edn refuses it).  A pid of another node is a RemotePid, and it
  is a process like any other (ensemble.process): !, link!, monitor! and
  exit! on it send the operation as a frame over the connection to its
  node, which performs it on the local process there.  So a gen-server
  call, a supervisor's link to its child, or a monitor works the same
  whether the other end is in this VM, another OS process or another
  machine.  Where each operation goes, what a pid in a message is on
  arrival, and what a lost connection does are decided by ensemble.dist.

  Connections come from a Transport.  The loopback transport connects nodes
  running in one VM, each frame still printed and read back, so what
  crosses is exactly what a wire carries; a byte-stream transport (TCP
  between OS processes or machines) plugs in the same way.

  As in Erlang:
  - a pid in a message crosses as data and arrives as a pid again;
  - a link to a process that is gone, or on a node that cannot be reached,
    gives noproc or noconnection;
  - when a connection is lost, every link across it breaks with
    noconnection, every monitor across it fires with noconnection, and
    every monitor_node of the peer gets [:nodedown peer];
  - (spawn-on node f-sym args) starts a process there, from a fn named by
    symbol, as spawn(Node, M, F, A) does."
  (:require [clojure.core.async :as a]
            [clojure.edn :as edn]
            [clojure.walk :as walk]
            [ensemble.actor :as act]
            [ensemble.dist :as dist]
            [ensemble.process :as proc]))

;; --- transports ---------------------------------------------------------

(defprotocol Transport
  (-connect [t from to on-frame on-close]
    "Open a connection from node from to node to.  Frames the peer sends
    are handed to (on-frame frame-string); (on-close) runs once when the
    connection is lost.  Returns a Conn, or nil when to cannot be
    reached."))

(defprotocol Conn
  (-send-frame [c s] "Send the frame string s.  False once the connection is lost.")
  (-close [c] "Close the connection; both ends' on-close run."))

;; --- node state ---------------------------------------------------------

(defonce ^:private nodes
  ;; name -> {:transport t :conns {peer conn} :watching {ref {...}}
  ;;          :exported {ref [peer id]} :node-mons [[id peer]] :pending {ref promise}}
  (atom {}))

(defn- state [n] (get @nodes n))

(defn started? "Is n a node running in this VM?" [n] (contains? @nodes n))

(declare deliver-remote signal-remote link-remote unlink-remote monitor-remote demonitor-remote
         monitor-named demonitor-named send-frame!)

(defrecord RemotePid [node id]
  proc/Process
  (-pid [_] id)
  (-node [_] node)
  (-alive? [_] (contains? (get-in @nodes [(act/node) :conns]) node))
  (-deliver [p msg] (deliver-remote p msg))
  (-signal [p from kind reason checked] (signal-remote p from kind reason checked))
  (-link [p from] (link-remote p from))
  (-unlink [p from] (unlink-remote p from))
  (-add-monitor [p ref notify] (monitor-remote p ref notify))
  (-drop-monitor [p ref] (demonitor-remote p ref)))

(defmethod print-method RemotePid [p ^java.io.Writer w]
  (.write w (str "#<actor " (name (:node p)) "/" (:id p) ">")))

(defrecord RemoteName [node nm]
  proc/Process
  (-pid [_] nil)
  (-node [_] node)
  (-alive? [_] (contains? (get-in @nodes [(act/node) :conns]) node))
  (-deliver [_ msg] (send-frame! (act/node) node [:send-named nm msg]))
  (-signal [_ _ _ _ _] (throw (ex-info "an exit signal goes to a pid, not a name" {:reason :badarg})))
  (-link [_ _] (throw (ex-info "a link goes to a pid, not a name" {:reason :badarg})))
  (-unlink [_ _] true)
  (-add-monitor [_ ref notify] (monitor-named nm node ref notify))
  (-drop-monitor [_ ref] (demonitor-named node ref)))

;; --- frames: what crosses, and how --------------------------------------

(defn- encode
  "v with each process in it as [:Pid node id], and each throwable as
  data, so it prints as EDN a peer can read."
  [v]
  (walk/prewalk
    (fn [x]
      (cond
        (act/pid? x) [:Pid (proc/-node x) (proc/-pid x)]
        (instance? Throwable x) {::exception (or (ex-message x) (str x)) ::data (ex-data x)}
        :else x))
    v))

(defn- decode
  "A frame's value as node self holds it: each pid of self its process
  again, each other pid a RemotePid.  A pid of a process of self that has
  gone stays a handle all the same, as a dead pid does in Erlang."
  [self v]
  (walk/postwalk
    (fn [x]
      (cond
        (and (vector? x) (= 2 (count x)) (= :Local (first x)))
        (or (act/local-actor (second x)) (->RemotePid self (second x)))
        (and (vector? x) (= 3 (count x)) (= :Pid (first x)) (keyword? (second x)))
        (->RemotePid (second x) (nth x 2))
        :else x))
    (dist/arrive self v)))

(declare nodedown! handle-frame)

(defn connected
  "The peers node n is connected to."
  ([] (connected (act/node)))
  ([n] (vec (keys (:conns (state n))))))

(defn- conn-to
  "The connection from n to peer, opened on first use as Erlang connects
  nodes on first contact; nil when peer cannot be reached."
  [n peer]
  (or (get-in @nodes [n :conns peer])
      (when-let [t (:transport (state n))]
        (when-let [c (-connect t n peer
                               (fn [s] (handle-frame n peer (edn/read-string s)))
                               (fn [] (nodedown! n peer)))]
          (swap! nodes assoc-in [n :conns peer] c)
          c))))

(defn- send-frame!
  "Send frame from node n to peer; false when peer cannot be reached."
  [n peer frame]
  (if-let [c (conn-to n peer)]
    (boolean (-send-frame c (pr-str (encode frame))))
    false))

;; --- a process of another node ------------------------------------------

(defn- here
  "The local process a route to this node names, or nil when it is gone."
  [id]
  (act/local-actor id))

(defn deliver-remote
  "Send msg to the process p names: through dist/route, locally if p is a
  pid of this node, over the connection otherwise."
  [p msg]
  (let [r (dist/route (act/node) [:Pid (:node p) (:id p)])]
    (case (first r)
      :Here (let [[_ id] r] (when-let [a (here id)] (proc/-deliver a msg)))
      :There (let [[_ n id] r] (send-frame! (act/node) n [:send id msg]))
      nil)))

(defn- signal-remote [p from kind reason checked]
  (let [r (dist/route (act/node) [:Pid (:node p) (:id p)])]
    (case (first r)
      :Here (let [[_ id] r] (when-let [a (here id)] (proc/-signal a from kind reason checked)))
      :There (let [[_ n id] r] (send-frame! (act/node) n [:signal id from kind reason checked]))
      nil)))

(defn- link-remote
  "A link to a remote process: asked of its node, which answers noproc if
  the process is gone.  An unreachable node is noconnection at once."
  [p from]
  (let [r (dist/route (act/node) [:Pid (:node p) (:id p)])]
    (case (first r)
      :Here (let [[_ id] r] (if-let [a (here id)] (proc/-link a from) false))
      :There (let [[_ n id] r]
               (or (send-frame! (act/node) n [:link id from])
                   (do (proc/-signal from p :link :noconnection false) true)))
      true)))

(defn- unlink-remote [p from]
  (let [r (dist/route (act/node) [:Pid (:node p) (:id p)])]
    (case (first r)
      :Here (let [[_ id] r] (when-let [a (here id)] (proc/-unlink a from)))
      :There (let [[_ n id] r] (send-frame! (act/node) n [:unlink id from]))
      nil)))

(defn- monitor-remote
  "A monitor of a remote process: the notify stays on this node, under
  ref, and the peer is asked to fire it; an unreachable node fires it at
  once with noconnection."
  [p ref notify]
  (let [n (act/node)
        watcher (act/self)
        r (dist/route n [:Pid (:node p) (:id p)])]
    (case (first r)
      :Here (let [[_ id] r]
              (if-let [a (here id)] (proc/-add-monitor a ref notify) (notify :noproc)))
      :There (let [[_ peer id] r]
               (swap! nodes assoc-in [n :watching ref]
                      {:notify notify :peer peer :target id :watcher (some-> watcher proc/-pid)})
               (when-not (send-frame! n peer [:monitor id ref])
                 (swap! nodes update-in [n :watching] dissoc ref)
                 (notify :noconnection)))
      nil)
    ref))

(defn- demonitor-remote [p ref]
  (let [n (act/node)
        r (dist/route n [:Pid (:node p) (:id p)])]
    (case (first r)
      :Here (let [[_ id] r] (when-let [a (here id)] (proc/-drop-monitor a ref)))
      :There (let [[_ peer id] r]
               (swap! nodes update-in [n :watching] dissoc ref)
               (send-frame! n peer [:demonitor id ref]))
      nil)))

(defn- monitor-named
  "A monitor of name nm on node peer, as monitor(process, {Name, Node}):
  the peer resolves the name when the request arrives, and a name nobody
  holds fires at once with noproc."
  [nm peer ref notify]
  (let [n (act/node)]
    (swap! nodes assoc-in [n :watching ref]
           {:notify notify :peer peer :target 0 :watcher (some-> (act/self) proc/-pid)})
    (when-not (send-frame! n peer [:monitor-named nm ref])
      (swap! nodes update-in [n :watching] dissoc ref)
      (notify :noconnection))
    ref))

(defn- demonitor-named [peer ref]
  (let [n (act/node)]
    (swap! nodes update-in [n :watching] dissoc ref)
    (send-frame! n peer [:demonitor nil ref])))

(defn- fire!
  "Run the notify of monitor ref of node n, once."
  [n ref reason]
  (let [[before _] (swap-vals! nodes update-in [n :watching] dissoc ref)]
    (when-let [{:keys [notify]} (get-in before [n :watching ref])]
      (notify reason))))

;; --- frames from a peer -------------------------------------------------

(defn- spawn-allowed?
  "May node n start the fn named sym for a peer?  Only what its :spawn
  option allows: a set of namespace symbols, or a predicate on sym.  With
  no :spawn nothing is: a peer is not authenticated yet, and a spawn runs
  any fn it names."
  [n sym]
  (let [allow (:spawn (state n))]
    (boolean
     (cond
       (set? allow) (contains? allow (symbol (namespace sym)))
       (ifn? allow) (allow sym)
       :else false))))

(defn handle-frame
  "Perform, as node n, the operation frame from peer asks for."
  [n peer frame]
  (binding [act/*node* n]
    (let [[op & args] (decode n frame)]
      (case op
        :send (let [[id msg] args] (when-let [a (here id)] (proc/-deliver a msg)))
        :send-named (let [[nm msg] args] (when-let [a (act/whereis nm)] (proc/-deliver a msg)))
        :signal (let [[id from kind reason checked] args]
                  (when-let [a (here id)] (proc/-signal a from kind reason checked)))
        :link (let [[id from] args]
                (when-not (if-let [a (here id)] (proc/-link a from) false)
                  (send-frame! n peer [:signal (proc/-pid from) (->RemotePid n id) :link :noproc false])))
        :unlink (let [[id from] args] (when-let [a (here id)] (proc/-unlink a from)))
        (:monitor :monitor-named)
        (let [[target ref] args
              a (if (= op :monitor) (here target) (act/whereis target))
              id (some-> a proc/-pid)]
          (if a
            (do (swap! nodes assoc-in [n :exported ref] [peer id])
                (proc/-add-monitor a ref (fn [reason]
                                           (swap! nodes update-in [n :exported] dissoc ref)
                                           (send-frame! n peer [:fired ref reason]))))
            (send-frame! n peer [:fired ref :noproc])))
        :demonitor (let [[id ref] args
                         id (or id (second (get-in @nodes [n :exported ref])))]
                     (swap! nodes update-in [n :exported] dissoc ref)
                     (when-let [a (and id (here id))] (proc/-drop-monitor a ref)))
        :fired (let [[ref reason] args] (fire! n ref reason))
        :alias (let [[alias msg] args] (act/send-alias! alias msg))
        :spawn (let [[ref sym fargs link from] args]
                 (if-not (spawn-allowed? n sym)
                   (send-frame! n peer [:spawned ref [:refused sym]])
                   (let [f (requiring-resolve sym)
                         ;; the link is made by the new process before it runs a
                         ;; step, so a crash at once still reaches the spawner
                         go (promise)
                         a (act/spawn (fn [] @go (when link (act/link! from)) (apply f fargs)))]
                     (send-frame! n peer [:spawned ref a])
                     (deliver go true))))
        :spawned (let [[ref pid] args]
                   (when-let [p (get-in @nodes [n :pending ref])] (deliver p pid)))
        nil))))

;; --- a lost connection --------------------------------------------------

(defn nodedown!
  "Node n lost its connection to peer: through dist/on-nodedown, break
  every link across it and fire every monitor across it with
  :noconnection, and tell every monitor_node of peer."
  [n peer]
  (let [[before _] (swap-vals! nodes update n
                               (fn [s] (when s (-> s (update :conns dissoc peer)
                                                   (update :exported (fn [e] (into {} (remove (fn [[_ [p]]] (= p peer))) e)))))))
        s (get before n)
        links (vec (for [[x h] (act/remote-links n)] [:Link (proc/-pid x) (proc/-node h) (proc/-pid h)]))
        mons (vec (for [[ref {:keys [peer target watcher]}] (:watching s)] [:Mon (or watcher 0) ref peer target]))
        nmons (vec (for [[id p] (:node-mons s)] [:NodeMon id p]))]
    (binding [act/*node* n]
      (doseq [e (dist/on-nodedown peer links mons nmons)]
        (case (first e)
          :Signal (let [[_ me [_ pn pid] reason] e]
                    (when-let [a (act/local-actor me)]
                      (proc/-signal a (->RemotePid pn pid) :link reason true)))
          :Deliver (let [[_ me msg] e]
                     (if (= :DOWN (first msg))
                       (fire! n (second msg) :noconnection)
                       (when-let [a (act/local-actor me)] (proc/-deliver a msg))))
          nil)))))

;; --- the API ------------------------------------------------------------

(defn start!
  "Start node name in this VM, reachable over transport.  Code that runs
  outside any actor now runs as this node; with-node runs code as another.
  Options:

      :spawn   what a peer may start here with spawn-on: a set of namespace
               symbols whose fns it may name, or a predicate on the fn's
               symbol.  Without it a peer may start nothing.

  Returns name."
  ([nm transport] (start! nm transport {}))
  ([nm transport opts]
  (when-not (and (keyword? nm) (= nm (edn/read-string (pr-str nm))))
    (throw (ex-info (str "a node name is a keyword that reads back as itself: " (pr-str nm))
                    {:reason :badarg :node nm})))
  (swap! nodes assoc nm {:transport transport :conns {} :watching {} :exported {}
                         :node-mons [] :pending {} :spawn (:spawn opts)})
  (act/set-remote-resolver! (fn [[_ remote-nm peer]] (->RemoteName peer remote-nm)))
  (act/set-peers-fn! (fn [] (connected (act/node))))
  (act/set-remote-alias-sender! (fn [alias msg]
                                  (send-frame! (act/node) (:ensemble.actor/owner-node alias) [:alias alias msg])))
  nm))

(defmacro with-node
  "Run body as node n: what it spawns runs there, and names resolve there."
  [n & body]
  `(binding [act/*node* ~n] ~@body))

(defn connect!
  "Connect this node to peer.  True if it could; a connection is also made
  on first contact."
  [peer]
  (boolean (conn-to (act/node) peer)))

(defn disconnect!
  "Drop the connection to peer, as erlang:disconnect_node: both ends see
  the node go down."
  [peer]
  (when-let [c (get-in @nodes [(act/node) :conns peer])]
    (-close c))
  true)

(defn monitor-node!
  "Receive [:nodedown peer] in the current actor when the connection to
  peer is lost; at once if there is none, as monitor_node(Node, true)."
  [peer]
  (let [n (act/node)
        me (proc/-pid (act/self))]
    (if (conn-to n peer)
      (swap! nodes update-in [n :node-mons] conj [me peer])
      (act/! (act/self) [:nodedown peer]))
    true))

(defn spawn-on
  "Start a process on node peer running (apply f args), f named by a
  symbol peer can resolve and allows (see start!'s :spawn), as
  spawn(Node, M, F, A).  With {:link true} it is linked to the current
  actor.  Returns its pid, a RemotePid; throws {:reason [:not-allowed
  sym]} when peer refuses."
  ([peer f-sym args] (spawn-on peer f-sym args {}))
  ([peer f-sym args {:keys [link timeout] :or {timeout 5000}}]
   (let [n (act/node)
         ref (act/make-ref)
         p (promise)]
     (swap! nodes assoc-in [n :pending ref] p)
     (when-not (send-frame! n peer [:spawn ref f-sym (vec args) (boolean link) (act/self)])
       (throw (ex-info "cannot reach node" {:reason :noconnection :node peer})))
     (let [t (act/timeout-ms timeout)
           r (if t (deref p t ::timeout) @p)]
       (swap! nodes update-in [n :pending] dissoc ref)
       (cond
         (= ::timeout r) (throw (ex-info "spawn-on timed out" {:reason :timeout :node peer}))
         (and (vector? r) (= :refused (first r)))
         (throw (ex-info "the node does not allow that spawn"
                         {:reason [:not-allowed (second r)] :node peer}))
         :else r)))))

;; --- the loopback transport ---------------------------------------------

(defrecord LoopbackConn [ch closed on-close peer-conn]
  Conn
  (-send-frame [_ s] (and (not @closed) (a/put! ch s)))
  (-close [this]
    (when (compare-and-set! closed false true)
      (a/close! ch)
      (on-close)
      (when-let [pc @peer-conn] (-close pc)))))

(defrecord Loopback []
  Transport
  (-connect [_ from to on-frame on-close]
    (when (and (started? to) (instance? Loopback (:transport (state to))))
      ;; from's end, whose frames the peer reads, and the peer's end, whose
      ;; frames from reads; each end's frames are read on a fiber of their
      ;; own, in order
      (let [to-ch (a/chan 1024)
            from-ch (a/chan 1024)
            from-end (->LoopbackConn to-ch (atom false) on-close (atom nil))
            to-end (->LoopbackConn from-ch (atom false) (fn [] (nodedown! to from)) (atom nil))]
        (reset! (:peer-conn from-end) to-end)
        (reset! (:peer-conn to-end) from-end)
        (swap! nodes assoc-in [to :conns from] to-end)
        (a/go-loop [] (when-let [s (a/<! to-ch)] (handle-frame to from (edn/read-string s)) (recur)))
        (a/go-loop [] (when-let [s (a/<! from-ch)] (on-frame s) (recur)))
        from-end))))

(defn loopback
  "The transport between nodes in this VM."
  []
  (->Loopback))

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
  arrival, what a lost connection does, and every step of the handshake
  are decided by ensemble.dist.

  The layers, bottom up:
  - a Transport (ensemble.transport) carries a byte stream between two
    nodes.  That is all a transport library implements; the loopback
    transport connects nodes in one VM;
  - each frame on the stream is its length, four bytes, then its payload,
    which a Codec (ensemble.codec, EDN by default) turns into a value;
  - a connection is used only once the handshake has named the peer and
    each end has proved it holds the cookie, by signing the other's
    challenge with HMAC-SHA256;
  - then frames carry the operations of the distribution protocol, each
    an envelope [op hdr body]: hdr the operation's own fields (the pids
    it names, a ref, a name), body the data it carries -- a message, an
    exit reason.  A pid in hdr is known by its field; in body each pid
    and throwable is tagged, and data that looks like a tag is escaped,
    so whatever a process sends arrives as it was sent.

  Each start of a node draws a creation number, and a pid carries its
  node's: a pid of an earlier run of a node names no process of a later
  one, though the id may be in use again.

  As in Erlang:
  - a pid in a message crosses as data and arrives as a pid again;
  - a link to a process that is gone, or on a node that cannot be reached,
    gives noproc or noconnection;
  - when a connection is lost, every link across it breaks with
    noconnection, every monitor across it fires with noconnection, and
    every monitor_node of the peer gets [:nodedown peer];
  - (spawn-on node f-sym args) starts a process there, from a fn named by
    symbol, as spawn(Node, M, F, A) does -- only from a peer that passed
    the handshake, and only a fn the node allows."
  (:require [clojure.core.async :as a]
            [clojure.edn :as edn]
            [ensemble.actor :as act]
            [ensemble.codec :as codec]
            [ensemble.digest :as digest]
            [ensemble.dist :as dist]
            [ensemble.logger :as logger]
            [ensemble.process :as proc]
            [ensemble.transport :as tr]))

;; --- authentication -----------------------------------------------------

(defprotocol Auth
  (-sign [a challenge]
    "Sign challenge, a string, as only a node that may connect could.
    Both ends of a connection must sign alike."))

(defrecord CookieAuth [key]
  Auth
  (-sign [_ challenge] (digest/hex (digest/hmac-sha-256 key (vec (.getBytes ^String challenge "UTF-8"))))))

(defn cookie-auth
  "Authentication by a shared secret, Erlang's cookie: a challenge is
  signed with HMAC-SHA256 under it."
  [cookie]
  (->CookieAuth (vec (.getBytes ^String (str cookie) "UTF-8"))))

(defonce ^:private rng (java.security.SecureRandom.))

(defn- random-hex [n]
  (let [bs (byte-array n)] (.nextBytes rng bs) (digest/hex bs)))

(defonce ^{:doc "The cookie of a node started without one: drawn once per VM, so
  nodes in one VM connect, as nodes on one host share ~/.erlang.cookie.
  Nodes in different VMs must be given the same :cookie."}
  vm-cookie
  (random-hex 32))

(defn- fresh-creation
  "A creation number for a node's start: random, never 0."
  []
  (inc (rand-int 2147483646)))

;; --- node state ---------------------------------------------------------

(defonce ^:private nodes
  ;; name -> {:transport t :listener l :codec c :auth a :creation n
  ;;          :conns {peer {:conn c :creation n :initiator node}}
  ;;          :connecting {peer promise}
  ;;          :watching {ref {...}} :exported {ref [peer pid]}
  ;;          :node-mons [[id peer]] :spawns {ref [peer promise]}
  ;;          :spawn allowed :max-frame n :handshake-timeout ms}
  (atom {}))

(defn- state [n] (get @nodes n))

(defn started? "Is n a node running in this VM?" [n] (contains? @nodes n))

(defn creation
  "The creation number of node n's current run (0 for a node not started)."
  ([] (creation (act/node)))
  ([n] (:creation (state n) 0)))

(declare deliver-remote signal-remote link-remote unlink-remote monitor-remote demonitor-remote
         monitor-named demonitor-named send-frame!)

(defrecord RemotePid [node id creation]
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
  (-deliver [_ msg] (send-frame! (act/node) node :send-named {:name nm} msg))
  (-signal [_ _ _ _ _] (throw (ex-info "an exit signal goes to a pid, not a name" {:reason :badarg})))
  (-link [_ _] (throw (ex-info "a link goes to a pid, not a name" {:reason :badarg})))
  (-unlink [_ _] true)
  (-add-monitor [_ ref notify] (monitor-named nm node ref notify))
  (-drop-monitor [_ ref] (demonitor-named node ref)))

(defn- pid-creation
  "The creation a process handle carries: a local actor's is its node's."
  [p]
  (if (instance? RemotePid p) (:creation p) (creation (proc/-node p))))

(defn- wire
  "A process handle as [node id creation]."
  [p]
  [(proc/-node p) (proc/-pid p) (pid-creation p)])

(defn- local
  "Node n's live actor id, or nil."
  [n id]
  (when-let [a (act/local-actor id)]
    (when (= n (proc/-node a)) a)))

(defn- pid-of
  "What the pid [node id creation] from the wire is at node self, through
  dist/arrive: a process of self's current run is its actor again, any
  other a RemotePid.  A process of self that has gone stays a handle all
  the same, as a dead pid does in Erlang."
  [self [pn id cr]]
  (let [r (dist/arrive self (creation self) [:Pid pn id cr])]
    (if (= :Local (first r))
      (or (local self id) (->RemotePid self id cr))
      (->RemotePid pn id cr))))

;; --- frames: what crosses, and how --------------------------------------

(def ^:private tags
  ;; what marks a value in a frame's body: a pid, a throwable, and data
  ;; that would read as either, escaped
  #{::pid ::ex ::q})

(defn- wire-leaf? [x]
  (or (nil? x) (boolean? x) (number? x) (string? x) (keyword? x) (symbol? x)
      (char? x) (uuid? x) (instance? java.util.Date x)))

(defn- pack
  "v as a frame's body carries it: plain data, each pid in it [::pid node
  id creation], each throwable [::ex message data], and each vector that
  begins with one of those tags escaped as [::q ...].  Anything else -- a
  record, an atom, a fn -- has no form on the wire: strict, it throws;
  otherwise it goes as [:unsendable its-printed-form], which an exit
  reason, that must go whatever it holds, may."
  [strict? v]
  (let [pk #(pack strict? %)]
    (cond
      (instance? RemoteName v) [:At (:nm v) (:node v)]
      (act/pid? v) (into [::pid] (wire v))
      (instance? Throwable v) [::ex (or (ex-message v) (str v)) (pk (ex-data v))]
      (record? v) (if strict?
                    (throw (ex-info (str "cannot send a " (.getName (class v)) " to another node")
                                    {:reason [:badarg :not-wire-data (.getName (class v))]}))
                    [:unsendable (pr-str v)])
      (map? v) (into {} (map (fn [[k x]] [(pk k) (pk x)])) v)
      (vector? v) (let [xs (mapv pk v)] (if (contains? tags (first v)) (into [::q] xs) xs))
      (set? v) (into #{} (map pk) v)
      (seq? v) (apply list (map pk v))
      (wire-leaf? v) v
      strict? (throw (ex-info (str "cannot send a " (.getName (class v)) " to another node")
                              {:reason [:badarg :not-wire-data (.getName (class v))]}))
      :else [:unsendable (pr-str v)])))

(defn- unpack
  "A frame's body as node self holds it: pack undone."
  [self v]
  (let [up #(unpack self %)]
    (cond
      (vector? v) (case (first v)
                    ::pid (pid-of self (subvec v 1 4))
                    ::ex (ex-info (str (nth v 1)) (or (up (nth v 2)) {}))
                    ::q (mapv up (rest v))
                    (mapv up v))
      (map? v) (into {} (map (fn [[k x]] [(up k) (up x)])) v)
      (set? v) (into #{} (map up) v)
      (seq? v) (apply list (map up v))
      :else v)))

(def ^:private pid-fields
  ;; the fields of a frame's hdr that hold a pid
  #{:to :from})

(def ^:private lossy-ops
  ;; the operations whose body is an exit reason, sent whatever it holds
  #{:signal :fired :spawned})

(defn- frame
  "The envelope [op hdr body] of operation op: hdr's pids as [node id
  creation], body packed."
  [op hdr body]
  [op
   (into {} (map (fn [[k v]] [k (if (and (contains? pid-fields k) (some? v)) (wire v) v)])) hdr)
   (pack (not (contains? lossy-ops op)) body)])

(defn- open-frame
  "A frame from the wire as node self holds it: [op hdr body]; throws if
  it is not an envelope."
  [self v]
  (when-not (and (vector? v) (= 3 (count v)) (keyword? (first v)) (map? (second v)))
    (throw (ex-info "not a frame" {:reason :bad-frame})))
  (let [[op hdr body] v]
    [op
     (into {} (map (fn [[k x]]
                     [k (if (and (contains? pid-fields k) (some? x))
                          (if (and (vector? x) (= 3 (count x)) (keyword? (first x)))
                            (pid-of self x)
                            (throw (ex-info "not a pid" {:reason :bad-frame :field k})))
                          x)]))
           hdr)
     (unpack self body)]))

(defn- framed
  "The bytes of one frame: payload's length, four bytes, then payload."
  ^bytes [^bytes payload]
  (let [n (alength payload)
        out (byte-array (+ 4 n))]
    (doseq [[i b] (map-indexed vector (dist/length-header n))]
      (aset-byte out i (unchecked-byte b)))
    (System/arraycopy payload 0 out 4 n)
    out))

(defn- write-frame!
  "Write v, encoded by node n's codec, as one frame on conn c."
  [n c v]
  (boolean (tr/-write c (framed (codec/-encode (:codec (state n)) v)))))

(def ^:private handshake-max-frame
  ;; the largest frame a connection takes before its handshake is done
  4096)

(def default-max-frame
  "The largest frame a node takes, in bytes, unless started with
  :max-frame."
  (* 64 1024 1024))

(defn- reader
  "A fn taking the stream's chunks, as tr/-start hands them, that calls
  (on-frame payload) with each whole frame's payload, in order, while it
  answers true.  A frame longer than (max-len) is not read: (on-bad
  reason) is called instead.  Once either has happened, the rest of the
  stream is ignored."
  [on-frame max-len on-bad]
  (let [buf (atom (byte-array 0))
        dead (atom false)]
    (fn [^bytes chunk]
      (when-not @dead
        (let [^bytes old @buf
              joined (byte-array (+ (alength old) (alength chunk)))]
          (System/arraycopy old 0 joined 0 (alength old))
          (System/arraycopy chunk 0 joined (alength old) (alength chunk))
          (loop [^bytes b joined]
            (let [len (alength b)
                  n (when (>= len 4) (dist/header-length (mapv #(bit-and (aget b %) 0xff) (range 4))))]
              (cond
                (and n (> n (max-len)))
                (do (reset! dead true) (reset! buf (byte-array 0)) (on-bad [:frame-too-large n]))
                (and n (>= len (+ 4 n)))
                (let [payload (byte-array n)
                      rest-b (byte-array (- len 4 n))]
                  (System/arraycopy b 4 payload 0 n)
                  (System/arraycopy b (+ 4 n) rest-b 0 (- len 4 n))
                  (if (on-frame payload)
                    (recur rest-b)
                    (do (reset! dead true) (reset! buf (byte-array 0)))))
                :else (reset! buf b)))))))))

(declare nodedown! handle-frame)

(defn- tell-watchers!
  "Send msg to every actor of node n that monitors all nodes."
  [n msg]
  (binding [act/*node* n]
    (doseq [id (:node-watchers (state n))]
      (if-let [w (local n id)]
        (proc/-deliver w msg)
        (swap! nodes update-in [n :node-watchers] disj id)))))

(defn connected
  "The peers node n is connected to."
  ([] (connected (act/node)))
  ([n] (vec (keys (:conns (state n))))))

;; --- connections and the handshake ----------------------------------------

(defn- ctx-of
  "What dist/handshake needs to know of node n."
  [n]
  (let [s (state n)]
    {:up-creation (fn [p] (get-in s [:conns p :creation]))
     :pending? (fn [p] (contains? (:connecting s) p))
     :fresh (random-hex 16)
     :sign (fn [c] (-sign (:auth s) (str c)))}))

(defn handshake-step!
  "Take a handshake frame on conn c of node n, through dist/handshake: send
  what it says, and once the peer is proved make c the connection to it.
  sess holds the handshake's state; done, if any, is told how it ended."
  [n c sess frame done]
  (let [[tag hs frames] (dist/handshake (:hs @sess) frame (ctx-of n))]
    (doseq [f frames] (write-frame! n c f))
    (case tag
      :Next (swap! sess assoc :hs hs)
      :Up (let [peer (:peer hs)
                new {:conn c :creation (:peer-creation hs)
                     :initiator (if (= :initiator (:role hs)) n peer)}
                existing (get-in @nodes [n :conns peer])
                decision (dist/on-up n (some-> existing (select-keys [:creation :initiator]))
                                     (select-keys new [:creation :initiator]))]
            (if (= :drop decision)
              ;; both ends keep the other connection
              (do (swap! sess assoc :hs hs :failed :duplicate)
                  (tr/-close c)
                  (when done (deliver done [:up (:conn existing)])))
              (do
                ;; an earlier run of the peer goes down before this one is up
                (when (= :replace-down decision) (nodedown! n peer))
                (swap! nodes assoc-in [n :conns peer] new)
                (swap! sess assoc :hs hs :peer peer)
                ;; closed once it no longer stands for the peer, so its close
                ;; is not a nodedown
                (when existing (tr/-close (:conn existing)))
                (when done (deliver done [:up c]))
                (when-not (= :replace decision) (tell-watchers! n [:nodeup peer])))))
      :Fail (do (swap! sess assoc :failed hs)
                (tr/-close c)
                (when done (deliver done [:failed hs]))))))

(declare handshake-timeout)

(defn- session!
  "Run conn c of node n: the handshake from state hs, then the
  distribution protocol.  done, if any, is told how the handshake ended;
  without done c was accepted, and is closed if its handshake takes too
  long.  A frame that does not decode drops the connection; one whose
  operation fails is reported, and the connection carries on."
  [n c hs first-frames done]
  (let [sess (atom {:hs hs})
        cdc (:codec (state n))
        drop! (fn [why]
                (logger/report! {:level :error :kind :dist-connection-dropped :node n
                                 :peer (:peer @sess) :reason why})
                (tr/-close c)
                false)
        on-frame (fn [payload]
                   (if-let [peer (:peer @sess)]
                     (if-let [[op hdr body] (try (open-frame n (codec/-decode cdc payload))
                                                 (catch Throwable e (drop! [:bad-frame (ex-message e)]) nil))]
                       (do (try (handle-frame n peer op hdr body)
                                (catch Throwable e
                                  (logger/report! {:level :error :kind :dist-frame-failed :node n
                                                   :peer peer :op op :reason e})))
                           true)
                       false)
                     (try (handshake-step! n c sess (codec/-decode cdc payload) done)
                          (not (:failed @sess))
                          (catch Throwable e (drop! [:bad-handshake (ex-message e)])))))
        max-len (fn [] (if (:peer @sess)
                         (:max-frame (state n) default-max-frame)
                         handshake-max-frame))]
    (tr/-start c (reader on-frame max-len drop!)
               (fn []
                 (if-let [peer (:peer @sess)]
                   ;; only the connection that is up stands for the peer
                   (when (= c (get-in @nodes [n :conns peer :conn])) (nodedown! n peer))
                   (when done (deliver done [:failed :closed])))))
    (when-not done
      (let [ms (handshake-timeout n)]
        (a/go (a/<! (a/timeout ms))
              (when-not (or (:peer @sess) (:failed @sess))
                (swap! sess assoc :failed :timeout)
                (tr/-close c)))))
    (doseq [f first-frames] (write-frame! n c f))))

(def ^:dynamic *handshake-timeout*
  "How long a connection's handshake may take, in ms, unless the node was
  started with :handshake-timeout."
  5000)

(defn- handshake-timeout [n]
  (or (:handshake-timeout (state n)) *handshake-timeout*))

(defn- wait-for-conn
  "The connection from n to peer once one is up, waiting up to ms: the
  other end's, when two nodes connected to each other at once and this
  end's attempt was the one dropped."
  [n peer ms]
  (let [deadline (+ (act/now-ms) ms)]
    (loop []
      (or (get-in @nodes [n :conns peer :conn])
          (when (< (act/now-ms) deadline)
            (a/<!! (a/timeout 2))
            (recur))))))

(defn- open!
  "Connect n to peer and shake hands; the connection, or nil."
  [n peer]
  (let [done (promise)
        [_ claimed] (swap-vals! nodes update-in [n :connecting] #(if (contains? % peer) % (assoc % peer done)))
        mine? (= done (get-in claimed [n :connecting peer]))]
    (if-not mine?
      ;; another attempt is under way: wait for it
      (let [other (get-in claimed [n :connecting peer])]
        (deref other (handshake-timeout n) nil)
        (get-in @nodes [n :conns peer :conn]))
      (try
        (if-let [c (some-> (state n) :transport (tr/-connect n peer))]
          (let [[_ hs frames] (dist/open-handshake n (creation n) peer)]
            (session! n c hs frames done)
            (let [[k v] (deref done (handshake-timeout n) [:failed :timeout])]
              (cond
                (= :up k) v
                ;; refused because the peer connected to us meanwhile
                (and (vector? v) (= :refused (first v)) (contains? #{:nok :alive} (second v)))
                (wait-for-conn n peer (handshake-timeout n))
                :else (do (tr/-close c) nil))))
          (do (deliver done [:failed :unreachable]) nil))
        (finally
          (swap! nodes update-in [n :connecting] dissoc peer))))))

(def ^:dynamic *connect*
  "Whether a send to a node with no connection opens one.  False within
  without-connecting."
  true)

(defmacro without-connecting
  "Run body with sends to a node that is not connected dropped instead of
  connecting to it, as erlang:send/3's noconnect: for a service's own
  traffic, which must not bring back a node that just went down."
  [& body]
  `(binding [*connect* false] ~@body))

(defn- conn-to
  "The connection from n to peer, opened on first use as Erlang connects
  nodes on first contact, unless *connect* is false; nil when peer cannot
  be reached or refuses."
  [n peer]
  (or (get-in @nodes [n :conns peer :conn])
      (when (and *connect* (state n) (not= n peer)) (open! n peer))))

(defn- accept!
  "Take a connection opened to node n: it shakes hands before anything
  else is read from it."
  [n c]
  (session! n c (dist/accept-handshake n (creation n)) [] nil))

(defn- send-frame!
  "Send operation op, with fields hdr and data body, from node n to peer;
  false when peer cannot be reached.  Throws, before anything is sent, if
  body holds what the wire cannot carry."
  ([n peer op hdr] (send-frame! n peer op hdr nil))
  ([n peer op hdr body]
   (let [f (frame op hdr body)]
     (if-let [c (conn-to n peer)]
       (write-frame! n c f)
       false))))

;; --- a process of another node ------------------------------------------

(defn- here
  "The local process a frame names, or nil when it is gone: a pid of this
  node's current run arrives as its actor."
  [x]
  (when (act/actor? x) x))

(defn- route-of [p]
  (dist/route (act/node) (creation) (into [:Pid] (wire p))))

(defn deliver-remote
  "Send msg to the process p names: through dist/route, locally if p is a
  pid of this node, over the connection otherwise."
  [p msg]
  (let [r (route-of p)]
    (case (first r)
      :Here (when-let [a (local (act/node) (second r))] (proc/-deliver a msg))
      :There (send-frame! (act/node) (second r) :send {:to p} msg)
      nil)))

(defn- signal-remote [p from kind reason checked]
  (let [r (route-of p)]
    (case (first r)
      :Here (when-let [a (local (act/node) (second r))] (proc/-signal a from kind reason checked))
      :There (send-frame! (act/node) (second r) :signal {:to p :from from :kind kind :checked checked} reason)
      nil)))

(defn- link-remote
  "A link to a remote process: asked of its node, which answers noproc if
  the process is gone.  An unreachable node is noconnection at once."
  [p from]
  (let [r (route-of p)]
    (case (first r)
      :Here (if-let [a (local (act/node) (second r))] (proc/-link a from) false)
      :Gone false
      :There (or (send-frame! (act/node) (second r) :link {:to p :from from})
                 (do (proc/-signal from p :link :noconnection false) true))
      true)))

(defn- unlink-remote [p from]
  (let [r (route-of p)]
    (case (first r)
      :Here (when-let [a (local (act/node) (second r))] (proc/-unlink a from))
      :There (send-frame! (act/node) (second r) :unlink {:to p :from from})
      nil)))

(defn- monitor-remote
  "A monitor of a remote process: the notify stays on this node, under
  ref, and the peer is asked to fire it; an unreachable node fires it at
  once with noconnection."
  [p ref notify]
  (let [n (act/node)
        watcher (act/self)
        r (route-of p)]
    (case (first r)
      :Here (if-let [a (local n (second r))] (proc/-add-monitor a ref notify) (notify :noproc))
      :Gone (notify :noproc)
      :There (let [[_ peer id cr] r]
               (swap! nodes assoc-in [n :watching ref]
                      {:notify notify :peer peer :target id :creation cr
                       :watcher (some-> watcher proc/-pid)})
               (when-not (send-frame! n peer :monitor {:to p :ref ref})
                 (swap! nodes update-in [n :watching] dissoc ref)
                 (notify :noconnection)))
      nil)
    ref))

(defn- demonitor-remote [p ref]
  (let [n (act/node)
        r (route-of p)]
    (case (first r)
      :Here (when-let [a (local n (second r))] (proc/-drop-monitor a ref))
      :There (do (swap! nodes update-in [n :watching] dissoc ref)
                 (send-frame! n (second r) :demonitor {:to p :ref ref}))
      nil)))

(defn- monitor-named
  "A monitor of name nm on node peer, as monitor(process, {Name, Node}):
  the peer resolves the name when the request arrives, and a name nobody
  holds fires at once with noproc."
  [nm peer ref notify]
  (let [n (act/node)]
    (swap! nodes assoc-in [n :watching ref]
           {:notify notify :peer peer :target 0 :creation 0 :watcher (some-> (act/self) proc/-pid)})
    (when-not (send-frame! n peer :monitor-named {:name nm :ref ref})
      (swap! nodes update-in [n :watching] dissoc ref)
      (notify :noconnection))
    ref))

(defn- demonitor-named [peer ref]
  (let [n (act/node)]
    (swap! nodes update-in [n :watching] dissoc ref)
    (send-frame! n peer :demonitor {:ref ref})))

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
  no :spawn nothing is."
  [n sym]
  (let [allow (:spawn (state n))]
    (boolean
     (and (symbol? sym)
          (cond
            (set? allow) (and (namespace sym) (contains? allow (symbol (namespace sym))))
            (ifn? allow) (allow sym)
            :else false)))))

(defn outcome
  "What running (f) came to, as erpc reports it: [:ok value], [:exit
  reason] for an exit, or [:exception throwable]."
  [f]
  (try [:ok (f)]
       (catch Throwable e
         (let [r (act/reason-of e)]
           (if (identical? r e) [:exception e] [:exit r])))))

(defn handle-frame
  "Perform, as node n, operation op from peer, with fields hdr and data
  body.  Only a peer that passed the handshake gets here."
  [n peer op hdr body]
  (binding [act/*node* n]
    (case op
      :send (when-let [a (here (:to hdr))] (proc/-deliver a body))
      :send-named (when-let [a (act/whereis (:name hdr))] (proc/-deliver a body))
      :signal (let [{:keys [to from kind checked]} hdr]
                (when-let [a (here to)] (proc/-signal a from kind body checked)))
      :link (let [{:keys [to from]} hdr]
              (when-not (if-let [a (here to)] (proc/-link a from) false)
                (send-frame! n peer :signal {:to from :from to :kind :link :checked false} :noproc)))
      :unlink (let [{:keys [to from]} hdr] (when-let [a (here to)] (proc/-unlink a from)))
      (:monitor :monitor-named)
      (let [ref (:ref hdr)
            a (if (= op :monitor) (here (:to hdr)) (act/whereis (:name hdr)))]
        (if a
          (do (swap! nodes assoc-in [n :exported ref] [peer a])
              (proc/-add-monitor a ref (fn [reason]
                                         (swap! nodes update-in [n :exported] dissoc ref)
                                         (send-frame! n peer :fired {:ref ref} reason))))
          (send-frame! n peer :fired {:ref ref} :noproc)))
      :demonitor (let [ref (:ref hdr)
                       a (or (here (:to hdr)) (second (get-in @nodes [n :exported ref])))]
                   (swap! nodes update-in [n :exported] dissoc ref)
                   (when a (proc/-drop-monitor a ref)))
      :fired (fire! n (:ref hdr) body)
      :alias (act/send-alias! (:alias hdr) body)
      :spawn (let [{:keys [ref sym link from]} hdr
                   {:keys [args reply]} body
                   f (when (spawn-allowed? n sym) (try (requiring-resolve sym) (catch Throwable _ nil)))]
               (cond
                 (not (spawn-allowed? n sym)) (send-frame! n peer :spawned {:ref ref} [:refused sym])
                 (nil? f) (send-frame! n peer :spawned {:ref ref} [:undefined sym])
                 :else
                 ;; the link is made by the new process before it runs a
                 ;; step, so a crash at once still reaches the spawner
                 (let [go (promise)
                       a (act/spawn (fn [] @go (when link (act/link! from))
                                      (if reply
                                        (proc/-deliver from [::result reply (outcome #(apply f args))])
                                        (apply f args))))]
                   (send-frame! n peer :spawned {:ref ref} [:ok a])
                   (deliver go true))))
      :spawned (when-let [[_ p] (get-in @nodes [n :spawns (:ref hdr)])] (deliver p body))
      nil)))

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
        links (vec (for [[x h] (act/remote-links n)]
                     [:Link (proc/-pid x) (proc/-node h) (proc/-pid h) (pid-creation h)]))
        mons (vec (for [[ref {:keys [peer target creation watcher]}] (:watching s)]
                    [:Mon (or watcher 0) ref peer target (or creation 0)]))
        nmons (vec (for [[id p] (:node-mons s)] [:NodeMon id p]))]
    (when (contains? (:conns s) peer)
      (binding [act/*node* n]
        ;; the monitors peer held of processes here go with it
        (doseq [[ref [p a]] (:exported s) :when (= p peer)]
          (proc/-drop-monitor a ref))
        ;; and a spawn asked of it gets no answer now
        (doseq [[_ [p pr]] (:spawns s) :when (= p peer)]
          (deliver pr [:nodedown]))
        (doseq [e (dist/on-nodedown peer links mons nmons)]
          (case (first e)
            :Signal (let [[_ me [_ pn pid cr] reason] e]
                      (when-let [a (local n me)]
                        (proc/-signal a (->RemotePid pn pid cr) :link reason true)))
            :Deliver (let [[_ me msg] e]
                       (if (= :DOWN (first msg))
                         (fire! n (second msg) :noconnection)
                         (when-let [a (local n me)] (proc/-deliver a msg))))
            nil))
        (swap! nodes update-in [n :node-mons] (fn [ms] (vec (remove #(= peer (second %)) ms))))
        (tell-watchers! n [:nodedown peer])))))

;; --- the API ------------------------------------------------------------

(defonce ^:private start-hooks
  ;; key -> (fn [node]), run as each node starts: the services it runs
  (atom {}))

(defn on-start!
  "Run (f node) as every node starts from now on, under key k (a second
  call with k replaces the first).  How a service -- global, pg -- runs
  on each node."
  [k f]
  (swap! start-hooks assoc k f)
  nil)

(defn start!
  "Start node name in this VM, reachable over transport.  Code that runs
  outside any actor now runs as this node; with-node runs code as another.
  Options:

      :cookie  the secret a peer must share to connect (default: one drawn
               once per VM, so nodes of this VM connect to each other)
      :auth    instead of :cookie, any Auth: how a challenge is signed
      :codec   how frames become bytes (default ensemble.codec/edn)
      :spawn   what a peer may start here with spawn-on: a set of namespace
               symbols whose fns it may name, or a predicate on the fn's
               symbol.  Without it a peer may start nothing.
      :max-frame          the largest frame taken from a peer, in bytes
                          (default-max-frame); a larger one drops it
      :handshake-timeout  how long a connection may take to shake hands,
                          in ms (*handshake-timeout*)

  Each start draws a new creation number.  Returns name."
  ([nm transport] (start! nm transport {}))
  ([nm transport opts]
   (when-not (and (keyword? nm) (= nm (edn/read-string (pr-str nm))))
     (throw (ex-info (str "a node name is a keyword that reads back as itself: " (pr-str nm))
                     {:reason :badarg :node nm})))
   (swap! nodes assoc nm {:transport transport :conns {} :connecting {} :watching {} :exported {}
                          :node-mons [] :spawns {} :spawn (:spawn opts)
                          :max-frame (:max-frame opts default-max-frame)
                          :handshake-timeout (:handshake-timeout opts)
                          :codec (or (:codec opts) (codec/edn))
                          :auth (or (:auth opts) (cookie-auth (or (:cookie opts) vm-cookie)))
                          :creation (fresh-creation)})
   (act/set-remote-resolver! (fn [[_ remote-nm peer]] (->RemoteName peer remote-nm)))
   (act/set-peers-fn! (fn [] (connected (act/node))))
   (act/set-creation-fn! creation)
   (act/set-remote-alias-sender! (fn [alias msg]
                                   (send-frame! (act/node) (:ensemble.actor/owner-node alias) :alias {:alias alias} msg)))
   ;; the services start before a peer can connect
   (binding [act/*node* nm]
     (doseq [[_ f] @start-hooks] (f nm)))
   (swap! nodes assoc-in [nm :listener] (tr/-listen transport nm (fn [c] (accept! nm c))))
   nm))

(defn stop!
  "Stop node name: it stops listening and drops every connection, so its
  peers see it go down.  Its processes run on."
  [nm]
  (when-let [s (state nm)]
    (some-> (:listener s) tr/-stop)
    (doseq [[_ {:keys [conn]}] (:conns s)] (tr/-close conn))
    (swap! nodes dissoc nm))
  nil)

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
  (when-let [c (get-in @nodes [(act/node) :conns peer :conn])]
    (tr/-close c))
  true)

(defn monitor-nodes!
  "With on true, the current actor receives [:nodeup peer] and [:nodedown
  peer] whenever its node connects to or loses a peer, as
  net_kernel:monitor_nodes; with false, no longer."
  [on]
  (let [n (act/node)
        me (proc/-pid (act/self))]
    (swap! nodes update-in [n :node-watchers] (fnil (if on conj disj) #{}) me)
    true))

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
  actor; with {:reply r} it sends the current actor [::result r outcome]
  when (apply f args) is done (see outcome), as erpc's processes do.
  Waits up to :timeout ms (default 5000; nil or :infinity, for ever) for
  peer to answer.  Returns its pid, a RemotePid; throws {:reason
  [:not-allowed sym]} when peer refuses, [:undefined sym] when it has no
  such fn, :noconnection when it cannot be reached or goes down first."
  ([peer f-sym args] (spawn-on peer f-sym args {}))
  ([peer f-sym args {:keys [link timeout reply] :or {timeout 5000}}]
   (let [n (act/node)
         ref (act/make-ref)
         p (promise)]
     (swap! nodes assoc-in [n :spawns ref] [peer p])
     (when-not (try (send-frame! n peer :spawn {:ref ref :sym f-sym :link (boolean link) :from (act/self)}
                                 {:args (vec args) :reply reply})
                    (catch Throwable e (swap! nodes update-in [n :spawns] dissoc ref) (throw e)))
       (swap! nodes update-in [n :spawns] dissoc ref)
       (throw (ex-info "cannot reach node" {:reason :noconnection :node peer})))
     (let [t (act/timeout-ms timeout)
           [k v] (if t (deref p t [::timeout]) @p)]
       (swap! nodes update-in [n :spawns] dissoc ref)
       (case k
         :ok v
         ::timeout (throw (ex-info "spawn-on timed out" {:reason :timeout :node peer}))
         :refused (throw (ex-info "the node does not allow that spawn"
                                  {:reason [:not-allowed v] :node peer}))
         :undefined (throw (ex-info "the node has no such fn" {:reason [:undefined v] :node peer}))
         :nodedown (throw (ex-info "the node went down" {:reason :noconnection :node peer})))))))

(defn loopback
  "The transport between nodes in this VM (ensemble.transport/loopback)."
  ([] (tr/loopback))
  ([opts] (tr/loopback opts)))

(ns ensemble.transport
  "What distribution needs of a network: a byte stream between two nodes.

  A transport library -- TCP, TLS, WebSocket, a pipe -- implements these
  three protocols and nothing else.  ensemble.node does the rest on top:
  the handshake that names the peer and proves it holds the cookie, the
  framing of the stream, the codec, and every operation of the
  distribution protocol.  A transport resolves a node name to an address
  however it likes (a fixed table, DNS, a port mapper).

  The loopback transport here connects nodes in one VM, through channels;
  it is for tests, and for an example of the protocols."
  (:require [clojure.core.async :as a]))

(defprotocol Transport
  (-listen [t node accept]
    "Accept connections to node: call (accept conn) with each one opened
    to it.  Returns a Listener.")
  (-connect [t node peer]
    "Open a byte stream from node to node peer.  Returns a Conn, or nil
    when peer cannot be reached."))

(defprotocol Listener
  (-stop [l] "Stop accepting connections."))

(defprotocol Conn
  (-start [c on-bytes on-close]
    "Begin reading, once: (on-bytes bs) with each chunk of the stream, a
    byte array, in order -- a chunk may hold part of what the peer wrote,
    or several writes -- and (on-close) once when the stream ends, from
    either end.")
  (-write [c bs]
    "Write the byte array bs, whole and after anything written before.
    Called from any fiber, at once; each call's bytes stay together.
    False once the stream is closed.")
  (-close [c] "Close the stream.  Both ends' on-close run."))

;; --- the loopback transport ---------------------------------------------

(defonce ^:private listening
  ;; node -> accept fn, for every loopback transport in this VM
  (atom {}))

(defn- chunks
  "bs as chunks of at most n bytes, or whole when n is nil."
  [^bytes bs n]
  (if (or (nil? n) (<= (alength bs) n))
    [bs]
    (for [i (range 0 (alength bs) n)]
      (let [m (min n (- (alength bs) i))
            out (byte-array m)]
        (System/arraycopy bs i out 0 m)
        out))))

(defrecord LoopbackEnd [in out closed chunk]
  Conn
  (-start [_ on-bytes on-close]
    (a/go-loop []
      (if-let [bs (a/<! in)]
        (do (doseq [c (chunks bs chunk)] (on-bytes c)) (recur))
        (on-close))))
  (-write [_ bs]
    (and (not @closed) (boolean (a/put! out bs))))
  (-close [_]
    (when (compare-and-set! closed false true)
      (a/close! in)
      (a/close! out))))

(defrecord LoopbackListener [node]
  Listener
  (-stop [_] (swap! listening dissoc node)))

(defrecord Loopback [chunk]
  Transport
  (-listen [_ node accept]
    (swap! listening assoc node accept)
    (->LoopbackListener node))
  (-connect [_ node peer]
    (when-let [accept (get @listening peer)]
      (let [to-peer (a/chan 65536)
            from-peer (a/chan 65536)
            closed (atom false)
            mine (->LoopbackEnd from-peer to-peer closed chunk)
            theirs (->LoopbackEnd to-peer from-peer closed chunk)]
        (accept theirs)
        mine))))

(defn loopback
  "The transport between nodes in this VM.  With {:chunk n} each write
  reaches the other end in chunks of at most n bytes, as a network may
  split it."
  ([] (loopback {}))
  ([{:keys [chunk]}] (->Loopback chunk)))

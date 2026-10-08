(ns ensemble.dist
  "The decisions of Erlang distribution, as pure functions: where a send
  goes, what a pid inside a message is on arrival, and what a lost
  connection does to the links and monitors that cross it.  ensemble.node
  makes every one of them through these.

  A pid is [:Pid node id creation], creation the number its node drew
  when it started, so a pid of an earlier run of a node names no process
  of the run after it; a destination is a pid, [:Name nm] or [:At nm
  node], Erlang's {Name, Node}.

  Before two nodes exchange anything else they shake hands, and the
  handshake's every decision is here too; so is how a frame's length is
  written on the wire."
  )

(defn route
  "Where a send to dest goes from node self, now in its run creation:
  [:Here id], [:Gone] for a pid of an earlier run of self, [:There node id
  creation], [:Named nm] or [:NamedThere node nm]."
  [self creation dest]
  (case (first dest)
    :Pid (let [[_ n id cr] dest]
           (if (= n self)
             (if (= cr creation) [:Here id] [:Gone])
             [:There n id cr]))
    :Name (let [[_ nm] dest] [:Named nm])
    :At (let [[_ nm n] dest] (if (= n self) [:Named nm] [:NamedThere n nm]))))

(defn arrive
  "What a pid that arrives at node self, in its run creation, is there: a
  pid of self's current run is its own process again, [:Local id]; any
  other, a pid of an earlier run among them, stays as it came."
  [self creation pid]
  (case (first pid)
    :Pid (let [[_ n id cr] pid]
           (if (and (= n self) (= cr creation)) [:Local id] pid))
    pid))

(defn on-nodedown
  "What losing node n does here, given the links and monitors from local
  processes to processes elsewhere and the node monitors: each link to a
  process of n is broken with :noconnection, each monitor of one gets a
  DOWN with :noconnection, each monitor of n gets [:nodedown n].  In that
  order; entries for other nodes do nothing."
  [n links mons nmons]
  (vec (concat
         (keep (fn [l] (let [[_ me pn pid cr] l]
                         (when (= pn n) [:Signal me [:Pid pn pid cr] :noconnection])))
               links)
         (keep (fn [m] (let [[_ me ref pn pid cr] m]
                         (when (= pn n) [:Deliver me [:DOWN ref :process [:Pid pn pid cr] :noconnection]])))
               mons)
         (keep (fn [x] (let [[_ me mn] x]
                         (when (= mn n) [:Deliver me [:nodedown mn]])))
               nmons))))

;; --- the handshake ----------------------------------------------------------
;; Erlang's, with the cookie digest left to a signer the node supplies:
;;
;;   A -> B  [:name A creation]
;;   B -> A  [:status s], and if s is :ok, [:challenge B creation cB]
;;   A -> B  [:reply cA (sign [cB A B])]
;;   B -> A  [:ack (sign [cA B A])]
;;
;; A signature covers the challenge, the node signing and the node it is
;; meant for.  Erlang's covers the challenge alone, which lets a node
;; without the cookie that gets one node to dial it pass the challenge of
;; a third along and answer that third node as the first.
;;
;; Each step takes this end's state, the frame read, and ctx: {:up-creation
;; f :pending? f :fresh c :sign f} -- the creation of the run of a peer
;; this node is connected to (nil when it is not), whether it is connecting
;; to a peer; a fresh challenge, should this step need one; and how to sign
;; a challenge with the cookie.  It answers [:Next hs frames],
;; [:Up hs frames] once the peer is proved, or [:Fail reason frames]: the
;; frames to send either way.

(defn- sorts-higher?
  "Does node name a sort above node name b?"
  [a b]
  (pos? (compare (str a) (str b))))

(defn name-status
  "What node self answers peer, in its run creation, with: :nok for its
  own name, or a peer it is connecting to itself while its own name sorts
  higher (that connection is kept instead); :alive when it is connected to
  that run of peer already; :ok -- also when the connection it has is to
  an earlier run of peer, which this one replaces."
  [self peer creation up-creation pending?]
  (cond
    (= self peer) :nok
    (= creation up-creation) :alive
    (some? up-creation) :ok
    (and pending? (sorts-higher? self peer)) :nok
    :else :ok))

(defn on-up
  "What node self does with a connection to a peer that has just come up,
  given the one it has already: nil, or {:creation c :initiator node},
  initiator the node that opened it.  :install it when there is none;
  :replace-down when the one there is to an earlier run of the peer, which
  goes down first; when both are to the same run, both ends keep the one
  the higher-named node opened, so they agree: :replace or :drop."
  [self existing new]
  (cond
    (nil? existing) :install
    (not= (:creation existing) (:creation new)) :replace-down
    (and (not= (:initiator existing) (:initiator new))
         (sorts-higher? (:initiator new) (:initiator existing))) :replace
    :else :drop))

(defn open-handshake
  "Start a handshake from node self, in its run creation, to node peer."
  [self creation peer]
  [:Next {:role :initiator :self self :creation creation :peer peer :phase :status}
   [[:name self creation]]])

(defn accept-handshake
  "The state of node self, in its run creation, waiting for a peer's name."
  [self creation]
  {:role :acceptor :self self :creation creation :phase :name})

(defn handshake
  "One step of the handshake: hs read frame, in ctx (see above)."
  [hs frame ctx]
  (let [tag (when (vector? frame) (first frame))
        phase (:phase hs)
        sign (:sign ctx)]
    (cond
      (and (= :name phase) (= :name tag))
      (let [[_ peer cr] frame
            st (name-status (:self hs) peer cr ((:up-creation ctx) peer) ((:pending? ctx) peer))]
        (if (= :ok st)
          [:Next (assoc hs :phase :reply :peer peer :peer-creation cr :mine (:fresh ctx))
           [[:status :ok] [:challenge (:self hs) (:creation hs) (:fresh ctx)]]]
          [:Fail [:refused st] [[:status st]]]))
      (and (= :status phase) (= :status tag))
      (if (= :ok (second frame))
        [:Next (assoc hs :phase :challenge) []]
        [:Fail [:refused (second frame)] []])
      (and (= :challenge phase) (= :challenge tag))
      (let [[_ nm cr c] frame]
        (if (= nm (:peer hs))
          [:Next (assoc hs :phase :ack :peer-creation cr :mine (:fresh ctx))
           [[:reply (:fresh ctx) (sign [c (:self hs) nm])]]]
          [:Fail [:wrong-node nm] []]))
      (and (= :reply phase) (= :reply tag))
      (let [[_ c digest] frame]
        (if (= digest (sign [(:mine hs) (:peer hs) (:self hs)]))
          [:Up (assoc hs :phase :up) [[:ack (sign [c (:self hs) (:peer hs)])]]]
          [:Fail :bad-cookie []]))
      (and (= :ack phase) (= :ack tag))
      (if (= (second frame) (sign [(:mine hs) (:peer hs) (:self hs)]))
        [:Up (assoc hs :phase :up) []]
        [:Fail :bad-cookie []])
      :else [:Fail [:unexpected frame] []])))

;; --- framing ----------------------------------------------------------------

(defn length-header
  "A frame's length n as the four bytes before its payload, big-endian."
  [n]
  [(bit-and (bit-shift-right n 24) 255)
   (bit-and (bit-shift-right n 16) 255)
   (bit-and (bit-shift-right n 8) 255)
   (bit-and n 255)])

(defn header-length
  "The length four header bytes give."
  [[a b c d]]
  (+ (* a 16777216) (* b 65536) (* c 256) d))

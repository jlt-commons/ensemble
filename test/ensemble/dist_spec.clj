(ns ensemble.dist-spec
  "Contract for ensemble.dist: the decisions of Erlang distribution, as the
  Erlang reference manual and the erlang module docs state them.

  - A pid names its node.  A message to a pid on this node, or to a name
    registered here, is delivered here; one to a pid or a {Name, Node}
    elsewhere goes to that node.  The same send, !, serves both.
  - Pids travel inside messages.  On arrival, a pid of the receiving node is
    that node's own process again; a pid of any other node stays remote.
    (Where in a frame a pid is, the node's envelope says; this decides
    each one.)
  - When the connection to a node goes down, every link to one of its
    processes is broken with the exit reason noconnection, every monitor of
    one gets a DOWN with noconnection, and every monitor_node of it gets
    {nodedown, Node}.  Links and monitors on other nodes are untouched, and
    each fires once.
  - A pid carries its node's creation, a number fresh each time the node
    starts: a pid of an earlier run of this node names no process here,
    though its id may be in use again.
  - Two nodes connect with a handshake: the one connecting sends its name
    and creation; the other answers with a status -- ok, nok when the name
    is its own or it is connecting to the same peer and its own name sorts
    higher (so of two simultaneous connections one is kept), alive when it
    is already connected to that run of the peer -- and with a challenge.
    A connection to an earlier run of the peer gives way to the new run, as
    Erlang's does, and that earlier run goes down.  Each end proves it holds
    the cookie by signing the other's challenge; a wrong signature ends the
    handshake, and only after it does any other frame count.
  - A frame on the wire is its length as four bytes, big-endian, then its
    payload."
  (:require [writ.spec :refer [spec data ann refine graph law calls]]
            [ensemble.dist :as dist]
            [ensemble.node :as node]))

(spec ensemble.dist {:require :proved})

(data Dest (Pid Keyword Nat Nat) (Name Keyword) (At Keyword Keyword))
(data Route (Here Nat) (Gone) (There Keyword Nat Nat) (Named Keyword) (NamedThere Keyword Keyword))
;; a link or monitor from local process me to pid [:Pid node id creation]
(data Link (Link Nat Keyword Nat Nat))
(data Mon (Mon Nat Any Keyword Nat Nat))
(data NodeMon (NodeMon Nat Keyword))
(data Effect (Signal Nat Any Any) (Deliver Nat Any))
(data Step (Next Any (Vec Any)) (Up Any (Vec Any)) (Fail Any (Vec Any)))

(ann route [Keyword Nat Dest -> Route])
(ann arrive [Keyword Nat Dest -> Any])
(ann on-up [Keyword Any Any -> Keyword])
(ann on-nodedown [Keyword (Vec Link) (Vec Mon) (Vec NodeMon) -> (Vec Effect)])
(ann name-status [Keyword Keyword Nat Any Bool -> Keyword])
(ann sorts-higher? [Keyword Keyword -> Bool])
(ann open-handshake [Keyword Nat Keyword -> Step])
(ann accept-handshake [Keyword Nat -> Any])
(ann handshake [Any Any Any -> Step])
(ann length-header [Nat -> (Vec Nat)])
(ann header-length [(Vec Nat) -> Nat])

(refine Local  [r Route] (contains? #{:Here :Named :Gone} (first r)))
(refine Remote [r Route] (contains? #{:There :NamedThere} (first r)))

(graph send
  {:states {:dest Dest, :local Local, :remote Remote}
   :edges  {:dest {[route Keyword Nat _] #{:local :remote}}}})

;; --- routing ------------------------------------------------------------

(law a-pid-here-is-delivered-here
  (forall [self Keyword, cr Nat, id Nat] (= (route self cr [:Pid self id cr]) [:Here id])))

(law a-pid-of-an-earlier-run-is-gone
  (forall [self Keyword, cr Nat, old Nat, id Nat]
    (=> (not= cr old) (= (route self cr [:Pid self id old]) [:Gone]))))

(law a-pid-elsewhere-goes-to-its-node
  (forall [self Keyword, cr Nat, n Keyword, id Nat, c Nat]
    (=> (not= self n) (= (route self cr [:Pid n id c]) [:There n id c]))))

(law a-name-alone-is-this-nodes
  (forall [self Keyword, cr Nat, nm Keyword] (= (route self cr [:Name nm]) [:Named nm])))

(law a-name-at-this-node-is-this-nodes
  (forall [self Keyword, cr Nat, nm Keyword] (= (route self cr [:At nm self]) [:Named nm])))

(law a-name-at-another-node-goes-there
  (forall [self Keyword, cr Nat, nm Keyword, n Keyword]
    (=> (not= self n) (= (route self cr [:At nm n]) [:NamedThere n nm]))))

;; --- pids in messages ---------------------------------------------------

(law a-pid-of-the-receiver-is-its-own-again
  (forall [self Keyword, cr Nat, id Nat] (= (arrive self cr [:Pid self id cr]) [:Local id])))

(law a-pid-of-an-earlier-run-stays-a-pid
  (forall [self Keyword, cr Nat, old Nat, id Nat]
    (=> (not= cr old) (= (arrive self cr [:Pid self id old]) [:Pid self id old]))))

(law a-pid-of-another-node-stays-remote
  (forall [self Keyword, cr Nat, n Keyword, id Nat]
    (=> (not= self n) (= (arrive self cr [:Pid n id cr]) [:Pid n id cr]))))

;; --- a node goes down ---------------------------------------------------

(defn effects-on
  "The docs' effects of node n going down, entry by entry."
  [n links mons nmons]
  (vec (concat
         (for [[_ me pn pid cr] links :when (= pn n)]
           [:Signal me [:Pid pn pid cr] :noconnection])
         (for [[_ me ref pn pid cr] mons :when (= pn n)]
           [:Deliver me [:DOWN ref :process [:Pid pn pid cr] :noconnection]])
         (for [[_ me mn] nmons :when (= mn n)]
           [:Deliver me [:nodedown mn]]))))

(law nodedown-is-the-docs
  (forall [n Keyword, links (Vec Link), mons (Vec Mon), nmons (Vec NodeMon)]
    (= (on-nodedown n links mons nmons) (effects-on n links mons nmons))))

(law a-link-to-the-lost-node-breaks-with-noconnection
  (forall [n Keyword, me Nat, id Nat, cr Nat]
    (= (on-nodedown n [[:Link me n id cr]] [] [])
       [[:Signal me [:Pid n id cr] :noconnection]])))

(law a-link-to-another-node-is-untouched
  (forall [n Keyword, m Keyword, me Nat, id Nat, cr Nat]
    (=> (not= n m) (= (on-nodedown n [[:Link me m id cr]] [] []) []))))

(law a-monitor-of-the-lost-node-gets-down-noconnection
  (forall [n Keyword, me Nat, ref Any, id Nat, cr Nat]
    (= (on-nodedown n [] [[:Mon me ref n id cr]] [])
       [[:Deliver me [:DOWN ref :process [:Pid n id cr] :noconnection]]])))

(law monitor-node-gets-nodedown
  (forall [n Keyword, me Nat]
    (= (on-nodedown n [] [] [[:NodeMon me n]]) [[:Deliver me [:nodedown n]]])))

;; --- the handshake -------------------------------------------------------

(law a-name-status-is-ok-for-a-new-peer
  (forall [self Keyword, peer Keyword, cr Nat]
    (=> (not= self peer) (= :ok (name-status self peer cr nil false)))))

(law a-node-refuses-its-own-name
  (forall [self Keyword, cr Nat, up Nat, pending Bool] (= :nok (name-status self self cr up pending))))

(law a-node-already-connected-is-alive
  (forall [self Keyword, peer Keyword, cr Nat, pending Bool]
    (=> (not= self peer) (= :alive (name-status self peer cr cr pending)))))

(law a-restarted-peer-is-let-in
  (forall [self Keyword, peer Keyword, cr Nat, old Nat, pending Bool]
    (=> (and (not= self peer) (not= cr old)) (= :ok (name-status self peer cr old pending)))))

;; of two simultaneous connections, the one from the higher name is kept
(law a-simultaneous-connection-keeps-the-higher-names
  (and (= :nok (name-status :b.vm :a.vm 1 nil true))
       (= :ok (name-status :a.vm :b.vm 1 nil true))))

;; a connection that comes up beside another: both ends decide alike
(law a-first-connection-is-installed
  (forall [self Keyword, cr Nat, i Keyword]
    (= :install (on-up self nil {:creation cr :initiator i}))))

(law a-connection-to-a-new-run-replaces-the-old-which-goes-down
  (forall [self Keyword, cr Nat, old Nat, i Keyword, j Keyword]
    (=> (not= cr old)
        (= :replace-down (on-up self {:creation old :initiator i} {:creation cr :initiator j})))))

(law of-two-connections-to-one-run-the-higher-openers-stays
  (and (= :replace (on-up :a.vm {:creation 1 :initiator :a.vm} {:creation 1 :initiator :b.vm}))
       (= :drop (on-up :a.vm {:creation 1 :initiator :b.vm} {:creation 1 :initiator :a.vm}))
       (= :replace (on-up :b.vm {:creation 1 :initiator :a.vm} {:creation 1 :initiator :b.vm}))
       (= :drop (on-up :b.vm {:creation 1 :initiator :b.vm} {:creation 1 :initiator :a.vm}))))

(defn sign-with
  "A signer holding cookie: what the shell computes as an HMAC, modelled as
  data the other end can check only by holding the same cookie."
  [cookie]
  (fn [challenge] [:signed cookie challenge]))

(defn ctx-for
  "A handshake context with no other connection, signing with cookie and
  drawing fresh as its challenge."
  [cookie fresh]
  {:up-creation (fn [_] nil) :pending? (fn [_] false) :fresh fresh :sign (sign-with cookie)})

;; a's name reaches b, b's status and challenge reach a, a's reply reaches
;; b, b's ack reaches a
(law the-same-cookie-brings-both-ends-up
  (let [[_ ia out-a] (open-handshake :a.vm 1 :b.vm)
        [_ hb out-b] (handshake (accept-handshake :b.vm 2) (first out-a) (ctx-for "c" :cb))
        [_ ia] (handshake ia (first out-b) (ctx-for "c" :ca))
        [_ ia out-a] (handshake ia (second out-b) (ctx-for "c" :ca))
        [tb hb out-b] (handshake hb (first out-a) (ctx-for "c" :cb))
        [ta ia] (handshake ia (first out-b) (ctx-for "c" :ca))]
    (and (= :Up ta) (= :Up tb)
         (= [:b.vm 2] [(:peer ia) (:peer-creation ia)])
         (= [:a.vm 1] [(:peer hb) (:peer-creation hb)]))))

(law a-wrong-cookie-is-refused-by-the-acceptor
  (let [[_ ia out-a] (open-handshake :a.vm 1 :b.vm)
        [_ hb out-b] (handshake (accept-handshake :b.vm 2) (first out-a) (ctx-for "d" :cb))
        [_ ia] (handshake ia (first out-b) (ctx-for "c" :ca))
        [_ _ out-a] (handshake ia (second out-b) (ctx-for "c" :ca))]
    (= [:Fail :bad-cookie []] (handshake hb (first out-a) (ctx-for "d" :cb)))))

(law the-acceptor-proves-itself-too
  ;; the initiator checks the acceptor's answer to its challenge
  (let [ctx {:up-creation (fn [_] nil) :pending? (fn [_] false) :fresh :ca :sign (sign-with "c")}
        [_ ia _] (open-handshake :a.vm 1 :b.vm)
        [_ ia] (handshake ia [:status :ok] ctx)
        [_ ia] (handshake ia [:challenge :b.vm 2 :cb] ctx)]
    (= [:Fail :bad-cookie []] (handshake ia [:ack [:signed "d" :ca]] ctx))))

(law a-refusing-status-ends-the-handshake
  (let [ctx {:up-creation (fn [_] nil) :pending? (fn [_] false) :fresh :ca :sign (sign-with "c")}
        [_ ia _] (open-handshake :a.vm 1 :b.vm)]
    (and (= [:Fail [:refused :nok] []] (handshake ia [:status :nok] ctx))
         (= [:Fail [:refused :alive] []] (handshake ia [:status :alive] ctx)))))

(law the-acceptor-answers-a-refused-name-and-stops
  (let [ctx {:up-creation (fn [p] (when (= p :a.vm) 1)) :pending? (fn [_] false) :fresh :cb :sign (sign-with "c")}]
    (and (= [:Fail [:refused :alive] [[:status :alive]]]
            (handshake (accept-handshake :b.vm 2) [:name :a.vm 1] ctx))
         (= [:Fail [:refused :nok] [[:status :nok]]]
            (handshake (accept-handshake :b.vm 2) [:name :b.vm 1] ctx)))))

(law the-peer-must-be-the-node-asked-for
  (let [ctx {:up-creation (fn [_] nil) :pending? (fn [_] false) :fresh :ca :sign (sign-with "c")}
        [_ ia _] (open-handshake :a.vm 1 :b.vm)
        [_ ia] (handshake ia [:status :ok] ctx)]
    (= [:Fail [:wrong-node :c.vm] []] (handshake ia [:challenge :c.vm 2 :cb] ctx))))

(law a-frame-out-of-turn-ends-the-handshake
  (let [ctx {:up-creation (fn [_] nil) :pending? (fn [_] false) :fresh :cb :sign (sign-with "c")}]
    (and (= [:Fail [:unexpected [:send 1 :x]] []]
            (handshake (accept-handshake :b.vm 2) [:send 1 :x] ctx))
         (= [:Fail [:unexpected [:ack :x]] []]
            (handshake (second (open-handshake :a.vm 1 :b.vm)) [:ack :x] ctx)))))

(law opening-sends-the-name-and-creation
  (forall [self Keyword, cr Nat, peer Keyword]
    (= [[:name self cr]] (nth (open-handshake self cr peer) 2))))

;; --- framing ----------------------------------------------------------------

(law a-header-is-four-bytes-big-endian
  (and (= [0 0 0 0] (length-header 0))
       (= [0 0 1 0] (length-header 256))
       (= [1 2 3 4] (length-header 16909060))
       (= [255 255 255 255] (length-header 4294967295))))

(law a-header-reads-back
  {:require :tested :because "bit shifts are outside what the prover models"}
  ;; a length past four bytes' reach keeps only what fits
  (forall [n Nat] (= (mod n 4294967296) (header-length (length-header n)))))

;; --- node follows these decisions ---------------------------------------

;; every operation on a pid goes where route says; a frame's pids are what
;; arrive says; a lost connection does what on-nodedown says
(calls node/deliver-remote {:through [dist/route]})
(calls node/signal-remote {:through [dist/route]})
(calls node/link-remote {:through [dist/route]})
(calls node/unlink-remote {:through [dist/route]})
(calls node/monitor-remote {:through [dist/route]})
(calls node/demonitor-remote {:through [dist/route]})
(calls node/handle-frame {:through [dist/arrive]})
(calls node/handshake-step! {:through [dist/handshake]})
(calls node/nodedown! {:through [dist/on-nodedown]})

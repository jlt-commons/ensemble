(ns ensemble.dist-spec
  "Contract for ensemble.dist: the decisions of Erlang distribution, as the
  Erlang reference manual and the erlang module docs state them.

  - A pid names its node.  A message to a pid on this node, or to a name
    registered here, is delivered here; one to a pid or a {Name, Node}
    elsewhere goes to that node.  The same send, !, serves both.
  - Pids travel inside messages.  On arrival, a pid of the receiving node is
    that node's own process again; a pid of any other node stays remote.
  - When the connection to a node goes down, every link to one of its
    processes is broken with the exit reason noconnection, every monitor of
    one gets a DOWN with noconnection, and every monitor_node of it gets
    {nodedown, Node}.  Links and monitors on other nodes are untouched, and
    each fires once."
  (:require [writ.spec :refer [spec data ann refine graph law calls]]
            [ensemble.dist :as dist]
            [ensemble.node :as node]))

(spec ensemble.dist {:require :proved})

(data Dest (Pid Keyword Nat) (Name Keyword) (At Keyword Keyword))
(data Route (Here Nat) (There Keyword Nat) (Named Keyword) (NamedThere Keyword Keyword))
;; a link or monitor from local process me to pid [:Pid node id]
(data Link (Link Nat Keyword Nat))
(data Mon (Mon Nat Any Keyword Nat))
(data NodeMon (NodeMon Nat Keyword))
(data Effect (Signal Nat Any Any) (Deliver Nat Any))

(ann route [Keyword Dest -> Route])
(ann arrive [Keyword Any -> Any])
(ann on-nodedown [Keyword (Vec Link) (Vec Mon) (Vec NodeMon) -> (Vec Effect)])

(refine Local  [r Route] (contains? #{:Here :Named} (first r)))
(refine Remote [r Route] (contains? #{:There :NamedThere} (first r)))

(graph send
  {:states {:dest Dest, :local Local, :remote Remote}
   :edges  {:dest {[route Keyword _] #{:local :remote}}}})

;; --- routing ------------------------------------------------------------

(law a-pid-here-is-delivered-here
  (forall [self Keyword, id Nat] (= (route self [:Pid self id]) [:Here id])))

(law a-pid-elsewhere-goes-to-its-node
  (forall [self Keyword, n Keyword, id Nat]
    (=> (not= self n) (= (route self [:Pid n id]) [:There n id]))))

(law a-name-alone-is-this-nodes
  (forall [self Keyword, nm Keyword] (= (route self [:Name nm]) [:Named nm])))

(law a-name-at-this-node-is-this-nodes
  (forall [self Keyword, nm Keyword] (= (route self [:At nm self]) [:Named nm])))

(law a-name-at-another-node-goes-there
  (forall [self Keyword, nm Keyword, n Keyword]
    (=> (not= self n) (= (route self [:At nm n]) [:NamedThere n nm]))))

;; --- pids in messages ---------------------------------------------------

(law a-pid-of-the-receiver-is-its-own-again
  {:require :tested :because "a pid may be anywhere in a message, which clojure.walk walks and the prover does not model"}
  (forall [self Keyword, id Nat] (= (arrive self [:Pid self id]) [:Local id])))

(law a-pid-of-another-node-stays-remote
  {:require :tested :because "a pid may be anywhere in a message, which clojure.walk walks and the prover does not model"}
  (forall [self Keyword, n Keyword, id Nat]
    (=> (not= self n) (= (arrive self [:Pid n id]) [:Pid n id]))))

(law pids-inside-a-message-arrive-too
  {:require :tested :because "a pid may be anywhere in a message, which clojure.walk walks and the prover does not model"}
  (forall [self Keyword, n Keyword, id Nat, k Keyword]
    (=> (not= self n)
        (= (arrive self [k [:Pid self id] [:Pid n id]]) [k [:Local id] [:Pid n id]]))))

(law a-pid-inside-a-map-arrives-too
  {:require :tested :because "a pid may be anywhere in a message, which clojure.walk walks and the prover does not model"}
  (forall [self Keyword, id Nat]
    (= (arrive self {:to [:Pid self id] :from #{[:Pid self id]}}) {:to [:Local id] :from #{[:Local id]}})))

(law data-without-pids-arrives-unchanged
  {:require :tested :because "a pid may be anywhere in a message, which clojure.walk walks and the prover does not model"}
  (forall [self Keyword, xs (Vec Int), k Keyword]
    (= (arrive self [k xs]) [k xs])))

;; --- a node goes down ---------------------------------------------------

(defn effects-on
  "The docs' effects of node n going down, entry by entry."
  [n links mons nmons]
  (vec (concat
         (for [[_ me pn pid] links :when (= pn n)]
           [:Signal me [:Pid pn pid] :noconnection])
         (for [[_ me ref pn pid] mons :when (= pn n)]
           [:Deliver me [:DOWN ref :process [:Pid pn pid] :noconnection]])
         (for [[_ me mn] nmons :when (= mn n)]
           [:Deliver me [:nodedown mn]]))))

(law nodedown-is-the-docs
  (forall [n Keyword, links (Vec Link), mons (Vec Mon), nmons (Vec NodeMon)]
    (= (on-nodedown n links mons nmons) (effects-on n links mons nmons))))

(law a-link-to-the-lost-node-breaks-with-noconnection
  (forall [n Keyword, me Nat, id Nat]
    (= (on-nodedown n [[:Link me n id]] [] [])
       [[:Signal me [:Pid n id] :noconnection]])))

(law a-link-to-another-node-is-untouched
  (forall [n Keyword, m Keyword, me Nat, id Nat]
    (=> (not= n m) (= (on-nodedown n [[:Link me m id]] [] []) []))))

(law a-monitor-of-the-lost-node-gets-down-noconnection
  (forall [n Keyword, me Nat, ref Any, id Nat]
    (= (on-nodedown n [] [[:Mon me ref n id]] [])
       [[:Deliver me [:DOWN ref :process [:Pid n id] :noconnection]]])))

(law monitor-node-gets-nodedown
  (forall [n Keyword, me Nat]
    (= (on-nodedown n [] [] [[:NodeMon me n]]) [[:Deliver me [:nodedown n]]])))

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
(calls node/nodedown! {:through [dist/on-nodedown]})

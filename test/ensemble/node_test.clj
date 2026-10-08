(ns ensemble.node-test
  "Processes on two nodes in one VM, over the loopback transport: every
  operation between them crosses as a frame, printed and read back."
  (:require [clojure.core.async :as a]
            [clojure.test :refer [deftest is testing]]
            [ensemble.actor :as act :refer [receive]]
            [ensemble.gen-server :as gs]
            [ensemble.node :as node]
            [ensemble.sys :as sys]))

(defn- sleep [ms] (a/<!! (a/timeout ms)))

(defn- eventually [pred]
  (loop [i 0]
    (cond (pred) true
          (> i 200) false
          :else (do (sleep 10) (recur (inc i))))))

(defn- two-nodes
  "Two fresh nodes on the loopback transport, connected: [a b]."
  []
  (let [a (keyword (str (gensym "a") ".vm"))
        b (keyword (str (gensym "b") ".vm"))]
    (node/start! a (node/loopback) {:spawn #{'ensemble.node-test}})
    (node/start! b (node/loopback) {:spawn #{'ensemble.node-test}})
    (node/with-node a (node/connect! b))
    [a b]))

;; fns a node starts by name, as spawn(Node, M, F, A) does

(defn echo
  "Answer [from msg] with [:echo msg] to from, for ever."
  []
  (loop [] (receive [[from msg] (do (act/! from [:echo msg]) (recur))])))

(defn crash-on [x] (receive [msg (when (= msg x) (act/exit! :crashed))]))

(defn idle [] (receive [:stop :ok]))

(defn- ask
  "From a fresh actor on node n, send msg to dest with a reply address and
  wait for the answer."
  [n dest msg]
  (let [p (promise)]
    (node/with-node n
      (act/spawn (fn [] (act/! dest [(act/self) msg])
                   (deliver p (receive [x x] [:after 2000 :timeout])))))
    (deref p 3000 :no-answer)))

(deftest a-message-reaches-a-pid-on-another-node-and-a-pid-in-it-comes-back
  (let [[a b] (two-nodes)
        e (node/with-node a (node/spawn-on b `echo []))]
    (is (= b (:node e)) "the pid names its node")
    (is (= [:echo :hi] (ask a e :hi)) "the reply address crossed as a pid and came back")))

(deftest a-name-on-another-node-is-reached-as-name-at-node
  (let [[a b] (two-nodes)]
    (node/with-node b (act/register! :svc (act/spawn echo)))
    (is (= [:echo 1] (ask a [:At :svc b] 1)))))

(deftest a-remote-crash-reaches-a-linked-process
  (let [[a b] (two-nodes)
        got (promise)]
    (node/with-node a
      (act/spawn (fn []
                   (act/trap-exit!)
                   (let [r (node/spawn-on b `crash-on [:die] {:link true})]
                     (act/! r :die)
                     (deliver got (receive [[:EXIT from reason] [from reason]] [:after 2000 :timeout]))))))
    (let [[from reason] (deref got 3000 nil)]
      (is (= b (:node from)))
      (is (= :crashed reason)))))

(deftest a-remote-exit-kills-a-linked-process-that-does-not-trap
  (let [[a b] (two-nodes)
        me (node/with-node a
             (act/spawn (fn [] (let [r (node/spawn-on b `crash-on [:die] {:link true})]
                                 (act/! r :die)
                                 (receive [:never :ok])))))]
    (is (= :crashed (act/exit-reason me)))))

(deftest a-monitor-across-nodes-gets-down
  (let [[a b] (two-nodes)
        got (promise)]
    (node/with-node a
      (act/spawn (fn []
                   (let [r (node/spawn-on b `crash-on [:die])
                         ref (act/monitor! r)]
                     (act/! r :die)
                     (deliver got (receive [[:DOWN ref :process p reason] [p reason]] [:after 2000 :timeout]))))))
    (let [[p reason] (deref got 3000 nil)]
      (is (= b (:node p)))
      (is (= :crashed reason)))))

(deftest a-link-to-a-gone-remote-process-is-noproc
  (let [[a b] (two-nodes)
        got (promise)]
    (node/with-node a
      (act/spawn (fn []
                   (act/trap-exit!)
                   (act/link! (node/->RemotePid b 999999 (node/creation b)))
                   (deliver got (receive [[:EXIT _ reason] reason] [:after 2000 :timeout])))))
    (is (= :noproc (deref got 3000 nil)))))

(defn- shape
  "m with each process in it as :pid, to compare messages whose pids and refs
  are fresh."
  [m]
  (if (vector? m) (mapv #(if (act/pid? %) :pid %) m) m))

(deftest a-lost-connection-breaks-links-and-fires-monitors
  (let [[a b] (two-nodes)
        ready (promise)
        got (promise)]
    (node/with-node a
      (act/spawn (fn []
                   (act/trap-exit!)
                   (let [r (node/spawn-on b `idle [] {:link true})]
                     (act/monitor! r)
                     (node/monitor-node! b)
                     (deliver ready true)
                     (deliver got (set (map shape (doall (for [_ (range 3)]
                                                          (receive [m m] [:after 2000 :timeout]))))))))))
    (deref ready 3000 nil)
    (node/with-node a (node/disconnect! b))
    (let [msgs (deref got 3000 nil)]
      (is (contains? msgs [:EXIT :pid :noconnection]) (pr-str msgs))
      (is (some #(and (vector? %) (= :DOWN (first %)) (= :noconnection (last %))) msgs) (pr-str msgs))
      (is (contains? msgs [:nodedown b]) (pr-str msgs)))))

;; a gen-server on another node

(defrecord Counter []
  gs/Server
  (init [_] [:ok 0])
  (handle-call [_ req _ n] (case (first req) :add (let [n (+ n (second req))] [:reply n n]) :get [:reply n n]))
  (handle-cast [_ _ n] [:noreply n])
  (handle-info [_ _ n] [:noreply n])
  (handle-timeout [_ n] [:noreply n])
  (terminate [_ _ _] nil))

(defn start-counter [] (gs/start (->Counter) {:name :counter}) (receive [:never :ok]))

(deftest a-gen-server-call-crosses-nodes
  (let [[a b] (two-nodes)]
    (node/with-node a (node/spawn-on b `start-counter []))
    (is (eventually #(node/with-node b (act/whereis :counter))))
    (testing "from an actor, and from outside any actor"
      (let [p (promise)]
        (node/with-node a (act/spawn (fn [] (deliver p (gs/call! [:At :counter b] [:add 2])))))
        (is (= 2 (deref p 3000 nil))))
      (is (= 5 (node/with-node a (gs/call! (node/with-node b (act/whereis :counter)) [:add 3]))) "a local handle works too")
      (is (= 5 (node/with-node a (gs/call! [:At :counter b] [:get])))))))

(deftest a-node-spawns-only-what-it-allows
  (let [a (keyword (str (gensym "a") ".vm"))
        b (keyword (str (gensym "b") ".vm"))]
    (node/start! a (node/loopback))
    (node/start! b (node/loopback) {:spawn (fn [sym] (= sym `idle))})
    (is (= [:not-allowed `echo]
           (try (node/with-node a (node/spawn-on b `echo [])) (catch Throwable e (:reason (ex-data e))))))
    (is (= [:not-allowed `echo]
           (try (node/with-node b (node/spawn-on a `echo [])) (catch Throwable e (:reason (ex-data e))))))
    (is (act/pid? (node/with-node a (node/spawn-on b `idle []))))))

;; --- multi_call and abcast ------------------------------------------------------

(defrecord Named []
  gs/Server
  (init [_] [:ok nil])
  (handle-call [_ req _ st] [:reply [(act/node) req] st])
  (handle-cast [_ req _] [:noreply req])
  (handle-info [_ _ st] [:noreply st])
  (handle-timeout [_ st] [:noreply st])
  (handle-continue [_ _ st] [:noreply st])
  (terminate [_ _ _] nil))

(deftest multi-call-asks-the-name-on-each-node
  (let [[a b] (two-nodes)
        c (keyword (str (gensym "c") ".vm"))]
    (node/with-node a (gs/start (->Named) {:name :svc}))
    (node/with-node b (gs/start (->Named) {:name :svc}))
    (let [[replies bad] (node/with-node a (gs/multi-call [a b c] :svc :q 2000))]
      (is (= #{[a [a :q]] [b [b :q]]} (set replies)))
      (is (= [c] bad) "a node that cannot be reached is bad"))
    (is (= #{a b} (set (map first (first (node/with-node a (gs/multi-call :svc :q)))))) "by default, this node and its peers")))

(deftest abcast-casts-the-name-on-each-node
  (let [[a b] (two-nodes)
        sa (node/with-node a (gs/start (->Named) {:name :svc}))
        sb (node/with-node b (gs/start (->Named) {:name :svc}))]
    (is (= :abcast (node/with-node a (gs/abcast [a b] :svc :hello))))
    (is (eventually #(= :hello (node/with-node a (sys/get-state sa)))))
    (is (eventually #(= :hello (node/with-node b (sys/get-state sb)))))))

;; --- the handshake, framing and creation ---------------------------------------

(defn- fresh-node [& [opts transport]]
  (let [n (keyword (str (gensym "n") ".vm"))]
    (node/start! n (or transport (node/loopback)) (merge {:spawn #{'ensemble.node-test}} opts))
    n))

(deftest a-peer-with-the-wrong-cookie-cannot-connect
  (let [a (fresh-node {:cookie "one"})
        b (fresh-node {:cookie "two"})]
    (is (false? (node/with-node a (node/connect! b))))
    (is (= [] (node/with-node a (node/connected))))
    (is (= [] (node/with-node b (node/connected))))
    (is (= :noconnection
           (try (node/with-node a (node/spawn-on b `idle [])) (catch Throwable e (:reason (ex-data e)))))
        "an unauthenticated peer cannot spawn")))

(deftest the-same-cookie-connects
  (let [a (fresh-node {:cookie "shared"})
        b (fresh-node {:cookie "shared"})]
    (is (true? (node/with-node a (node/connect! b))))
    (is (= [b] (node/with-node a (node/connected))))
    (is (eventually #(= [a] (node/with-node b (node/connected)))))))

(deftest frames-survive-a-stream-split-anywhere
  (let [a (fresh-node {} (node/loopback {:chunk 3}))
        b (fresh-node {} (node/loopback {:chunk 3}))
        e (node/with-node a (node/spawn-on b `echo []))]
    (is (= [:echo (apply str (repeat 500 "x"))] (ask a e (apply str (repeat 500 "x")))))
    (is (= [:echo {:k [1 2 {:z #{3}}]}] (ask a e {:k [1 2 {:z #{3}}]})))))

(deftest a-pid-of-an-earlier-run-names-no-process
  (let [[a b] (two-nodes)
        e (node/with-node a (node/spawn-on b `echo []))
        stale (node/->RemotePid b (:id e) (inc (node/creation b)))]
    (is (= :timeout (ask a stale :hi)) "the id is in use, but not by that run")
    (is (= [:echo :hi] (ask a e :hi)))))

(deftest two-nodes-connecting-at-once-share-one-connection
  (dotimes [_ 5]
    (let [a (fresh-node)
          b (fresh-node)
          ra (future (node/with-node a (node/connect! b)))
          rb (future (node/with-node b (node/connect! a)))]
      (is (true? @ra))
      (is (true? @rb))
      (is (= [b] (node/with-node a (node/connected))))
      (is (= [a] (node/with-node b (node/connected)))))))

(deftest a-stopped-node-goes-down-for-its-peers
  (let [[a b] (two-nodes)
        got (promise)]
    (node/with-node a
      (act/spawn (fn [] (node/monitor-node! b)
                   (deliver got (receive [m m] [:after 2000 :timeout])))))
    (sleep 20)
    (node/stop! b)
    (is (= [:nodedown b] (deref got 3000 nil)))
    (is (false? (node/with-node a (node/connect! b))))))

(deftest refs-carry-their-nodes-creation
  (let [a (fresh-node)]
    (is (= (node/creation a) (:ensemble.actor/creation (node/with-node a (act/make-ref)))))))

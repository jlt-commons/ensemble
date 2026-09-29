(ns ensemble.node-test
  "Processes on two nodes in one VM, over the loopback transport: every
  operation between them crosses as a frame, printed and read back."
  (:require [clojure.core.async :as a]
            [clojure.test :refer [deftest is testing]]
            [ensemble.actor :as act :refer [receive]]
            [ensemble.gen-server :as gs]
            [ensemble.node :as node]))

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
    (node/start! a (node/loopback))
    (node/start! b (node/loopback))
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
                   (act/link! (node/->RemotePid b 999999))
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

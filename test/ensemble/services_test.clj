(ns ensemble.services-test
  "The distribution services, over loopback nodes: monitor-nodes, rpc,
  global and pg."
  (:require [clojure.core.async :as a]
            [clojure.test :refer [deftest is testing]]
            [ensemble.actor :as act :refer [receive]]
            [ensemble.gen-server :as gs]
            [ensemble.global :as global]
            [ensemble.node :as node]
            [ensemble.pg :as pg]
            [ensemble.process :as proc]
            [ensemble.rpc :as rpc]
            [ensemble.sys :as sys]))

(defn- sleep [ms] (a/<!! (a/timeout ms)))

(defn- id
  "A pid as [node id]: nodes sharing one VM see one process as a local actor
  on its own node and a RemotePid on the others, which are not ="
  [p]
  (when p [(proc/-node p) (proc/-pid p)]))

(defn- ids [ps] (set (map id ps)))

(defn- eventually [pred]
  (loop [i 0] (cond (pred) true (> i 300) false :else (do (sleep 10) (recur (inc i))))))

(defn- fresh-node []
  (let [n (keyword (str (gensym "s") ".vm"))]
    (node/start! n (node/loopback) {:spawn #{'ensemble.services-test}})
    n))

(defn- nodes
  "k fresh nodes, all connected."
  [k]
  (let [ns (vec (repeatedly k fresh-node))]
    (doseq [x ns, y ns :when (not= x y)] (node/with-node x (node/connect! y)))
    ns))

(defn- on
  "Run f in a fresh actor of node n and return what it returns."
  [n f]
  (let [p (promise)]
    (node/with-node n (act/spawn (fn [] (deliver p (try [:ok (f)] (catch Throwable e [:err e]))))))
    (let [[k v] (deref p 5000 [:err (ex-info "no answer" {})])] (if (= :ok k) v (throw v)))))

(defn idle [] (receive [:stop :ok]))
(defn add [a b] (+ a b))
(defn boom [] (throw (ex-info "rpc boom" {:x 1})))
(defn slow [ms] (sleep ms) :slow)
(defn where [] (act/node))
(defn tell [nm msg] (act/! nm msg))

;; --- monitor-nodes ---------------------------------------------------------------

(deftest monitor-nodes-sees-each-node-come-and-go
  (let [a (fresh-node) b (fresh-node)
        got (atom [])
        w (node/with-node a (act/spawn (fn [] (node/monitor-nodes! true)
                                         (loop [] (swap! got conj (receive [m m])) (recur)))))]
    (sleep 20)
    (node/with-node a (node/connect! b))
    (is (eventually #(= [[:nodeup b]] @got)))
    (node/with-node a (node/disconnect! b))
    (is (eventually #(= [[:nodeup b] [:nodedown b]] @got)) (pr-str @got))
    (act/exit! w :kill)))

(deftest act-nodes-lists-the-connected-nodes
  (let [[a b c] (nodes 3)]
    (is (= #{b c} (set (node/with-node a (act/nodes)))))))

;; --- is_process_alive on a remote pid -----------------------------------------------

(deftest alive-on-a-remote-pid-is-a-badarg
  (let [[a b] (nodes 2)
        p (node/with-node a (node/spawn-on b `idle []))]
    (is (= :badarg (try (act/alive? p) (catch Throwable e (:reason (ex-data e))))))))

;; --- rpc ----------------------------------------------------------------------

(deftest rpc-call-runs-a-fn-on-another-node
  (let [[a b] (nodes 2)]
    (is (= 5 (node/with-node a (rpc/call b `add [2 3]))))
    (is (= 5 (on a #(rpc/call b `add [2 3]))) "from an actor too")
    (is (= b (node/with-node a (rpc/call b `where []))) "it ran there")))

(deftest rpc-call-rethrows-what-the-fn-threw
  (let [[a b] (nodes 2)
        r (try (node/with-node a (rpc/call b `boom [])) (catch Throwable e (ex-data e)))]
    (is (= :exception (first (:reason r))))
    (is (= "rpc boom" (ex-message (get-in r [:reason 1]))) "the throwable crosses as an ex-info")))

(deftest rpc-call-times-out
  (let [[a b] (nodes 2)]
    (is (= :timeout (try (node/with-node a (rpc/call b `slow [500] 50))
                         (catch Throwable e (:reason (ex-data e))))))))

(deftest rpc-call-to-a-node-out-of-reach-is-noconnection
  (let [a (fresh-node)]
    (is (= :noconnection (try (node/with-node a (rpc/call :nowhere.vm `add [1 2]))
                              (catch Throwable e (:reason (ex-data e))))))))

(deftest rpc-runs-only-what-the-node-allows
  (let [[a b] (nodes 2)]
    (is (= [:not-allowed `slurp]
           (try (node/with-node a (rpc/call b `slurp ["/etc/passwd"]))
                (catch Throwable e (:reason (ex-data e))))))))

(deftest rpc-multicall-asks-every-node
  (let [[a b c] (nodes 3)]
    (is (= [[:ok 3] [:ok 3] [:error :noconnection]]
           (node/with-node a (rpc/multicall [b c :nowhere.vm] `add [1 2]))))))

(deftest rpc-cast-runs-without-an-answer
  (let [[a b] (nodes 2)
        box (node/with-node b (act/spawn (fn [] (receive [m m])) {:name :box}))]
    (is (= true (node/with-node a (rpc/cast b `tell [:box :hi]))))
    (is (= :hi (act/join box 2000)))))

;; --- global ---------------------------------------------------------------------

(deftest a-global-name-is-seen-on-every-node
  (let [[a b c] (nodes 3)
        p (node/with-node a (act/spawn idle))]
    (is (= :yes (node/with-node a (global/register-name :svc p))))
    (is (eventually #(= (id p) (id (node/with-node c (global/whereis-name :svc))))))
    (is (= :no (node/with-node b (global/register-name :svc (node/with-node b (act/spawn idle)))))
        "a name taken anywhere is taken")
    (is (some #{:svc} (node/with-node b (global/registered-names))))
    (node/with-node a (global/unregister-name :svc))
    (is (eventually #(nil? (node/with-node c (global/whereis-name :svc)))))))

(deftest a-global-name-goes-with-its-process
  (let [[a b] (nodes 2)
        p (node/with-node a (act/spawn idle))]
    (node/with-node a (global/register-name :temp p))
    (is (eventually #(= (id p) (id (node/with-node b (global/whereis-name :temp))))))
    (act/! p :stop)
    (is (eventually #(nil? (node/with-node b (global/whereis-name :temp)))))))

(deftest a-global-name-goes-with-its-node
  (let [[a b] (nodes 2)
        p (node/with-node a (act/spawn idle))]
    (node/with-node a (global/register-name :far p))
    (is (eventually #(= (id p) (id (node/with-node b (global/whereis-name :far))))))
    (node/with-node b (node/disconnect! a))
    (is (eventually #(nil? (node/with-node b (global/whereis-name :far)))))))

(deftest global-names-merge-when-nodes-connect
  (let [a (fresh-node) b (fresh-node)
        pa (node/with-node a (act/spawn idle))
        pb (node/with-node b (act/spawn idle))
        qa (node/with-node a (act/spawn idle))
        qb (node/with-node b (act/spawn idle))]
    (node/with-node a (global/register-name :only-a pa))
    (node/with-node b (global/register-name :only-b pb))
    (node/with-node a (global/register-name :both qa))
    (node/with-node b (global/register-name :both qb))
    (node/with-node a (node/connect! b))
    (is (eventually #(= (id pb) (id (node/with-node a (global/whereis-name :only-b))))))
    (is (eventually #(= (id pa) (id (node/with-node b (global/whereis-name :only-a))))))
    (testing "a clash keeps one of the two, the same on both nodes, and the other process is killed"
      (is (eventually #(let [x (id (node/with-node a (global/whereis-name :both)))
                             y (id (node/with-node b (global/whereis-name :both)))]
                         (and x (= x y)))))
      (let [kept (id (node/with-node a (global/whereis-name :both)))
            lost (if (= kept (id qa)) qb qa)]
        (is (= :killed (act/exit-reason lost 2000)))))))

(defrecord Echo []
  gs/Server
  (init [_] [:ok nil])
  (handle-call [_ req _ st] [:reply [(act/node) req] st])
  (handle-cast [_ _ st] [:noreply st])
  (handle-info [_ _ st] [:noreply st])
  (handle-timeout [_ st] [:noreply st])
  (handle-continue [_ _ st] [:noreply st])
  (terminate [_ _ _] nil))

(deftest a-gen-server-with-a-global-name
  (let [[a b] (nodes 2)]
    (node/with-node a (gs/start (->Echo) {:name [:global :echo]}))
    (is (eventually #(node/with-node b (global/whereis-name :echo))))
    (is (= [a :hi] (node/with-node b (gs/call! [:global :echo] :hi))))
    (let [r (try (node/with-node b (gs/start (->Echo) {:name [:global :echo]}))
                 (catch Throwable e (:reason (ex-data e))))]
      (is (= :already-started (first r)))
      (is (= (id (node/with-node a (global/whereis-name :echo))) (id (second r)))))
    (node/with-node b (global/send :echo [:not-a-call]))))

(deftest concurrent-global-registrations-pick-one
  (let [[a b c] (nodes 3)
        ps (mapv #(node/with-node % (act/spawn idle)) [a b c])
        rs (mapv (fn [n p] (future (node/with-node n (global/register-name :race p)))) [a b c] ps)
        answers (mapv deref rs)]
    (is (= 1 (count (filter #{:yes} answers))) (pr-str answers))
    (let [winner (nth ps (.indexOf answers :yes))]
      (is (eventually #(every? (fn [n] (= (id winner) (id (node/with-node n (global/whereis-name :race))))) [a b c]))))))

;; --- pg -------------------------------------------------------------------------

(deftest pg-groups-span-nodes
  (let [[a b] (nodes 2)
        pa (node/with-node a (act/spawn idle))
        pb (node/with-node b (act/spawn idle))]
    (node/with-node a (pg/join :workers pa))
    (node/with-node b (pg/join :workers pb))
    (is (eventually #(= (ids [pa pb]) (ids (node/with-node a (pg/get-members :workers))))))
    (is (eventually #(= (ids [pa pb]) (ids (node/with-node b (pg/get-members :workers))))))
    (is (= [pa] (node/with-node a (pg/get-local-members :workers))))
    (is (some #{:workers} (node/with-node b (pg/which-groups))))
    (node/with-node a (pg/leave :workers pa))
    (is (eventually #(= (ids [pb]) (ids (node/with-node b (pg/get-members :workers))))))))

(deftest a-process-leaves-its-groups-when-it-exits
  (let [[a b] (nodes 2)
        p (node/with-node a (act/spawn idle))]
    (node/with-node a (pg/join :g p))
    (is (eventually #(= (ids [p]) (ids (node/with-node b (pg/get-members :g))))))
    (act/! p :stop)
    (is (eventually #(empty? (node/with-node b (pg/get-members :g)))))))

(deftest a-node-s-members-go-with-the-node
  (let [[a b] (nodes 2)
        p (node/with-node a (act/spawn idle))]
    (node/with-node a (pg/join :g2 p))
    (is (eventually #(= (ids [p]) (ids (node/with-node b (pg/get-members :g2))))))
    (node/with-node b (node/disconnect! a))
    (is (eventually #(empty? (node/with-node b (pg/get-members :g2)))))))

(deftest members-already-joined-reach-a-node-that-connects-later
  (let [a (fresh-node) b (fresh-node)
        p (node/with-node a (act/spawn idle))]
    (node/with-node a (pg/join :late p))
    (node/with-node a (node/connect! b))
    (is (eventually #(= (ids [p]) (ids (node/with-node b (pg/get-members :late))))))))

(deftest a-pg-monitor-sees-joins-and-leaves
  (let [[a b] (nodes 2)
        p (node/with-node b (act/spawn idle))
        got (promise)]
    (node/with-node a
      (act/spawn (fn []
                   (let [[ref members] (pg/monitor :watched)]
                     (deliver got [members
                                   (receive [[ref :join :watched ps] [:join (mapv id ps)]] [:after 2000 :none])
                                   (receive [[ref :leave :watched ps] [:leave (mapv id ps)]] [:after 2000 :none])])))))
    (sleep 30)
    (node/with-node b (pg/join :watched p))
    (sleep 30)
    (node/with-node b (pg/leave :watched p))
    (is (= [[] [:join [(id p)]] [:leave [(id p)]]] (deref got 3000 nil)))))

(deftest a-pid-joins-only-from-its-own-node
  (let [[a b] (nodes 2)
        p (node/with-node b (act/spawn idle))]
    (is (= :badarg (try (node/with-node a (pg/join :g3 p)) (catch Throwable e (:reason (ex-data e))))))))

(deftest a-pg-demonitor-drops-the-scopes-monitor-of-the-watcher
  (let [a (fresh-node)
        mons #(node/with-node a (count (act/process-info (act/whereis :ensemble.pg/scope) :monitors)))
        before (mons)
        r (promise)]
    (node/with-node a
      (act/spawn (fn []
                   (let [[ref _] (pg/monitor :dm)
                         during (mons)]
                     (pg/demonitor :dm ref)
                     (deliver r [during (mons)]))
                   (receive [:never nil]))))
    (is (= [(inc before) before] (deref r 2000 nil)))))

(deftest a-service-catching-up-on-nodeup-does-not-reconnect-a-dropped-node
  ;; the services hear [:nodeup b] only after the test has dropped b, as a
  ;; slow scheduler may arrange: their sync must not bring b back
  (let [a (fresh-node) b (fresh-node)
        services (fn [n] (node/with-node n (into [] (keep act/whereis) [:ensemble.pg/scope :ensemble.global/registrar])))
        held (concat (services a) (services b))
        got (atom [])
        w (node/with-node a (act/spawn (fn [] (node/monitor-nodes! true)
                                         (loop [] (swap! got conj (receive [m m])) (recur)))))]
    (is (= 4 (count held)) "pg and global run on both nodes")
    (doseq [s held] (sys/suspend! s))
    (sleep 20)
    (node/with-node a (node/connect! b))
    (is (eventually #(= [[:nodeup b]] @got)))
    (node/with-node a (node/disconnect! b))
    (is (eventually #(= [[:nodeup b] [:nodedown b]] @got)))
    (doseq [s held] (sys/resume! s))
    (sleep 200)
    (is (= [[:nodeup b] [:nodedown b]] @got) "no sync reconnected b")
    (is (= [] (node/with-node a (node/connected))))
    (act/exit! w :kill)))

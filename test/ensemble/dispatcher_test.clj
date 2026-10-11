(ns ensemble.dispatcher-test
  (:require [clojure.core.async :as a]
            [clojure.test :refer [deftest is testing]]
            [ensemble.actor :as act :refer [receive]]
            [ensemble.dispatcher :as disp]
            [ensemble.gen-server :as gs]
            [ensemble.gen-statem :as gsm]
            [jolt.ffi :as ffi]
            [jolt.fibers :as fib]))

(defn- sleep [ms] (a/<!! (a/timeout ms)))

(defn- eventually [pred]
  (loop [i 0] (cond (pred) true (> i 200) false :else (do (sleep 10) (recur (inc i))))))

(defn- pool-here [] (fib/pool-of (fib/current-fiber)))

(defn- reason-of-throw [f]
  (try (f) nil (catch Exception e (:reason (ex-data e)))))

(deftest an-actor-runs-on-the-default-dispatcher-unless-told
  (let [p (promise)
        a (act/spawn (fn [] (deliver p (pool-here)) (receive [:stop nil])))]
    (is (nil? (deref p 1000 :none)) "the default pool")
    (is (= :default (act/process-info a :dispatcher)))
    (is (= :default (:dispatcher (act/process-info a))))
    (act/! a :stop)))

(deftest a-defined-dispatcher-runs-its-actors-on-its-own-pool
  (disp/define! ::db {:size 2})
  (try
    (let [p (promise)
          a (act/spawn (fn [] (deliver p (pool-here)) (receive [:stop nil])) {:dispatcher ::db})]
      (is (identical? (disp/pool ::db) (deref p 1000 :none)))
      (is (= 2 (fib/pool-size (disp/pool ::db))))
      (is (= ::db (act/process-info a :dispatcher)))
      (act/! a :stop)
      (is (= :normal (act/exit-reason a))))
    (finally (disp/shutdown! ::db))))

(deftest the-blocking-dispatcher-is-there-from-the-start
  (is (contains? (set (disp/dispatchers)) :blocking))
  (is (contains? (set (disp/dispatchers)) :default))
  (let [p (promise)]
    (act/spawn (fn [] (deliver p (pool-here))) {:dispatcher :blocking})
    (is (identical? (disp/pool :blocking) (deref p 1000 :none)))
    (is (some? (disp/pool :blocking)))
    (is (nil? (disp/pool :default)) "the default dispatcher is jolt's default pool")))

(deftest an-unknown-dispatcher-is-a-badarg-and-spawns-nothing
  (let [before (count (act/processes))]
    (is (= :badarg (reason-of-throw #(act/spawn (fn [] nil) {:dispatcher ::nope :name ::never}))))
    (is (nil? (act/whereis ::never)) "the name it asked for is not taken")
    (is (= before (count (act/processes))))))

(deftest defining-a-dispatcher-checks-it
  (is (= :badarg (reason-of-throw #(disp/define! ::bad {:size 0}))))
  (is (= :badarg (reason-of-throw #(disp/define! "not-a-keyword" {:size 1}))))
  (is (= :badarg (reason-of-throw #(disp/define! :default {:size 1}))) "the default is jolt's")
  (testing "a dispatcher may be redefined until its pool starts, and not after"
    (disp/define! ::later {:size 1})
    (disp/define! ::later {:size 3})
    (try
      (act/join (act/spawn (fn [] nil) {:dispatcher ::later}) 1000)
      (is (= 3 (fib/pool-size (disp/pool ::later))))
      (is (= :badarg (reason-of-throw #(disp/define! ::later {:size 4}))))
      (finally (disp/shutdown! ::later)))))

(deftest a-shut-down-dispatcher-takes-no-new-actors-but-keeps-its-running-ones
  (disp/define! ::brief {:size 1})
  (let [a (act/spawn (fn [] (receive [[:ping from] (act/! from :pong)])) {:dispatcher ::brief})
        me (promise)]
    (disp/shutdown! ::brief)
    (is (not (contains? (set (disp/dispatchers)) ::brief)))
    (is (= :badarg (reason-of-throw #(act/spawn (fn [] nil) {:dispatcher ::brief}))))
    (act/spawn (fn [] (act/! a [:ping (act/self)]) (deliver me (receive [:pong :pong] [:after 1000 :none]))))
    (is (= :pong (deref me 2000 :no)) "an actor already on it runs to the end")))

(deftest a-child-does-not-inherit-its-parents-dispatcher
  (disp/define! ::parent {:size 1})
  (try
    (let [p (promise)]
      (act/spawn (fn [] (act/spawn (fn [] (deliver p (pool-here))))) {:dispatcher ::parent})
      (is (nil? (deref p 1000 :none)) "it runs on the default unless it names one"))
    (finally (disp/shutdown! ::parent))))

(deftest a-hibernating-actor-wakes-on-its-own-dispatcher
  (disp/define! ::sleepy {:size 1})
  (try
    (let [where (atom [])
          step (fn step []
                 (swap! where conj (pool-here))
                 (receive [:stop (act/exit! :stopped)] [_ nil])
                 (act/hibernate! step))
          a (act/spawn #(act/hibernate! step) {:dispatcher ::sleepy})]
      (is (eventually #(act/hibernating? a)))
      (act/! a :wake)
      (is (eventually #(and (= 1 (count @where)) (act/hibernating? a))))
      (act/! a :stop)
      (is (= :stopped (act/exit-reason a 1000)))
      (is (= 2 (count @where)))
      (is (every? #(identical? (disp/pool ::sleepy) %) @where)))
    (finally (disp/shutdown! ::sleepy))))

(deftest a-hibernating-actor-whose-dispatcher-went-wakes-on-the-default
  (disp/define! ::gone {:size 1})
  (let [p (promise)
        a (act/spawn #(act/hibernate! (fn [] (deliver p (pool-here)) (receive [:done nil])))
                     {:dispatcher ::gone})]
    (is (eventually #(act/hibernating? a)))
    (disp/shutdown! ::gone)
    (act/! a :wake)
    (is (nil? (deref p 1000 :none)))
    (is (= :default (act/process-info a :dispatcher)))
    (act/! a :done)
    (is (= :normal (act/exit-reason a 1000)))))

(deftest signals-and-links-cross-dispatchers
  (disp/define! ::far {:size 1})
  (try
    (testing "a kill reaches an actor parked on another dispatcher"
      (let [a (act/spawn (fn [] (receive [:never nil])) {:dispatcher ::far})]
        (sleep 20)
        (act/exit! a :kill)
        (is (= :killed (act/exit-reason a)))))
    (testing "a link from the default dispatcher sees it die"
      (let [got (promise)]
        (act/spawn (fn []
                     (act/trap-exit!)
                     (let [b (act/spawn-link (fn [] (act/exit! :boom)) {:dispatcher ::far})]
                       (deliver got (receive [[:EXIT b r] r] [:after 1000 :none])))))
        (is (= :boom (deref got 2000 :no)))))
    (finally (disp/shutdown! ::far))))

(defrecord Echo []
  gs/Server
  (init [_] [:ok nil])
  (handle-call [_ msg _ st] [:reply [msg (pool-here)] st])
  (handle-cast [_ _ st] [:noreply st])
  (handle-info [_ _ st] [:noreply st])
  (handle-timeout [_ st] [:noreply st])
  (handle-continue [_ _ st] [:noreply st])
  (terminate [_ _ _] nil))

(defrecord Where [seen]
  gsm/Machine
  (init [_] (deliver seen (pool-here)) [:ok :idle nil])
  (handle-event [_ _ _ _ _] [:keep-state-and-data])
  (terminate [_ _ _ _] nil))

(deftest gen-server-and-gen-statem-take-a-dispatcher
  (disp/define! ::svc {:size 1})
  (try
    (let [srv (gs/start (->Echo) {:dispatcher ::svc})
          [v pool] (gs/call! srv :hi)]
      (is (= :hi v))
      (is (identical? (disp/pool ::svc) pool))
      (is (= ::svc (act/process-info srv :dispatcher)))
      (gs/stop! srv))
    (let [seen (promise)
          m (gsm/start (->Where seen) {:dispatcher ::svc})]
      (is (identical? (disp/pool ::svc) (deref seen 1000 :none)))
      (is (= ::svc (act/process-info m :dispatcher)))
      (act/exit! m :kill))
    (finally (disp/shutdown! ::svc))))

;; What a dispatcher is for: an actor stuck in a blocking foreign call pins
;; its carrier, and every actor queued on that carrier with it.  On a
;; dispatcher of its own it holds up only that dispatcher's actors.
(when-not (re-find #"(?i)windows" (str (System/getProperty "os.name")))
  (ffi/defcfn c-usleep "usleep" [:int] :int)

  (deftest a-blocked-carrier-holds-up-only-its-own-dispatcher
    (disp/define! ::io {:size 1})
    (disp/define! ::cpu {:size 1})
    (try
      (let [order (atom [])
            started (promise)
            blocker (act/spawn (fn [] (deliver started true) (c-usleep 300000) (swap! order conj :blocker-done))
                               {:dispatcher ::io})
            _ (deref started 1000 nil)
            same (act/spawn (fn [] (swap! order conj :same-dispatcher)) {:dispatcher ::io})
            apart (act/spawn (fn [] (swap! order conj :other-dispatcher)) {:dispatcher ::cpu})]
        (doseq [a [blocker same apart]] (act/exit-reason a))
        (is (= [:other-dispatcher :blocker-done :same-dispatcher] @order)))
      (finally (disp/shutdown! ::io) (disp/shutdown! ::cpu)))))

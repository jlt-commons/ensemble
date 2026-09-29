(ns ensemble.actor-test
  (:require [clojure.core.async :as a]
            [clojure.test :refer [deftest is testing]]
            [ensemble.actor :as act :refer [receive]]
            [jolt.fibers :as fib]))

(defn- sleep [ms] (a/<!! (a/timeout ms)))

(deftest send-and-receive
  (let [a (act/spawn (fn [] (receive [[:hello x] x])))]
    (act/! a [:hello 42])
    (is (= 42 (act/join a 1000)))))

(deftest send-returns-the-message
  (let [a (act/spawn (fn [] (receive [_ :got])))]
    (is (= :anything (act/! a :anything)))
    (is (= :got (act/join a 1000)))))

(deftest selective-receive-leaves-skipped-messages
  (let [a (act/spawn (fn [] [(receive [[:b x] x]) (receive [[:a y] y])]))]
    (act/! a [:a 1])
    (act/! a [:b 2])
    (is (= [2 1] (act/join a 1000)))))

(deftest skipped-messages-stay-ahead-of-later-ones
  (testing "messages a receive stepped over come before ones sent after it"
    (let [a (act/spawn (fn []
                         (let [c (receive [[:c x] x])]
                           (receive [:more nil])
                           [c (receive [m m]) (receive [m m]) (receive [m m])])))]
      (doseq [m [[:a 1] [:b 2] [:c 3]]] (act/! a m))
      (sleep 20)
      (act/! a [:d 4])
      (act/! a :more)
      (is (= [3 [:a 1] [:b 2] [:d 4]] (act/join a 1000))))))

(deftest a-long-mailbox-drains-in-order
  (let [n 20000
        a (act/spawn (fn [] (receive [:go nil])
                       (loop [i 0 ok true]
                         (if (= i n) ok (recur (inc i) (and ok (= i (receive [m m]))))))))]
    (dotimes [i n] (act/! a i))
    (act/! a :go)
    (is (true? (act/join a 10000)))))

(deftest messages-from-one-sender-arrive-in-order
  (let [a (act/spawn (fn [] (vec (for [_ (range 100)] (receive [[:n i] i])))))]
    (doseq [i (range 100)] (act/! a [:n i]))
    (is (= (vec (range 100)) (act/join a 1000)))))

(deftest request-and-reply-through-self
  (let [server (act/spawn (fn [] (receive [[from m] (act/! from [:reply (str m "!")])])))
        client (act/spawn (fn [] (act/! server [(act/self) "hi"]) (receive [[:reply r] r])))]
    (is (= "hi!" (act/join client 1000)))))

(deftest else-takes-the-next-message
  (let [a (act/spawn (fn [] (receive [[:stop] :stopped] [:else :other])))]
    (act/! a [:whatever 1])
    (is (= :other (act/join a 1000)))))

(deftest after-times-out
  (let [a (act/spawn (fn [] (receive [[:msg x] x] [:after 30 :timed-out])))]
    (is (= :timed-out (act/join a 1000)))))

(deftest after-zero-polls
  (let [a (act/spawn (fn [] [(receive [_ :got] [:after 0 :empty])
                             (do (act/! (act/self) :x)
                                 (receive [_ :got] [:after 0 :empty]))]))]
    (is (= [:empty :got] (act/join a 1000)))))

(deftest a-non-matching-message-does-not-reset-the-timeout
  (let [a (act/spawn (fn [] (receive [[:want x] x] [:after 100 :timed-out])))]
    (dotimes [_ 5] (sleep 10) (act/! a :noise))
    (is (= :timed-out (act/join a 1000)))))

(deftest a-late-match-wins-over-the-timeout
  (let [a (act/spawn (fn [] (receive [[:msg x] x] [:after 2000 :timed-out])))]
    (sleep 20)
    (act/! a [:msg :arrived])
    (is (= :arrived (act/join a 1000)))))

(deftest a-guard-leaves-a-message-for-a-later-clause
  (let [a (act/spawn (fn [] (vec (for [_ (range 3)]
                                   (receive [[:n x] :when (even? x) [:even x]]
                                            [[:n x] [:odd x]])))))]
    (doseq [i [1 2 3]] (act/! a [:n i]))
    (is (= [[:odd 1] [:even 2] [:odd 3]] (act/join a 1000)))))

(deftest a-guard-can-skip-a-message-entirely
  (let [a (act/spawn (fn [] (receive [[:n x] :when (> x 10) x])))]
    (act/! a [:n 1])
    (act/! a [:n 20])
    (is (= 20 (act/join a 1000)))))

(deftest a-throwing-guard-is-false
  (let [a (act/spawn (fn [] (receive [x :when (pos? x) :pos] [_ :other])))]
    (act/! a :not-a-number)
    (is (= :other (act/join a 1000)))))

(deftest a-bound-local-matches-its-value
  (let [a (act/spawn (fn []
                       (let [ref (act/make-ref)]
                         (act/! (act/self) [:reply (act/make-ref) :wrong])
                         (act/! (act/self) [:reply ref :right])
                         (receive [[:reply ref v] v]))))]
    (is (= :right (act/join a 1000)))))

(deftest a-repeated-name-must-match-equal-values
  (let [a (act/spawn (fn [] (receive [[x x] [:same x]] [_ :different])))]
    (act/! a [1 2])
    (is (= :different (act/join a 1000))))
  (let [a (act/spawn (fn [] (receive [[x x] [:same x]] [_ :different])))]
    (act/! a [3 3])
    (is (= [:same 3] (act/join a 1000)))))

(deftest receive-outside-an-actor-throws
  (is (thrown? Throwable (receive [_ :x] [:after 0 :none]))))

(deftest join-rethrows-and-exit-reason-reports
  (let [e (ex-info "boom" {})
        a (act/spawn (fn [] (throw e)))]
    (is (thrown? Throwable (act/join a 1000)))
    (is (= e (act/exit-reason a)))))

(deftest join-times-out
  (let [a (act/spawn (fn [] (receive [_ :never])))]
    (is (thrown? Throwable (act/join a 30)))))

(deftest exit-normal-is-clean
  (let [a (act/spawn (fn [] (act/exit! :normal)))]
    (is (nil? (act/join a 1000)))
    (is (= :normal (act/exit-reason a)))
    (is (false? (act/alive? a)))))

(deftest exit-is-catchable-like-erlang-exit-1
  (let [a (act/spawn (fn [] (try (act/exit! :boom) (catch Throwable _ :caught))))]
    (is (= :caught (act/join a 1000)))))

(deftest actors-print-by-pid
  (let [a (act/spawn (fn [] (receive [_ :ok])))]
    (is (re-matches #"#<actor \d+>" (pr-str a)))
    (act/! a :go)))

;; registry ---------------------------------------------------------------

(deftest register-and-send-by-name
  (let [a (act/spawn (fn [] (receive [m m])) {:name :svc-1})]
    (is (= a (act/whereis :svc-1)))
    (act/! :svc-1 :hi)
    (is (= :hi (act/join a 1000)))))

(deftest names-normalise
  (let [a (act/spawn (fn [] (receive [_ :ok])) {:name "svc-2"})]
    (is (= a (act/whereis :svc-2)))
    (is (= a (act/whereis 'svc-2)))
    (act/! a :go)))

(deftest a-taken-name-cannot-be-registered-again
  (let [a (act/spawn (fn [] (receive [_ :ok])) {:name :svc-3})
        b (act/spawn (fn [] (receive [_ :ok])))]
    (is (thrown? Throwable (act/register! :svc-3 b)))
    (is (thrown? Throwable (act/register! :svc-3b a)))
    (act/! a :go) (act/! b :go)))

(deftest a-name-is-released-when-its-actor-exits
  (let [a (act/spawn (fn [] :done) {:name :svc-4})]
    (act/join a 1000)
    (is (nil? (act/whereis :svc-4)))
    (let [b (act/spawn (fn [] (receive [_ :ok])) {:name :svc-4})]
      (is (= b (act/whereis :svc-4)))
      (act/! b :go))))

(deftest sending-to-an-unknown-name-throws
  (is (thrown? Throwable (act/! :nobody-here [:x]))))

(deftest sending-to-a-dead-actor-is-dropped
  (let [a (act/spawn (fn [] :done))]
    (act/join a 1000)
    (is (= :msg (act/! a :msg)))))

;; monitors ---------------------------------------------------------------

(deftest monitor-delivers-down-with-the-reason
  (let [target (act/spawn (fn [] (receive [_ (act/exit! :boom)])))
        watcher (act/spawn (fn []
                             (let [ref (act/monitor! target)]
                               (act/! target :go)
                               (receive [[:DOWN ref :process who reason] [(= who target) reason]]))))]
    (is (= [true :boom] (act/join watcher 1000)))))

(deftest monitor-reports-a-normal-exit
  (let [target (act/spawn (fn [] (receive [_ :ok])))
        watcher (act/spawn (fn []
                             (let [ref (act/monitor! target)]
                               (act/! target :go)
                               (receive [[:DOWN ref :process _ reason] reason]))))]
    (is (= :normal (act/join watcher 1000)))))

(deftest monitoring-a-dead-actor-is-noproc
  (let [target (act/spawn (fn [] :done))
        _ (act/join target 1000)
        watcher (act/spawn (fn []
                             (let [ref (act/monitor! target)]
                               (receive [[:DOWN ref :process _ reason] reason]))))]
    (is (= :noproc (act/join watcher 1000)))))

(deftest demonitor-stops-the-down
  (let [target (act/spawn (fn [] (receive [_ :ok])))
        watcher (act/spawn (fn []
                             (let [ref (act/monitor! target)]
                               (act/demonitor! ref)
                               (act/! target :go)
                               (receive [[:DOWN _ _ _ _] :down] [:after 100 :quiet]))))]
    (is (= :quiet (act/join watcher 1000)))))

(deftest demonitor-flush-removes-a-delivered-down
  (let [target (act/spawn (fn [] :done))
        watcher (act/spawn (fn []
                             (let [ref (act/monitor! target)]
                               (receive [[:DOWN ref _ _ _] nil] [:after 0 nil])
                               (let [ref2 (act/monitor! target)]
                                 (sleep 20)
                                 (act/demonitor! ref2 {:flush true})
                                 (receive [[:DOWN _ _ _ _] :down] [:after 50 :flushed])))))]
    (is (= :flushed (act/join watcher 1000)))))

(deftest no-down-arrives-after-a-flushing-demonitor
  (testing "even when the target dies as the demonitor runs"
    (let [w (act/spawn
             (fn []
               (every? true?
                       (for [i (range 300)]
                         (let [t (act/spawn (fn [] (dotimes [_ (mod i 7)] (fib/yield)) :done))
                               ref (act/monitor! t)]
                           (dotimes [_ (mod i 5)] (fib/yield))
                           (act/demonitor! ref {:flush true})
                           (act/join t 1000)
                           (receive [[:DOWN ref :process _ _] false] [:after 1 true]))))))]
      (is (true? (act/join w 20000))))))

(deftest a-monitor-is-one-way
  (let [target (act/spawn (fn [] (receive [_ (act/exit! :boom)])))
        watcher (act/spawn (fn []
                             (act/monitor! target)
                             (act/! target :go)
                             (receive [[:DOWN _ _ _ _] nil])
                             :alive))]
    (is (= :alive (act/join watcher 1000)))))

(deftest spawn-monitor-sees-an-immediate-crash
  (let [watcher (act/spawn (fn []
                             (let [[_ ref] (act/spawn-monitor (fn [] (act/exit! :fast)))]
                               (receive [[:DOWN ref :process _ r] r]))))]
    (is (= :fast (act/join watcher 1000)))))

(deftest many-monitors-all-fire
  (let [target (act/spawn (fn [] (receive [_ :ok])))
        ws (doall (for [_ (range 3)]
                    (act/spawn (fn [] (act/monitor! target) (receive [[:DOWN _ _ _ _] :notified])))))]
    (sleep 20)
    (act/! target :go)
    (is (= [:notified :notified :notified] (mapv #(act/join % 1000) ws)))))

;; state ------------------------------------------------------------------

(deftest spawn-actor-folds-messages-into-state
  (let [c (act/spawn-actor (fn [st msg]
                             (case (first msg)
                               :add (+ st (second msg))
                               :get (do (act/! (second msg) [:count st]) st)
                               :stop (act/exit! :normal)))
                           0)
        reader (act/spawn (fn []
                            (act/! c [:add 3])
                            (act/! c [:add 4])
                            (act/! c [:get (act/self)])
                            (receive [[:count n] n])))]
    (is (= 7 (act/join reader 1000)))
    (is (= 7 (act/state c)))
    (act/! c [:stop])
    (is (= :normal (act/exit-reason c 1000)))))

(deftest a-crashing-handler-keeps-the-last-good-state-and-exits
  (let [c (act/spawn-actor (fn [st msg] (if (= :boom msg) (throw (ex-info "bad" {})) (conj st msg))) [])]
    (act/! c :a)
    (act/! c :boom)
    (is (instance? Throwable (act/exit-reason c 1000)))
    (is (= [:a] (act/state c)))))

(deftest spawned-actors-keep-state-in-a-cell
  (let [a (act/spawn (fn []
                       (receive [[:bump n] (act/update-state! + n)]))
                     {:state 10})]
    (act/! a [:bump 5])
    (is (= 15 (act/join a 1000)))
    (is (= 15 (act/state a)))))

(deftest an-actor-killed-before-its-first-step-still-dies
  ;; an exit signal can reach a new actor's fiber before it has run a step;
  ;; the actor must still die of it, and its monitors and links hear so
  (let [out (promise)]
    (act/spawn (fn []
                 (deliver out
                          (vec (for [_ (range 300)]
                                 (let [a (act/spawn-link (fn [] (receive [_ nil])))]
                                   (act/unlink! a)
                                   (act/exit! a :shutdown)
                                   (act/exit-reason a 1000)))))))
    (is (every? #{:shutdown} (deref out 60000 [:timeout])))))

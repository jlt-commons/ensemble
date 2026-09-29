(ns ensemble.timer-test
  (:require [clojure.core.async :as a]
            [clojure.test :refer [deftest is testing]]
            [ensemble.actor :as act :refer [receive]]
            [ensemble.timer :as timer]))

(defn- sleep [ms] (a/<!! (a/timeout ms)))

(defn- in-actor
  "Run f in a fresh actor and return its value."
  [f]
  (act/join (act/spawn f) 5000))

(deftest send-after-sends-the-message-after-the-time
  (is (= [:late :ping]
         (in-actor (fn []
                     (timer/send-after 30 (act/self) :ping)
                     [(receive [:ping :early] [:after 10 :late])
                      (receive [:ping :ping] [:after 500 :never])])))))

(deftest start-timer-wraps-the-message-with-its-ref
  (is (in-actor (fn []
                  (let [ref (timer/start-timer 10 (act/self) :tick)]
                    (receive [[:timeout ref :tick] true] [:after 500 false]))))))

(deftest cancel-answers-the-time-left-and-the-timer-never-fires
  (is (= [true false :nothing]
         (in-actor (fn []
                     (let [ref (timer/send-after 200 (act/self) :ping)
                           left (timer/cancel-timer ref)]
                       [(and (integer? left) (< 100 left 201))
                        (timer/cancel-timer ref)
                        (receive [:ping :fired] [:after 300 :nothing])]))))))

(deftest a-fired-timer-cannot-be-cancelled-or-read
  (is (= [:ping false false]
         (in-actor (fn []
                     (let [ref (timer/send-after 5 (act/self) :ping)]
                       [(receive [:ping :ping] [:after 500 :never])
                        (timer/cancel-timer ref)
                        (timer/read-timer ref)]))))))

(deftest read-timer-counts-down-without-cancelling
  (is (= [true true :ping]
         (in-actor (fn []
                     (let [ref (timer/send-after 100 (act/self) :ping)
                           a (timer/read-timer ref)
                           _ (sleep 30)
                           b (timer/read-timer ref)]
                       [(< 50 a 101) (< b a) (receive [:ping :ping] [:after 500 :never])]))))))

(deftest an-async-cancel-answers-by-message
  (is (in-actor (fn []
                  (let [ref (timer/send-after 200 (act/self) :ping)]
                    (and (= :ok (timer/cancel-timer ref {:async true}))
                         (receive [[:cancel-timer ref left] (integer? left)] [:after 500 false])))))))

(deftest timers-fire-in-deadline-order
  (is (= [:a :b :c]
         (in-actor (fn []
                     (let [me (act/self)]
                       (timer/send-after 40 me :c)
                       (timer/send-after 10 me :a)
                       (timer/send-after 25 me :b)
                       (vec (for [_ (range 3)] (receive [m m] [:after 500 :never])))))))))

(deftest a-timer-to-a-name-looks-it-up-when-it-fires
  (let [got (promise)]
    (timer/send-after 30 :late-comer :hello)
    (act/spawn (fn [] (act/register! :late-comer (act/self))
                 (deliver got (receive [m m] [:after 500 :never]))))
    (is (= :hello (deref got 1000 :no)))))

(deftest a-timer-to-a-name-nobody-holds-is-dropped
  (timer/send-after 5 :nobody-holds-this :x)
  (sleep 30)
  (testing "and the timer server carries on"
    (is (in-actor (fn [] (timer/send-after 5 (act/self) :ok) (receive [:ok true] [:after 500 false]))))))

(deftest a-timer-to-a-process-that-exits-is-cancelled
  (let [ref (promise)
        p (act/spawn (fn [] (deliver ref (timer/send-after 300 (act/self) :x)) (receive [:quit :ok])))]
    (act/! p :quit)
    (act/join p 1000)
    (sleep 30)
    (is (false? (timer/read-timer @ref)))))

(deftest an-interval-fires-until-cancelled
  (is (= [:tick :tick :tick :none]
         (in-actor (fn []
                     (let [ref (timer/send-interval 15 :tick)
                           ticks (vec (for [_ (range 3)] (receive [:tick :tick] [:after 500 :never])))]
                       (timer/cancel ref)
                       (sleep 20)
                       ;; a tick sent before the cancel may be waiting
                       (receive [:tick nil] [:after 0 nil])
                       (conj ticks (receive [:tick :tick] [:after 60 :none]))))))))

(deftest an-interval-ends-when-its-starter-exits
  (let [got (atom 0)
        sink (act/spawn (fn [] (loop [] (receive [:tick (do (swap! got inc) (recur))] [:stop :ok]))))
        starter (act/spawn (fn [] (timer/send-interval 10 sink :tick) (receive [:quit :ok])))]
    (sleep 45)
    (act/! starter :quit)
    (act/join starter 1000)
    (sleep 20)
    (let [n @got]
      (sleep 60)
      (is (pos? n))
      (is (= n @got) "no tick after the starter exited"))
    (act/! sink :stop)))

(deftest apply-after-runs-the-fn-in-a-new-process
  (let [p (promise)]
    (timer/apply-after 10 (fn [x] (deliver p [x (some? (act/self))])) :hi)
    (is (= [:hi true] (deref p 1000 :no)))))

(deftest exit-after-sends-the-exit-signal
  (let [p (act/spawn (fn [] (receive [:never :ok])))]
    (timer/exit-after 10 p :timeout-reached)
    (is (= :timeout-reached (act/exit-reason p 1000)))))

(deftest a-negative-time-is-refused
  (is (= :badarg (try (timer/send-after -1 :x :y) (catch Throwable e (:reason (ex-data e)))))))

(deftest an-absolute-deadline-is-a-monotonic-time
  (is (in-actor (fn []
                  (timer/send-after (+ (timer/monotonic-time) 20) (act/self) :abs {:abs true})
                  (receive [:abs true] [:after 500 false])))))

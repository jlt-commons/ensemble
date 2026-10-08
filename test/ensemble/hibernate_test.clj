(ns ensemble.hibernate-test
  (:require [clojure.core.async :as a]
            [clojure.test :refer [deftest is testing]]
            [ensemble.actor :as act :refer [receive]]
            [jolt.fs]))

(defn- sleep [ms] (a/<!! (a/timeout ms)))

(defn- eventually [pred]
  (loop [i 0] (cond (pred) true (> i 200) false :else (do (sleep 10) (recur (inc i))))))

(defn counter
  "Count messages, hibernating between each."
  [n]
  (receive
   [[:get from] (act/! from [:count n])]
   [:stop (act/exit! :stopped)]
   [_ nil])
  (act/hibernate! counter (inc n)))

(deftest a-hibernating-process-holds-no-fiber-and-wakes-on-a-message
  (let [p (act/spawn #(act/hibernate! counter 0))]
    (is (eventually #(act/hibernating? p)))
    (is (act/alive? p))
    (act/! p :one)
    (act/! p :two)
    (let [me (promise)]
      (act/spawn (fn [] (act/! p [:get (act/self)]) (deliver me (receive [[:count n] n] [:after 1000 :none]))))
      (is (= 2 (deref me 2000 :no)) "it ran again from the fn it named, with its count"))
    (is (eventually #(act/hibernating? p)))))

(deftest a-message-waiting-wakes-it-at-once
  (let [p (act/spawn (fn [] (act/! (act/self) :already-here) (act/hibernate! (fn [] (receive [:already-here :woke])))))]
    (is (= :woke (act/join p 1000)))))

(deftest a-hibernating-process-keeps-its-name-links-and-monitors
  (let [watcher (promise)
        linked (promise)
        p (act/spawn #(act/hibernate! counter 0) {:name :sleepy})]
    (act/spawn (fn [] (act/trap-exit!) (act/link! p) (deliver linked true)
                 (deliver watcher (receive [[:EXIT _ r] r] [:after 2000 :none]))))
    ;; linked before p stops, or the link finds it gone: :noproc
    (is (deref linked 1000 false))
    (is (eventually #(act/hibernating? p)))
    (is (= p (act/whereis :sleepy)))
    (act/! :sleepy :stop)
    (is (= :stopped (deref watcher 2000 :no)) "its link still stands: the exit reaches the linked process")))

(deftest an-exit-signal-reaches-a-hibernating-process
  (let [p (act/spawn #(act/hibernate! counter 0))]
    (is (eventually #(act/hibernating? p)))
    (act/exit! p :go-away)
    (is (= :go-away (act/exit-reason p 1000))))
  (let [p (act/spawn (fn [] (act/trap-exit!) (act/hibernate! (fn [] (receive [[:EXIT _ r] [:trapped r]])))))]
    (is (eventually #(act/hibernating? p)))
    (act/exit! p :go-away)
    (is (= [:trapped :go-away] (act/join p 1000)))))

(deftest a-kill-reaches-a-hibernating-process
  (let [p (act/spawn (fn [] (act/trap-exit!) (act/hibernate! counter 0)))]
    (is (eventually #(act/hibernating? p)))
    (act/exit! p :kill)
    (is (= :killed (act/exit-reason p 1000)))))

(deftest many-wakes-in-a-row-run-it-once-each
  (let [p (act/spawn #(act/hibernate! counter 0))
        me (promise)]
    (dotimes [_ 200] (act/! p :x))
    (act/spawn (fn [] (act/! p [:get (act/self)]) (deliver me (receive [[:count n] n] [:after 2000 :none]))))
    (is (= 200 (deref me 3000 :no)))))

;; --- passivation: hibernating to disk -----------------------------------------

(defn passive-counter
  "Count messages, passivating between each; peer is a process it answers
  through."
  [n peer]
  (receive
   [[:get from] (act/! from [:count n (act/self)])]
   [:ping (act/! peer [:pong n])]
   [:stop (act/exit! :stopped)]
   [_ nil])
  (act/passivate! passive-counter (inc n) peer))

(deftest a-passivated-process-wakes-from-disk
  (let [sink (act/spawn (fn [] (loop [] (receive [[:pong n] (recur)] [:quit :ok]))))
        p (act/spawn #(act/passivate! passive-counter 0 sink))]
    (is (eventually #(act/passivated? p)))
    (is (act/alive? p))
    (act/! p :one)
    (is (eventually #(act/passivated? p)) "it passivates again after each message")
    (let [me (promise)]
      (act/spawn (fn [] (act/! p [:get (act/self)]) (deliver me (receive [[:count n who] [n (= who p)]] [:after 2000 :none]))))
      (is (= [1 true] (deref me 3000 :no)) "its state came back from the image, and its own pid is its pid"))
    (testing "an actor it holds is the live actor again"
      (act/! p :ping)
      (is (eventually #(act/passivated? p)))
      (is (act/alive? sink)))
    (act/! p :stop)
    (is (= :stopped (act/exit-reason p 2000)))
    (act/! sink :quit)))

(deftest a-shared-reference-keeps-the-process-in-memory
  (let [shared (atom 0)
        p (act/spawn #(act/passivate! (fn [a] (receive [:bump (swap! a inc)])) shared))]
    (is (eventually #(act/hibernating? p)))
    (sleep 30)
    (is (not (act/passivated? p)) "an atom cannot go to disk apart from who shares it")
    (act/! p :bump)
    (is (= 1 (act/join p 1000)))
    (is (= 1 @shared) "and it is still the atom that is shared")))

(deftest a-signal-wakes-a-passivated-process
  (let [p (act/spawn #(act/passivate! counter 0))]
    (is (eventually #(act/passivated? p)))
    (act/exit! p :go-away)
    (is (= :go-away (act/exit-reason p 1000)))))

(deftest waking-removes-the-image
  (let [dir (str act/*image-dir*)
        before (set (map str (jolt.fs/glob dir "ensemble-*.jimg")))
        p (act/spawn #(act/passivate! counter 0))]
    (is (eventually #(act/passivated? p)))
    (is (seq (remove before (map str (jolt.fs/glob dir "ensemble-*.jimg")))) "its image is on disk")
    (act/! p :stop)
    (act/exit-reason p 1000)
    (is (empty? (remove before (map str (jolt.fs/glob dir "ensemble-*.jimg")))) "and gone once it has woken")))

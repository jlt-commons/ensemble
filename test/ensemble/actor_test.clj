(ns ensemble.actor-test
  (:require [clojure.test :refer [deftest is testing]]
            [ensemble.actor :as act]))

(deftest send-and-receive
  (let [a (act/spawn (fn [] (act/receive [[:hello x] x])))]
    (act/! a [:hello 42])
    (is (= 42 (act/join a)))))

(deftest send-returns-actor
  (let [a (act/spawn (fn [] (act/receive [_ :got])))]
    (is (= a (act/! a :anything)))
    (is (= :got (act/join a)))))

(deftest selective-retains-skipped
  (let [a (act/spawn (fn []
                       [(act/receive [[:b x] x])
                        (act/receive [[:a y] y])]))]
    (act/! a [:a 1])
    (act/! a [:b 2])
    (is (= [2 1] (act/join a)))))

(deftest else-takes-next
  (let [a (act/spawn (fn [] (act/receive [[:stop] :stopped]
                                         [:else :other])))]
    (act/! a [:whatever 1])
    (is (= :other (act/join a)))))

(deftest else-not-needed-when-match
  (let [a (act/spawn (fn [] (act/receive [[:stop] :stopped]
                                         [:else :other])))]
    (act/! a [:stop])
    (is (= :stopped (act/join a)))))

(deftest after-times-out
  (let [a (act/spawn (fn [] (act/receive [[:msg x] x]
                                         [:after 50 :timed-out])))]
    (is (= :timed-out (act/join a)))))

(deftest after-yields-to-a-late-message
  (let [a (act/spawn (fn [] (act/receive [[:msg x] x]
                                         [:after 2000 :timed-out])))]
    (act/! a [:msg :arrived])
    (is (= :arrived (act/join a)))))

(deftest empty-pattern-tuple-matches-empty-message
  (let [a (act/spawn (fn [] (act/receive [[] :empty])))]
    (act/! a [])
    (is (= :empty (act/join a)))))

(deftest state-is-per-actor
  (let [a (act/spawn (fn []
                       (act/set-state! (act/self) 10)
                       (act/receive
                        [[:bump n]
                         (do (act/set-state! (act/self)
                                             (+ (act/state (act/self)) n))
                             (act/state (act/self)))]))
                   {:state 0})]
    (act/! a [:bump 5])
    (is (= 15 (act/join a)))))

(deftest actors-are-isolated
  (let [a (act/spawn (fn [] (act/receive [[:x v] v])))
        b (act/spawn (fn [] (act/receive [[:x v] v])))]
    (act/! a [:x :a-only])
    (act/! b [:x :b-only])
    (is (= :a-only (act/join a)))
    (is (= :b-only (act/join b)))))

(deftest done?-before-and-after
  (let [a (act/spawn (fn [] (act/receive [_ :done])))]
    (is (false? (act/done? a)))
    (act/! a :go)
    (act/join a)
    (is (true? (act/done? a)))))

(deftest join-rethrows
  (let [a (act/spawn (fn [] (throw (ex-info "boom" {}))))]
    (is (thrown? Throwable (act/join a)))))

(deftest register-and-whereis
  (let [a (act/spawn (fn [] (act/receive [_ :ok])) {:name :svc})]
    (is (= a (act/whereis :svc)))
    (act/! (act/whereis :svc) :hi)
    (is (= :ok (act/join a)))))

(deftest spawn-returns-and-body-sees-actor
  (let [a (act/spawn (fn [] (act/self)))]
    (is (= a (act/join a)))))

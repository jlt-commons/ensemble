(ns ensemble.ensemble-test
  (:require [clojure.test :refer [deftest is]]
            [ensemble :as e]
            [ensemble.gen-server :as gs]))

(deftest ping-pong-through-the-umbrella
  (let [ping (e/spawn (fn [] (e/receive [[:ping from] (e/! from :pong)])))
        hearer (e/spawn (fn [] (e/! ping [:ping (e/self)]) (e/receive [:pong :pong])))]
    (is (= :pong (e/join hearer 1000)))))

(deftest readme-counter
  (let [counter (e/spawn-actor (fn [n msg]
                                 (case (first msg)
                                   :add (+ n (second msg))
                                   :get (do (e/! (second msg) n) n)))
                               0)
        reader (e/spawn (fn []
                          (e/! counter [:add 3])
                          (e/! counter [:get (e/self)])
                          (e/receive [n n])))]
    (is (= 3 (e/join reader 1000)))
    (is (= 3 (e/state counter)))))

(defrecord Echo []
  gs/Server
  (init [_] [:ok nil])
  (handle-call [_ req _ st] [:reply req st])
  (handle-cast [_ _ st] [:noreply st])
  (handle-info [_ _ st] [:noreply st])
  (handle-timeout [_ st] [:noreply st])
  (terminate [_ _ _] nil))

(deftest the-gen-protocol-through-the-umbrella
  (let [s (gs/start (->Echo))]
    (is (= :hi (e/call! s :hi)))
    (is (= :ok (e/stop! s)))))

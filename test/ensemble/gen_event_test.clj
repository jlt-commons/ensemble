(ns ensemble.gen-event-test
  (:require [clojure.test :refer [deftest is]]
            [ensemble.gen-event :as ev]))

(defrecord Sink [log]
  ev/Handler
  (h-init [_] 0)
  (h-handle-event [_ e st] (swap! log conj e) (inc st))
  (h-handle-call [_ msg st]
    (case (first msg)
      :count [:reply st st]
      :last [:reply (last @log) st]))
  (h-terminate [_ reason _st] (swap! log conj [:terminated reason])))

(deftest notify-reaches-all-handlers
  (let [l1 (atom []) l2 (atom [])
        m (ev/start-manager)]
    (ev/add-handler! m :a (->Sink l1))
    (ev/add-handler! m :b (->Sink l2))
    (ev/sync-notify! m [:tick 1])
    (ev/sync-notify! m [:tick 2])
    (is (= [[:tick 1] [:tick 2]] @l1))
    (is (= [[:tick 1] [:tick 2]] @l2))))

(deftest notify-keeps-per-handler-state
  (let [l (atom [])
        m (ev/start-manager)]
    (ev/add-handler! m :a (->Sink l))
    (ev/sync-notify! m :one)
    (ev/sync-notify! m :two)
    (is (= 2 (ev/call-handler! m :a [:count])))))

(deftest remove-handler-stops-events-and-terminates
  (let [l (atom [])
        m (ev/start-manager)]
    (ev/add-handler! m :a (->Sink l))
    (ev/sync-notify! m :one)
    (ev/remove-handler! m :a)
    (ev/sync-notify! m :two)
    (is (= [:one [:terminated :removed]] @l))))

(deftest replacing-a-handler-keeps-one-entry
  (let [l1 (atom []) l2 (atom [])
        m (ev/start-manager)]
    (ev/add-handler! m :a (->Sink l1))
    (ev/add-handler! m :a (->Sink l2))
    (ev/sync-notify! m :x)
    (is (= [:x] @l2))
    (is (= [] @l1))))

(deftest call-handler-returns-reply
  (let [l (atom [])
        m (ev/start-manager)]
    (ev/add-handler! m :a (->Sink l))
    (ev/sync-notify! m :hello)
    (is (= :hello (ev/call-handler! m :a [:last])))))

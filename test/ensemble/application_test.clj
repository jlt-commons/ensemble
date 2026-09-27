(ns ensemble.application-test
  (:require [clojure.test :refer [deftest is]]
            [ensemble.actor :as act]
            [ensemble.application :as app]
            [ensemble.supervisor :as sup]))

(deftest start-then-stop-runs-the-lifecycle
  (let [log (atom [])
        a {:name ::demo
           :start (fn [] (swap! log conj :start) :state)
           :stop  (fn [state] (swap! log conj [:stop state]))}]
    (is (= ::demo (app/start-application a)))
    (is (app/started? ::demo))
    (is (= [:start] @log))
    (is (= ::demo (app/stop-application ::demo)))
    (is (not (app/started? ::demo)))
    (is (= [:start [:stop :state]] @log))))

(deftest starting-an-already-started-application-throws
  (let [a {:name ::once :start (fn [] :s) :stop (fn [_] nil)}]
    (app/start-application a)
    (is (thrown? Exception (app/start-application a)))
    (app/stop-application ::once)))

(deftest stopping-an-unknown-application-is-a-no-op
  (is (nil? (app/stop-application ::missing))))

(deftest stopping-an-application-stops-its-supervisor
  (let [sup (atom nil)
        a {:name ::tree
           :start (fn [] (reset! sup (sup/start-supervisor {})))
           :stop  (fn [s] (sup/stop-supervisor! s))}]
    (app/start-application a)
    (app/stop-application ::tree)
    (is (act/done? @sup))))

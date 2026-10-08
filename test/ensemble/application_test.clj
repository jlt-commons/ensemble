(ns ensemble.application-test
  (:require [clojure.core.async :as a]
            [clojure.test :refer [deftest is]]
            [ensemble.actor :as act :refer [receive]]
            [ensemble.application :as app]
            [ensemble.supervisor :as sup]))

(defn- eventually [pred]
  (loop [i 0] (cond (pred) true (> i 200) false :else (do (a/<!! (a/timeout 10)) (recur (inc i))))))

(defn- worker [] (act/spawn-link (fn [] (receive [:die (act/exit! :boom)]))))

(defn- tree [] (sup/start {:intensity 0} [{:id :w :start worker}]))

(defn- spec [name & {:as more}]
  (merge {:name name :start (fn [] [(tree) name])} more))

(deftest start-and-stop-run-the-lifecycle
  (let [log (atom [])]
    (app/load! (spec ::a :stop (fn [st] (swap! log conj [:stop st]))))
    (is (= :ok (app/start! ::a)))
    (is (app/started? ::a))
    (is (thrown? Throwable (app/start! ::a)))
    (let [top (:top (get @@#'app/running ::a))]
      (is (= :ok (app/stop! ::a)))
      (is (false? (act/alive? top))))
    (is (= [[:stop ::a]] @log))
    (is (nil? (app/stop! ::a)))))

(deftest dependencies-must-be-running
  (app/load! (spec ::base))
  (app/load! (spec ::top :applications [::base]))
  (is (= [:not-started ::base] (try (app/start! ::top) (catch Throwable e (:reason (ex-data e))))))
  (is (= [::base ::top] (app/ensure-all-started! ::top)))
  (is (= [] (app/ensure-all-started! ::top)))
  (app/stop! ::top) (app/stop! ::base))

(deftest a-temporary-app-whose-tree-dies-is-only-reported
  (app/load! (spec ::temp))
  (app/load! (spec ::bystander))
  (app/start! ::temp) (app/start! ::bystander)
  (let [top (:top (get @@#'app/running ::temp))]
    (act/! (sup/child top :w) :die)
    (is (eventually #(not (app/started? ::temp))))
    (is (some #(= ::temp (first %)) (app/exits)))
    (is (app/started? ::bystander)))
  (app/stop! ::bystander))

(deftest a-permanent-app-whose-tree-dies-stops-everything
  (app/load! (spec ::perm :type :permanent))
  (app/load! (spec ::victim))
  (app/start! ::victim) (app/start! ::perm)
  (let [top (:top (get @@#'app/running ::perm))]
    (act/! (sup/child top :w) :die)
    (is (eventually #(not (app/started? ::victim))))))

(deftest applications-stop-in-reverse-start-order
  (let [log (atom [])
        names (mapv #(keyword "ensemble.application-test" (str "chain" %)) (range 8))]
    (doseq [[i n] (map-indexed vector names)]
      (app/load! (spec n :applications (if (pos? i) [(nth names (dec i))] [])
                       :type (if (= i 7) :permanent :temporary)
                       :stop (fn [_] (swap! log conj i)))))
    (app/ensure-all-started! (peek names))
    (let [top (:top (get @@#'app/running (peek names)))]
      (act/! (sup/child top :w) :die)
      (is (eventually #(= 7 (count @log))))
      (is (= [6 5 4 3 2 1 0] @log)))))

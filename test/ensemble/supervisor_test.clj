(ns ensemble.supervisor-test
  (:require [clojure.core.async :as a]
            [clojure.test :refer [deftest is]]
            [ensemble.actor :as act]
            [ensemble.supervisor :as sup]))

(defn- eventually
  "Poll pred for up to ~2s, parking the fiber between checks."
  [pred]
  (loop [i 0]
    (cond
      (pred) true
      (> i 200) false
      :else (do (a/<!! (a/timeout 10)) (recur (inc i))))))

(defn- boom
  "A child start fn: bump started, then die abnormally on any message."
  [started kill]
  (fn []
    (swap! started inc)
    (let [x (act/spawn (fn [] (act/receive [_ (throw (ex-info "child died" {}))])))]
      (reset! kill x)
      x)))

(defn- quiet
  "A child start fn: bump started, then return normally on any message."
  [started kill]
  (fn []
    (swap! started inc)
    (let [x (act/spawn (fn [] (act/receive [_ :ok])))]
      (reset! kill x)
      x)))

(deftest one-for-one-restarts-only-failed-child
  (let [a (atom 0) b (atom 0) kill-a (atom nil)
        sup (sup/start-supervisor {:strategy :one-for-one :max-restarts 5})]
    (is (= :a (sup/start-child! sup :a {:start (boom a kill-a)})))
    (sup/start-child! sup :b {:start (boom b (atom nil))})
    (is (= 1 @a))
    (is (= 1 @b))
    (act/! @kill-a :die)
    (is (eventually (fn [] (= 2 @a))))
    (is (= 1 @b))
    (is (= [:a :b] (sup/which-children! sup)))))

(deftest one-for-all-restarts-every-child
  (let [a (atom 0) b (atom 0) kill-a (atom nil)
        sup (sup/start-supervisor {:strategy :one-for-all :max-restarts 5})]
    (sup/start-child! sup :a {:start (boom a kill-a)})
    (sup/start-child! sup :b {:start (boom b (atom nil))})
    (act/! @kill-a :die)
    (is (eventually (fn [] (= 2 @a))))
    (is (eventually (fn [] (= 2 @b))))))

(deftest rest-for-one-restarts-failed-and-later
  (let [a (atom 0) b (atom 0) c (atom 0) kill-b (atom nil)
        sup (sup/start-supervisor {:strategy :rest-for-one :max-restarts 5})]
    (sup/start-child! sup :a {:start (boom a (atom nil))})
    (sup/start-child! sup :b {:start (boom b kill-b)})
    (sup/start-child! sup :c {:start (boom c (atom nil))})
    (act/! @kill-b :die)
    (is (eventually (fn [] (= 2 @b))))
    (is (eventually (fn [] (= 2 @c))))
    (is (= 1 @a))))

(deftest restart-intensity-shuts-supervisor-down
  (let [a (atom 0) kill (atom nil)
        sup (sup/start-supervisor {:strategy :one-for-one :max-restarts 2 :max-seconds 60})]
    (sup/start-child! sup :a {:start (boom a kill)})
    (act/! @kill :die)
    (is (eventually (fn [] (= 2 @a))))
    (act/! @kill :die)
    (is (eventually (fn [] (= 3 @a))))
    (act/! @kill :die)
    (is (eventually (fn [] (act/done? sup))))
    (is (= 3 @a))))

(deftest transient-child-survives-its-normal-exit
  (let [a (atom 0) kill (atom nil)
        sup (sup/start-supervisor {:strategy :one-for-one :max-restarts 5})]
    (sup/start-child! sup :a {:start (quiet a kill) :restart :transient})
    (act/! @kill :bye)
    (is (eventually (fn [] (empty? (sup/which-children! sup)))))
    (is (= 1 @a))))

(deftest transient-child-restarted-on-abnormal-exit
  (let [a (atom 0) kill (atom nil)
        sup (sup/start-supervisor {:strategy :one-for-one :max-restarts 5})]
    (sup/start-child! sup :a {:start (boom a kill) :restart :transient})
    (act/! @kill :die)
    (is (eventually (fn [] (= 2 @a))))))

(deftest temporary-child-never-restarted
  (let [a (atom 0) kill (atom nil)
        sup (sup/start-supervisor {:strategy :one-for-one :max-restarts 5})]
    (sup/start-child! sup :a {:start (boom a kill) :restart :temporary})
    (act/! @kill :die)
    (is (eventually (fn [] (empty? (sup/which-children! sup)))))
    (is (= 1 @a))))

(deftest terminate-child-untracks-it
  (let [a (atom 0)
        sup (sup/start-supervisor {})]
    (sup/start-child! sup :a {:start (quiet a (atom nil))})
    (is (= [:a] (sup/which-children! sup)))
    (sup/terminate-child! sup :a)
    (is (= [] (sup/which-children! sup)))))

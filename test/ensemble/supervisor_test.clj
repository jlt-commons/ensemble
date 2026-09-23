(ns ensemble.supervisor-test
  (:require [clojure.core.async :as a]
            [clojure.test :refer [deftest is]]
            [ensemble.actor :as act]
            [ensemble.gen-server :as gs]
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

(defrecord Rester []
  gs/Server
  (init [_] nil)
  (handle-call [_ _from _msg st] [:reply :ok st])
  (handle-cast [_ _msg st] [:noreply st])
  (handle-info [_ _msg st] [:noreply st])
  (handle-timeout [_ st] [:noreply st])
  (terminate [_ _reason _st] nil))

(deftest get-child-returns-the-actor
  (let [a (atom 0)
        sup (sup/start-supervisor {})]
    (sup/start-child! sup :a {:start (quiet a (atom nil))})
    (is (some? (sup/get-child sup :a)))
    (is (nil? (sup/get-child sup :missing)))))

(deftest remove-child-untracks-without-stopping-it
  (let [a (atom 0) kill (atom nil)
        sup (sup/start-supervisor {})]
    (sup/start-child! sup :a {:start (quiet a kill)})
    (sup/remove-child! sup :a)
    (is (= [] (sup/which-children! sup)))
    (is (false? (act/done? @kill)))
    (act/! @kill :bye)
    (is (= :ok (act/join @kill)))))

(deftest remove-and-terminate-child-stops-the-child
  (let [sup (sup/start-supervisor {})]
    (sup/start-child! sup :a {:start (fn [] (gs/gen-server (->Rester)))})
    (let [child (sup/get-child sup :a)]
      (sup/remove-and-terminate-child! sup :a)
      (is (= [] (sup/which-children! sup)))
      (is (eventually (fn [] (act/done? child)))))))

(deftest init-time-children
  (let [a (atom 0)
        sup (sup/start-supervisor {:children [{:id :a :start (quiet a (atom nil))}]})]
    (is (= [:a] (sup/which-children! sup)))
    (is (= 1 @a))))

(deftest permanent-child-restarts-on-normal-exit
  (let [a (atom 0) kill (atom nil)
        sup (sup/start-supervisor {:strategy :one-for-one :max-restarts 5})]
    (sup/start-child! sup :a {:start (quiet a kill) :restart :permanent})
    (act/! @kill :bye)
    (is (eventually (fn [] (= 2 @a))))
    (is (= [:a] (sup/which-children! sup)))))

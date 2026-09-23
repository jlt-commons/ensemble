(ns ensemble.ensemble-test
  (:require [clojure.test :refer [deftest is]]
            [ensemble :as e]))

(deftest actor-roundtrip-through-the-umbrella
  (let [got (atom nil)
        hearer (e/spawn (fn [] (reset! got (e/receive [:pong :pong]))))
        ping (e/spawn (fn [] (e/receive [[:ping to] (e/! to :pong)])))]
    (e/! ping [:ping hearer])
    (e/join hearer)
    (is (= :pong @got))))

(defn counter-loop [me]
  (e/receive
   [[:add k] (do (e/set-state! me (+ (e/state me) k)) (counter-loop me))]
   [[:get from] (do (e/! from (e/state me)) (counter-loop me))]))

(deftest readme-actor-example
  (let [got (atom nil)
        getter (e/spawn (fn [] (reset! got (e/receive [v v]))))
        counter (e/spawn (fn [] (counter-loop (e/self))) {:state 0})]
    (e/! counter [:add 3])
    (e/! counter [:add 4])
    (e/! counter [:get getter])
    (e/join getter)
    (is (= 7 @got))))

(defrecord Counter []
  e/Server
  (init [_] 0)
  (handle-call [_ _from msg st]
    (case (first msg)
      :add [:reply (+ st (nth msg 1)) (+ st (nth msg 1))]
      :get [:reply st st]))
  (handle-cast [_ _msg st] [:noreply st])
  (handle-info [_ _msg st] [:noreply st])
  (handle-timeout [_ st] [:noreply st])
  (terminate [_ _reason _st] nil))

(deftest gen-server-through-the-umbrella
  (let [s (e/gen-server (->Counter))]
    (is (= 3 (e/call! s [:add 3])))
    (is (= 3 (e/call! s [:get])))))

(deftest supervisor-through-the-umbrella
  (let [sup (e/start-supervisor {:strategy :one-for-one})]
    (e/start-child! sup :c {:start (fn [] (e/gen-server (->Counter)))})
    (is (= [:c] (e/which-children! sup)))
    (e/terminate-child! sup :c)
    (is (= [] (e/which-children! sup)))))

(deftest new-gen-server-api-through-the-umbrella
  (let [s (e/gen-server (->Counter))]
    (is (= 3 (e/call-timed! s 500 [:add 3])))
    (is (= 3 (e/call-timed! s 500 [:get])))
    (e/shutdown! s)
    (is (nil? (e/join s)))))

(deftest watch-through-the-umbrella
  (let [dead (e/spawn (fn [] :ok))
        w (e/spawn (fn [] (e/watch! dead) (e/receive [[:exit _ a c] [a c]])))]
    (is (= [dead nil] (e/join w)))))

(deftest child-ops-through-the-umbrella
  (let [sup (e/start-supervisor {})]
    (e/start-child! sup :c {:start (fn [] (e/gen-server (->Counter)))})
    (is (some? (e/get-child sup :c)))
    (e/remove-child! sup :c)
    (is (= [] (e/which-children! sup)))))

(ns ensemble.gen-event-test
  (:require [clojure.core.async :as a]
            [clojure.test :refer [deftest is]]
            [ensemble.actor :as act :refer [receive]]
            [ensemble.gen-event :as ev]))

(defrecord Sink [log]
  ev/Handler
  (h-init [_] 0)
  (h-handle-event [_ e st]
    (case e
      :crash [:ok st]
      :explode (throw (ex-info "handler crashed" {}))
      :leave :remove-handler
      :garbage :not-ok
      (do (swap! log conj e) [:ok (inc st)])))
  (h-handle-call [_ req st]
    (case req
      :count [:ok st st]
      :leave [:remove-handler :bye]))
  (h-terminate [_ reason _] (swap! log conj [:terminated reason]) :terminated))

(deftest notify-reaches-every-handler-in-order
  (let [l1 (atom []) l2 (atom [])
        m (ev/start)]
    (ev/add-handler! m :a (->Sink l1))
    (ev/add-handler! m :b (->Sink l2))
    (ev/notify m :one)
    (ev/sync-notify! m :two)
    (is (= [:one :two] @l1))
    (is (= [:one :two] @l2))
    (is (= [:a :b] (ev/which-handlers m)))
    (is (= 2 (ev/call! m :a :count)))))

(deftest adding-a-taken-id-throws
  (let [m (ev/start)]
    (ev/add-handler! m :a (->Sink (atom [])))
    (is (thrown? Throwable (ev/add-handler! m :a (->Sink (atom [])))))))

(deftest delete-handler-returns-terminates-result
  (let [l (atom [])
        m (ev/start)]
    (ev/add-handler! m :a (->Sink l))
    (is (= :terminated (ev/delete-handler! m :a :bye)))
    (ev/sync-notify! m :after)
    (is (= [[:terminated :bye]] @l))))

(defrecord Fragile [log]
  ev/Handler
  (h-init [_] nil)
  (h-handle-event [_ e st] (if (= :crash e) (throw (ex-info "fragile" {})) [:ok st]))
  (h-handle-call [_ _ st] [:ok nil st])
  (h-terminate [_ reason _] (swap! log conj reason)))

(deftest a-crashing-handler-is-removed-and-the-rest-carry-on
  (let [l1 (atom []) l2 (atom [])
        m (ev/start)]
    (ev/add-handler! m :a (->Fragile l1))
    (ev/add-handler! m :b (->Sink l2))
    (ev/sync-notify! m :ok)
    (swap! l2 (constantly []))
    (ev/sync-notify! m :crash)
    (is (= [:b] (ev/which-handlers m)))
    (is (= :error (first (first @l1))))
    (ev/sync-notify! m :after)
    (is (= [:after] @l2) "the surviving handler saw the later event")
    (is (act/alive? m))))

(deftest remove-handler-return-and-bad-returns
  (let [l (atom [])
        m (ev/start {:handlers [[:a (->Sink l)] [:b (->Sink l)]]})]
    (ev/sync-notify! m :leave)
    (is (= [] (ev/which-handlers m)))
    (ev/add-handler! m :c (->Sink l))
    (ev/sync-notify! m :garbage)
    (is (= [] (ev/which-handlers m)))
    (ev/add-handler! m :d (->Sink l))
    (is (= :bye (ev/call! m :d :leave)))
    (is (thrown? Throwable (ev/call! m :d :count)))))

(deftest a-sup-handler-goes-when-its-owner-exits
  (let [l (atom [])
        m (ev/start)
        owner (act/spawn (fn [] (ev/add-sup-handler! m :a (->Sink l)) (receive [_ (act/exit! :gone)])))]
    (loop [] (when (empty? (ev/which-handlers m)) (recur)))
    (act/! owner :go)
    (act/exit-reason owner 1000)
    (loop [n 0] (when (and (seq (ev/which-handlers m)) (< n 1000)) (recur (inc n))))
    (is (= [] (ev/which-handlers m)))
    (is (= [[:terminated [:stop :gone]]] @l))))

(deftest a-sup-handlers-owner-hears-when-it-is-removed
  (let [m (ev/start)
        owner (act/spawn (fn []
                           (ev/add-sup-handler! m :a (->Sink (atom [])))
                           (ev/notify m :explode)
                           (receive [[:gen-event-EXIT id reason] [id (first reason)]])))]
    (is (= [:a :error] (act/join owner 1000)))))

(deftest stopping-the-manager-terminates-the-handlers
  (let [l (atom [])
        m (ev/start {:handlers [[:a (->Sink l)]]})]
    (ev/stop! m)
    (is (= [[:terminated :stop]] @l))))

(defn- eventually [pred]
  (loop [i 0] (cond (pred) true (> i 200) false :else (do (a/<!! (a/timeout 10)) (recur (inc i))))))

;; a handler that also takes the manager's other messages, swaps itself out,
;; or asks the manager to hibernate
(defrecord Infoed [log tag]
  ev/Handler
  (h-init [_] tag)
  (h-handle-event [_ e st]
    (case e
      :swap [:swap-handler :swapped st :next (fn [res] (->Infoed log [:from res]))]
      :sleep [:ok st :hibernate]
      (do (swap! log conj [tag e]) [:ok st])))
  (h-handle-call [_ _ st] [:ok st st])
  (h-terminate [_ reason st] (swap! log conj [:terminated tag reason]) [:handed st])
  ev/InfoHandler
  (h-handle-info [_ msg st] (swap! log conj [:info tag msg]) [:ok st]))

(deftest other-messages-go-to-handle-info
  (let [l (atom [])
        m (ev/start)]
    (ev/add-handler! m :a (->Infoed l :a))
    (ev/add-handler! m :b (->Sink (atom [])))
    (act/! m :hello)
    (is (eventually #(= [[:info :a :hello]] @l)))
    (is (= [:a :b] (ev/which-handlers m)))))

(deftest swap-handler-hands-terminate-result-to-the-new-one
  (let [l (atom [])
        m (ev/start)]
    (ev/add-handler! m :old (->Infoed l :old))
    (is (= :ok (ev/swap-handler! m :old :bye :new (fn [res] (->Infoed l [:got res])))))
    (is (= [:new] (ev/which-handlers m)))
    (is (= [:got [:handed :old]] (ev/call! m :new :state)))
    (is (= [[:terminated :old :bye]] @l))))

(deftest a-handler-may-swap-itself
  (let [l (atom [])
        m (ev/start)]
    (ev/add-handler! m :old (->Infoed l :old))
    (ev/sync-notify! m :swap)
    (is (= [:next] (ev/which-handlers m)))
    (is (= [:from [:handed :old]] (ev/call! m :next :state)))))

(deftest a-handler-may-hibernate-the-manager
  (let [m (ev/start)]
    (ev/add-handler! m :a (->Infoed (atom []) :a))
    (ev/notify m :sleep)
    (is (eventually #(act/hibernating? m)))
    (is (= :a (ev/call! m :a :state)))))

(deftest a-supervised-handler-going-unlinks-its-owner
  (let [m (ev/start)
        owner (act/spawn (fn []
                           (ev/add-sup-handler! m :a (->Sink (atom [])))
                           (ev/delete-handler! m :a)
                           ;; linked still, the manager's death would kill it
                           (receive [:check (do (act/exit! m :kill) (receive [_ nil] [:after 100 nil]) :alive)])))]
    (a/<!! (a/timeout 50))
    (act/! owner :check)
    (is (= :alive (act/join owner 2000)))))

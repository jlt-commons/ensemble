(ns ensemble.gen-event-test
  (:require [clojure.test :refer [deftest is]]
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

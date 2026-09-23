(ns ensemble.gen-server-test
  (:require [clojure.test :refer [deftest is]]
            [clojure.core.async :as a]
            [ensemble.actor :as act]
            [ensemble.gen-server :as gs]))

(defrecord Counter []
  gs/Server
  (init [_] 0)
  (handle-call [_ _from msg st]
    (case (first msg)
      :get [:reply st st]
      :add (let [n (nth msg 1)] [:reply (+ st n) (+ st n)])))
  (handle-cast [_ msg st]
    (case (first msg)
      :tick [:noreply (inc st)]))
  (handle-info [_ _msg st] [:noreply st])
  (handle-timeout [_ st] [:noreply st])
  (terminate [_ _reason _st] nil))

(deftest call-get-and-add
  (let [s (gs/gen-server (->Counter))]
    (is (= 0 (gs/call! s [:get])))
    (is (= 5 (gs/call! s [:add 5])))
    (is (= 5 (gs/call! s [:get])))
    (is (= 9 (gs/call! s [:add 4])))))

(deftest cast-updates-state
  (let [s (gs/gen-server (->Counter))]
    (gs/cast! s [:tick])
    (gs/cast! s [:tick])
    (is (= 5 (gs/call! s [:add 3])))
    (gs/cast! s [:tick])
    (is (= 6 (gs/call! s [:get])))))

(defrecord Idle [limit]
  gs/Server
  (init [_] 0)
  (handle-call [_ _from _msg st] [:reply st st limit])
  (handle-cast [_ _msg st] [:noreply st limit])
  (handle-info [_ _msg st] [:noreply st limit])
  (handle-timeout [_ st] [:stop :idle st])
  (terminate [_ _reason _st] nil))

(deftest timeout-fires-and-stops
  (let [s (gs/gen-server (->Idle 40) {:timeout 40})]
    (is (= 0 (gs/call! s [:get])))
    (is (= :idle (act/join s)))))

(deftest stop-returns-reason
  (let [s (gs/gen-server (->Idle 30) {:timeout 30})]
    (is (= :idle (act/join s)))))

(defrecord Stopper []
  gs/Server
  (init [_] :up)
  (handle-call [_ _from _msg _st] [:stop :shutdown :down])
  (handle-cast [_ _msg st] [:noreply st])
  (handle-info [_ _msg st] [:noreply st])
  (handle-timeout [_ st] [:noreply st])
  (terminate [this reason st] (swap! (:log this) conj [:terminate reason st])))

(deftest terminate-runs-on-stop
  (let [log (atom [])
        s (gs/gen-server (assoc (->Stopper) :log log))]
    (act/! s [:call {:chan (a/chan 1)} :go])
    (is (= :shutdown (act/join s)))
    (is (= [[:terminate :shutdown :down]] @log))))

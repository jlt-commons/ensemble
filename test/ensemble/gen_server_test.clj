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

(deftest call-variadic-packs-args
  (let [s (gs/gen-server (->Counter))]
    (is (= 5 (gs/call! s :add 5)))
    (is (= 5 (gs/call! s [:get])))))

(deftest cast-variadic-packs-args
  (let [s (gs/gen-server (->Counter))]
    (gs/cast! s :tick :ignored)
    (is (= 1 (gs/call! s [:get])))))

(defrecord InfoSink [log]
  gs/Server
  (init [_] nil)
  (handle-call [_ _from _msg st] [:reply :ok st])
  (handle-cast [_ _msg st] [:noreply st])
  (handle-info [_ msg st] (swap! log conj msg) [:noreply st])
  (handle-timeout [_ st] [:noreply st])
  (terminate [_ _reason _st] nil))

(deftest bare-and-tagged-messages-reach-handle-info
  (let [log (atom [])
        s (gs/gen-server (->InfoSink log))]
    (act/! s :ping)
    (act/! s [:info :tagged])
    (is (= :ok (gs/call! s :sync)))
    (is (= [:ping :tagged] @log))))

(defrecord Deferred []
  gs/Server
  (init [_] nil)
  (handle-call [_ from _msg _st] [:noreply {:from from} 20])
  (handle-cast [_ _msg st] [:noreply st])
  (handle-info [_ _msg st] [:noreply st])
  (handle-timeout [_ st]
    (if-let [from (:from st)]
      (do (gs/reply! from :got-it) [:noreply nil])
      [:noreply st]))
  (terminate [_ _reason _st] nil))

(defrecord Silent []
  gs/Server
  (init [_] nil)
  (handle-call [_ _from _msg st] [:noreply st])
  (handle-cast [_ _msg st] [:noreply st])
  (handle-info [_ _msg st] [:noreply st])
  (handle-timeout [_ st] [:noreply st])
  (terminate [_ _reason _st] nil))

(deftest deferred-reply-from-handler
  (let [s (gs/gen-server (->Deferred))]
    (is (= :got-it (gs/call! s :later)))))

(defrecord Crasher [log]
  gs/Server
  (init [_] 0)
  (handle-call [_ _from _msg _st] (throw (ex-info "boom" {})))
  (handle-cast [_ _msg st] [:noreply st])
  (handle-info [_ _msg st] [:noreply st])
  (handle-timeout [_ st] [:noreply st])
  (terminate [_ reason _st] (swap! log conj (some? reason))))

(deftest crash-replies-error-and-terminates
  (let [log (atom [])
        s (gs/gen-server (->Crasher log))]
    (is (thrown? Throwable (gs/call! s :oops)))
    (is (= [true] @log))
    (is (thrown? Throwable (act/join s)))))

(deftest shutdown-stops-the-server
  (let [log (atom [])
        s (gs/gen-server (assoc (->Stopper) :log log))]
    (gs/shutdown! s)
    (is (nil? (act/join s)))
    (is (= [[:terminate nil :up]] @log))))

(deftest call-timed-throws-on-timeout
  (let [s (gs/gen-server (->Silent))]
    (is (thrown? Throwable (gs/call-timed! s 40 :never)))))

(defrecord NilReply []
  gs/Server
  (init [_] nil)
  (handle-call [_ _from _msg st] [:reply nil st])
  (handle-cast [_ _msg st] [:noreply st])
  (handle-info [_ _msg st] [:noreply st])
  (handle-timeout [_ st] [:noreply st])
  (terminate [_ _reason _st] nil))

(deftest nil-reply-is-not-a-timeout
  (let [s (gs/gen-server (->NilReply))]
    (is (nil? (gs/call! s :give-nil)))))

(defrecord ErrorReply []
  gs/Server
  (init [_] nil)
  (handle-call [_ from _msg st]
    (gs/reply-error! from (ex-info "nope" {}))
    [:noreply st])
  (handle-cast [_ _msg st] [:noreply st])
  (handle-info [_ _msg st] [:noreply st])
  (handle-timeout [_ st] [:noreply st])
  (terminate [_ _reason _st] nil))

(deftest explicit-error-reply-rethrows-in-caller
  (let [s (gs/gen-server (->ErrorReply))]
    (is (thrown-with-msg? Throwable #"nope" (gs/call! s :boom)))))

(defrecord RawReply []
  gs/Server
  (init [_] nil)
  (handle-call [_ from _msg st]
    (a/>!! (:chan from) [:unexpected 1])
    [:noreply st])
  (handle-cast [_ _msg st] [:noreply st])
  (handle-info [_ _msg st] [:noreply st])
  (handle-timeout [_ st] [:noreply st])
  (terminate [_ _reason _st] nil))

(deftest untagged-reply-is-rejected
  (let [s (gs/gen-server (->RawReply))]
    (is (thrown-with-msg? Throwable #"not tagged" (gs/call! s :x)))))

(defrecord ReceiveInCall []
  gs/Server
  (init [_] nil)
  (handle-call [_ _from msg st]
    (let [[peer a b] msg]
      (act/! peer [(act/self) (+ a b)])
      (act/receive [[_ m] [:reply m st]])))
  (handle-cast [_ _msg st] [:noreply st])
  (handle-info [_ _msg st] [:noreply st])
  (handle-timeout [_ st] [:noreply st])
  (terminate [_ _reason _st] nil))

(deftest receive-inside-handle-call
  (let [peer (act/spawn (fn []
                          (act/receive [[from m]
                                        (act/! from [(act/self) (str m "!!!")])])))
        s (gs/gen-server (->ReceiveInCall))]
    (is (= "7!!!" (gs/call! s peer 3 4)))))

(defrecord InfoCrasher [log]
  gs/Server
  (init [_] nil)
  (handle-call [_ _from _msg st] [:reply :ok st])
  (handle-cast [_ _msg st] [:noreply st])
  (handle-info [_ _msg st] (throw (ex-info "oops!" {})))
  (handle-timeout [_ st] [:noreply st])
  (terminate [_ reason _st] (swap! log conj reason)))

(deftest handle-info-crash-terminates-with-cause
  (let [log (atom [])
        s (gs/gen-server (->InfoCrasher log))]
    (act/! s :boom)
    (is (thrown? Throwable (act/join s)))
    (is (= ["oops!"] (mapv ex-message @log)))))

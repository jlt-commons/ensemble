(ns ensemble.gen-server-test
  (:require [clojure.core.async :as a]
            [clojure.test :refer [deftest is]]
            [ensemble.actor :as act :refer [receive]]
            [ensemble.gen-server :as gs]))

(defn- sleep [ms] (a/<!! (a/timeout ms)))

(defn- reason-of-failure [f]
  (try (f) :no-failure (catch Throwable e (:reason (ex-data e)))))

(defrecord Counter [log]
  gs/Server
  (init [_] 0)
  (handle-call [_ req _from st]
    (case (first req)
      :get   [:reply st st]
      :add   (let [n (+ st (second req))] [:reply n n])
      :crash (throw (ex-info "handler crashed" {}))
      :stop  [:stop (second req) :stopped st]
      :bad   :not-a-tagged-vector
      :slow  (do (sleep (second req)) [:reply :late st])))
  (handle-cast [_ req st]
    (case (first req)
      :tick [:noreply (inc st)]
      :stop [:stop (second req) st]))
  (handle-info [_ msg st] (when log (swap! log conj msg)) [:noreply st])
  (handle-timeout [_ st] [:noreply st])
  (terminate [_ reason st] (when log (swap! log conj [:terminate reason st]))))

(defn- counter
  ([] (gs/start (->Counter nil)))
  ([log] (gs/start (->Counter log))))

(deftest call-and-cast
  (let [s (counter)]
    (is (= 0 (gs/call! s [:get])))
    (is (= 5 (gs/call! s [:add 5])))
    (is (= :ok (gs/cast! s [:tick])))
    (is (= 6 (gs/call! s [:get])))))

(deftest calls-work-from-inside-an-actor
  (let [s (counter)
        c (act/spawn (fn [] [(gs/call! s [:add 2]) (gs/call! s [:add 3])]))]
    (is (= [2 5] (act/join c 1000)))))

(deftest a-call-to-a-name
  (let [s (gs/start (->Counter nil) {:name :gs-counter-1})]
    (is (= 1 (gs/call! :gs-counter-1 [:add 1])))
    (gs/stop! s)))

(deftest start-with-a-taken-name-is-already-started
  (let [s (gs/start (->Counter nil) {:name :gs-counter-2})]
    (is (= [:already-started s]
           (reason-of-failure #(gs/start (->Counter nil) {:name :gs-counter-2}))))
    (gs/stop! s)))

(deftest a-call-to-nothing-is-noproc
  (is (= :noproc (reason-of-failure #(gs/call! :gs-nobody [:get]))))
  (let [s (counter)]
    (gs/stop! s)
    (is (= :noproc (reason-of-failure #(gs/call! s [:get]))))
    (is (= :noproc (reason-of-failure #(act/join (act/spawn (fn [] (gs/call! s [:get])))))))))

(deftest a-cast-to-nothing-is-fine
  (is (= :ok (gs/cast! :gs-nobody [:tick]))))

(deftest a-crashing-handler-fails-the-call-and-stops-the-server
  (let [log (atom [])
        s (counter log)
        r (reason-of-failure #(gs/call! s [:crash]))]
    (is (instance? Throwable r))
    (is (= r (act/exit-reason s 1000)))
    (is (= [[:terminate r 0]] @log))))

(deftest a-crash-seen-from-an-actor-caller
  (let [s (counter)
        c (act/spawn (fn [] (reason-of-failure #(gs/call! s [:crash]))))]
    (is (instance? Throwable (act/join c 1000)))))

(deftest stop-with-reply-answers-then-exits-with-reason
  (let [log (atom [])
        s (counter log)]
    (is (= :stopped (gs/call! s [:stop :done])))
    (is (= :done (act/exit-reason s 1000)))
    (is (= [[:terminate :done 0]] @log))))

(deftest a-cast-stop-exits-with-the-reason
  (let [s (counter)]
    (gs/cast! s [:stop :shutdown])
    (is (= :shutdown (act/exit-reason s 1000)))))

(deftest a-bad-return-value-stops-the-server
  (let [s (counter)]
    (is (= [:bad-return-value :not-a-tagged-vector]
           (reason-of-failure #(gs/call! s [:bad]))))))

(deftest a-call-times-out-and-the-late-reply-is-dropped
  (let [s (counter)
        c (act/spawn (fn []
                       (let [r (reason-of-failure #(gs/call! s [:slow 100] 20))]
                         (sleep 200)
                         [r (receive [m m] [:after 0 :mailbox-empty])])))]
    (is (= [:timeout :mailbox-empty] (act/join c 1000)))))

(deftest calling-yourself-fails
  (let [s (gs/start (reify gs/Server
                      (init [_] nil)
                      (handle-call [this req _ st]
                        [:reply (reason-of-failure #(gs/call! (act/self) :again)) st])
                      (handle-cast [_ _ st] [:noreply st])
                      (handle-info [_ _ st] [:noreply st])
                      (handle-timeout [_ st] [:noreply st])
                      (terminate [_ _ _] nil)))]
    (is (= :calling-self (gs/call! s :go)))))

(deftest plain-messages-go-to-handle-info
  (let [log (atom [])
        s (counter log)]
    (act/! s [:hello])
    (act/! s [:cast :looks-like-a-cast-but-is-not])
    (gs/call! s [:get])
    (is (= [[:hello] [:cast :looks-like-a-cast-but-is-not]] @log))))

(deftest stop!-runs-terminate-and-waits
  (let [log (atom [])
        s (counter log)]
    (is (= :ok (gs/stop! s)))
    (is (false? (act/alive? s)))
    (is (= [[:terminate :normal 0]] @log))
    (is (= :noproc (reason-of-failure #(gs/stop! s))))))

(defrecord FailInit []
  gs/Server
  (init [_] (throw (ex-info "no" {:why :init})))
  (handle-call [_ _ _ st] [:reply nil st])
  (handle-cast [_ _ st] [:noreply st])
  (handle-info [_ _ st] [:noreply st])
  (handle-timeout [_ st] [:noreply st])
  (terminate [_ _ _] nil))

(deftest a-failing-init-throws-to-the-starter
  (is (thrown? Throwable (gs/start (->FailInit)))))

(deftest a-failing-start-link-does-not-kill-the-caller
  (let [c (act/spawn (fn [] (try (gs/start-link (->FailInit)) (catch Throwable _ :failed))))]
    (is (= :failed (act/join c 1000)))))

(defrecord Idle [limit log]
  gs/Server
  (init [_] 0)
  (handle-call [_ _ _ st] [:reply st st limit])
  (handle-cast [_ _ st] [:noreply st limit])
  (handle-info [_ _ st] [:noreply st limit])
  (handle-timeout [_ st] [:stop :idle st])
  (terminate [_ reason _] (swap! log conj reason)))

(deftest a-returned-timeout-runs-handle-timeout
  (let [log (atom [])
        s (gs/start (->Idle 30 log) {:timeout 30})]
    (is (= :idle (act/exit-reason s 1000)))
    (is (= [:idle] @log))))

(deftest any-message-rearms-the-timeout
  (let [log (atom [])
        s (gs/start (->Idle 60 log))]
    (dotimes [_ 4] (sleep 20) (gs/cast! s :poke))
    (is (act/alive? s))
    (is (= :idle (act/exit-reason s 1000)))))

(deftest a-trapping-server-terminates-when-its-parent-exits
  (let [log (atom [])
        child (promise)
        parent (act/spawn (fn []
                            (deliver child (gs/start-link (->Counter log) {:trap true}))
                            (receive [:die (act/exit! :parent-gone)])))]
    (let [s @child]
      (act/! parent :die)
      (is (= :parent-gone (act/exit-reason s 1000)))
      (is (= [[:terminate :parent-gone 0]] @log)))))

(deftest a-non-trapping-server-dies-with-its-parent-without-terminate
  (let [log (atom [])
        child (promise)
        parent (act/spawn (fn []
                            (deliver child (gs/start-link (->Counter log)))
                            (receive [:die (act/exit! :parent-gone)])))]
    (let [s @child]
      (act/! parent :die)
      (is (= :parent-gone (act/exit-reason s 1000)))
      (is (= [] @log)))))

(deftest an-exit-from-a-non-parent-is-just-info-for-a-trapping-server
  (let [log (atom [])
        s (gs/start (->Counter log) {:trap true})
        other (act/spawn (fn [] (act/exit! s :hello) :sent))]
    (act/join other 1000)
    (gs/call! s [:get])
    (is (= [[:EXIT other :hello]] @log))
    (gs/stop! s)))

(defrecord Deferred []
  gs/Server
  (init [_] nil)
  (handle-call [_ req from st]
    (act/spawn (fn [] (sleep 10) (gs/reply! from [:later req])))
    [:noreply st])
  (handle-cast [_ _ st] [:noreply st])
  (handle-info [_ _ st] [:noreply st])
  (handle-timeout [_ st] [:noreply st])
  (terminate [_ _ _] nil))

(deftest reply!-answers-later-from-anywhere
  (let [s (gs/start (->Deferred))]
    (is (= [:later :q] (gs/call! s :q)))
    (is (= [:later :r] (act/join (act/spawn (fn [] (gs/call! s :r))) 1000)))))

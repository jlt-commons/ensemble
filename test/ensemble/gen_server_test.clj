(ns ensemble.gen-server-test
  (:require [clojure.core.async :as a]
            [clojure.test :refer [deftest is testing]]
            [ensemble.actor :as act :refer [receive]]
            [ensemble.gen-server :as gs]))

(defn- sleep [ms] (a/<!! (a/timeout ms)))

(defn- reason-of-failure [f]
  (try (f) :no-failure (catch Throwable e (:reason (ex-data e)))))

(defrecord Counter [log]
  gs/Server
  (init [_] [:ok 0])
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
                      (init [_] [:ok nil])
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
  (init [_] [:ok 0 limit])
  (handle-call [_ _ _ st] [:reply st st limit])
  (handle-cast [_ _ st] [:noreply st limit])
  (handle-info [_ _ st] [:noreply st limit])
  (handle-timeout [_ st] [:stop :idle st])
  (terminate [_ reason _] (swap! log conj reason)))

(deftest a-returned-timeout-runs-handle-timeout
  (let [log (atom [])
        s (gs/start (->Idle 30 log))]
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
  (init [_] [:ok nil])
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

;; --- init results and continue --------------------------------------------

(defrecord Init [ret log]
  gs/Server
  (init [_] (ret))
  (handle-call [_ req _ st]
    (case (first req)
      :get [:reply st st]
      :then [:reply :ok st [:continue (second req)]]))
  (handle-cast [_ req st] (swap! log conj [:cast req]) [:noreply st])
  (handle-info [_ msg st] (swap! log conj [:info msg]) [:noreply st])
  (handle-timeout [_ st] (swap! log conj :timeout) [:noreply st])
  (handle-continue [_ c st]
    (swap! log conj [:continue c])
    (case c
      :slow (do (sleep 30) [:noreply st])
      :chain [:noreply st [:continue :chained]]
      :stop [:stop :done st]
      [:noreply [st c]]))
  (terminate [_ _ _] nil))

(defn- init-server [ret] (let [log (atom [])] [(gs/start (->Init ret log)) log]))

(deftest init-ok-starts-with-the-state
  (let [[s] (init-server (fn [] [:ok 7]))] (is (= 7 (gs/call! s [:get])))))

(deftest init-with-a-timeout-arms-it
  (let [[s log] (init-server (fn [] [:ok 0 20]))]
    (sleep 60)
    (is (= [:timeout] @log))
    (gs/stop! s)))

(deftest init-hibernate-starts-and-serves
  (let [[s] (init-server (fn [] [:ok 3 :hibernate]))] (is (= 3 (gs/call! s [:get])))))

(deftest init-ignore-returns-ignore-and-the-process-exits-normal
  (let [log (atom [])
        a (atom nil)
        r (gs/start (->Init (fn [] (reset! a (act/self)) :ignore) log))]
    (is (= :ignore r))
    (is (= :normal (act/exit-reason @a 1000)))))

(deftest init-error-fails-the-start-and-the-process-exits-normal
  (let [a (atom nil)
        why (try (gs/start (->Init (fn [] (reset! a (act/self)) [:error :nope]) (atom [])))
                 (catch Throwable e (:reason (ex-data e))))]
    (is (= :nope why))
    (is (= :normal (act/exit-reason @a 1000)))))

(deftest init-stop-fails-the-start-and-the-process-exits-with-the-reason
  (let [a (atom nil)
        why (try (gs/start (->Init (fn [] (reset! a (act/self)) [:stop :nope]) (atom [])))
                 (catch Throwable e (:reason (ex-data e))))]
    (is (= :nope why))
    (is (= :nope (act/exit-reason @a 1000)))))

(deftest a-linked-start-that-stops-does-not-kill-the-caller
  (let [c (act/spawn (fn [] (try (gs/start-link (->Init (fn [] [:stop :nope]) (atom [])))
                                 (catch Throwable e (:reason (ex-data e))))))]
    (is (= :nope (act/join c 1000)))))

(deftest a-bare-state-from-init-is-a-bad-return-value
  (is (= [:bad-return-value 0]
         (try (gs/start (->Init (fn [] 0) (atom []))) (catch Throwable e (:reason (ex-data e)))))))

(deftest a-continue-from-init-runs-before-any-message
  (let [[s log] (init-server (fn [] [:ok nil [:continue :slow]]))]
    (gs/cast! s :first)
    (act/! s :hello)
    (is (nil? (gs/call! s [:get])))
    (is (= [[:continue :slow] [:cast :first] [:info :hello]] @log))))

(deftest a-continue-after-a-reply-runs-before-the-next-message
  (let [[s log] (init-server (fn [] [:ok :s]))]
    (is (= :ok (gs/call! s [:then :x])))
    (is (= [:s :x] (gs/call! s [:get])))
    (is (= [[:continue :x]] @log))))

(deftest a-continue-may-continue-and-may-stop
  (let [[s log] (init-server (fn [] [:ok :s [:continue :chain]]))]
    (is (= [:s :chained] (gs/call! s [:get])))
    (is (= [[:continue :chain] [:continue :chained]] @log)))
  (let [[s log] (init-server (fn [] [:ok :s [:continue :stop]]))]
    (is (= :done (act/exit-reason s 1000)))))

;; --- hibernation ------------------------------------------------------------

(defrecord Sleeper []
  gs/Server
  (init [_] [:ok 0])
  (handle-call [_ req _ n]
    (case req
      :get [:reply n n]
      :nap [:reply :ok (inc n) :hibernate]))
  (handle-cast [_ _ n] [:noreply (inc n) :hibernate])
  (handle-info [_ _ n] [:noreply n])
  (handle-timeout [_ n] [:noreply n])
  (terminate [_ _ _] nil))

(defn- eventually [pred]
  (loop [i 0] (cond (pred) true (> i 200) false :else (do (sleep 10) (recur (inc i))))))

(deftest a-hibernate-action-hibernates-and-the-next-call-wakes-it
  (let [s (gs/start (->Sleeper))]
    (is (= :ok (gs/call! s :nap)))
    (is (eventually #(act/hibernating? s)))
    (is (= 1 (gs/call! s :get)) "it serves again, with its state")
    (gs/cast! s :tick)
    (is (eventually #(act/hibernating? s)))
    (is (= 2 (gs/call! s :get)))
    (is (= :ok (gs/stop! s)) "and stops as ever")))

(deftest hibernate-after-hibernates-an-idle-server
  (let [s (gs/start (->Sleeper) {:hibernate-after 20})]
    (is (not (act/hibernating? s)))
    (is (eventually #(act/hibernating? s)))
    (is (= 0 (gs/call! s :get)))
    (is (eventually #(act/hibernating? s)))))

(deftest init-hibernate-starts-hibernated
  (let [[s] (init-server (fn [] [:ok 5 :hibernate]))]
    (is (eventually #(act/hibernating? s)))
    (is (= 5 (gs/call! s [:get])))))

(deftest passivate-after-keeps-an-idle-server-on-disk
  (let [s (gs/start (->Sleeper) {:passivate-after 20})]
    (is (eventually #(act/passivated? s)))
    (is (= :ok (gs/call! s :nap)) "a call wakes it from disk")
    (is (= 1 (gs/call! s :get)))
    (is (eventually #(act/passivated? s)))
    (is (= :ok (gs/stop! s)))))

;; --- asynchronous calls -----------------------------------------------------

(deftest send-request-and-receive-response
  (let [s (counter)]
    (is (= [:reply 5]
           (act/join (act/spawn (fn [] (gs/receive-response (gs/send-request s [:add 5]) 1000))) 2000)))
    (testing "several in flight, answered in any order"
      (is (= [[:reply 6] [:reply 6]]
             (act/join (act/spawn (fn [] (let [a (gs/send-request s [:add 1]) b (gs/send-request s [:get])]
                                           [(gs/receive-response a 1000) (gs/receive-response b 1000)])))
                       2000))))))

(deftest a-request-to-a-server-that-dies-is-an-error
  (let [s (counter)
        r (act/join (act/spawn (fn [] (gs/receive-response (gs/send-request s [:crash]) 1000))) 2000)]
    (is (= :error (first r)))
    (is (= s (second (second r))) "with the server as it was named")))

(deftest a-request-to-a-name-nobody-holds-is-noproc
  (is (= [:error [:noproc :nobody-here]]
         (act/join (act/spawn (fn [] (gs/receive-response (gs/send-request :nobody-here [:get]) 1000))) 2000))))

(deftest wait-keeps-and-receive-abandons-on-timeout
  (let [s (counter)]
    (is (= [:timeout [:reply :late]]
           (act/join (act/spawn (fn [] (let [r (gs/send-request s [:slow 40])]
                                         [(gs/wait-response r 5) (gs/wait-response r 1000)])))
                     2000)))
    (is (= [:timeout :nothing]
           (act/join (act/spawn (fn [] (let [r (gs/send-request s [:slow 40])]
                                         [(gs/receive-response r 5)
                                          (receive [[_ :late] :late-reply-arrived] [:after 100 :nothing])])))
                     2000)))))

(deftest check-response-reads-one-message
  (let [s (counter)]
    (is (= [:no-reply [:reply 0]]
           (act/join (act/spawn (fn [] (let [r (gs/send-request s [:get])
                                             m (receive [x :when (and (vector? x) (= 2 (count x))) x])]
                                         [(gs/check-response :unrelated r) (gs/check-response m r)])))
                     2000)))))

(deftest a-collection-answers-with-labels
  (let [s (counter)]
    (is (= [#{[[:reply 2] :two] [[:reply 2] :get-after]} 0 :no-request]
           (act/join (act/spawn (fn []
                                  (let [c (->> (gs/reqids-new)
                                               (gs/send-request s [:add 2] :two)
                                               (gs/send-request s [:get] :get-after))
                                        [r1 l1 c] (gs/receive-response c 1000 true)
                                        [r2 l2 c] (gs/receive-response c 1000 true)]
                                    [#{[r1 l1] [r2 l2]} (gs/reqids-size c) (gs/receive-response c 10 true)])))
                     2000)))))

;; --- start's :timeout bounds init, as OTP's {timeout, T} -----------------

(defrecord SlowInit [ms]
  gs/Server
  (init [_] (a/<!! (a/timeout ms)) [:ok :up])
  (handle-call [_ _ _ st] [:reply st st])
  (handle-cast [_ _ st] [:noreply st])
  (handle-info [_ _ st] [:noreply st])
  (handle-timeout [_ st] [:noreply st])
  (handle-continue [_ _ st] [:noreply st])
  (terminate [_ _ _] nil))

(deftest an-init-that-takes-too-long-fails-the-start-with-timeout
  (let [t0 (System/currentTimeMillis)
        r (try (gs/start (->SlowInit 5000) {:timeout 100 :name ::slow})
               (catch Throwable e (:reason (ex-data e))))]
    (is (= :timeout r))
    (is (< (- (System/currentTimeMillis) t0) 2000))
    (is (nil? (act/whereis ::slow)))))

(deftest an-init-within-the-timeout-starts
  (let [s (gs/start (->SlowInit 20) {:timeout 1000})]
    (is (= :up (gs/call! s :get)))
    (gs/stop! s)))

(deftest a-start-link-whose-init-times-out-does-not-kill-the-caller
  (let [c (act/spawn (fn [] (try (gs/start-link (->SlowInit 5000) {:timeout 50})
                                 (catch Throwable e (:reason (ex-data e))))))]
    (is (= :timeout (act/join c 2000)))))

(deftest call-and-stop-take-infinity
  (let [s (gs/start (->SlowInit 0) {:timeout :infinity})]
    (is (= :up (gs/call! s :get :infinity)))
    (is (= :up (act/join (act/spawn (fn [] (gs/call! s :get :infinity))) 1000)))
    (is (= :ok (gs/stop! s :normal :infinity)))))

(deftest wait-response-takes-infinity
  (let [s (gs/start (->SlowInit 0))
        a (act/spawn (fn [] (gs/wait-response (gs/send-request s :get) :infinity)))]
    (is (= [:reply :up] (act/join a 1000)))))

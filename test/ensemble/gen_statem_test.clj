(ns ensemble.gen-statem-test
  (:require [clojure.core.async :as a]
            [clojure.test :refer [deftest is]]
            [ensemble.actor :as act :refer [receive]]
            [ensemble.gen-server :as gs]
            [ensemble.gen-statem :as sm]))

(defn- sleep [ms] (a/<!! (a/timeout ms)))

;; the gen_statem docs' code lock: the right digits unlock it for a while
(defrecord CodeLock [code log]
  sm/Machine
  (init [_] [:ok :locked []])
  (handle-event [_ type content state buttons]
    (when log (swap! log conj [state type content]))
    (case state
      :locked
      (cond
        (= :cast type)
        (let [bs (conj buttons content)]
          (cond
            (= code bs) [:next-state :open [] [[:state-timeout 50 :lock]]]
            (< (count bs) (count code)) [:keep-state bs]
            :else [:keep-state []]))
        (and (vector? type) (= :call (first type)))
        [:keep-state-and-data [[:reply (second type) :locked]]]
        :else [:keep-state-and-data])
      :open
      (cond
        (= :state-timeout type) [:next-state :locked []]
        (and (vector? type) (= :call (first type)))
        [:keep-state-and-data [[:reply (second type) :open]]]
        (= :cast type) [:keep-state-and-data [:postpone]]
        :else [:keep-state-and-data])))
  (terminate [_ reason state _] (when log (swap! log conj [:terminate reason state]))))

(deftest the-code-lock-opens-and-locks-again
  (let [m (sm/start (->CodeLock [1 2 3] nil))]
    (is (= :locked (gs/call! m :state?)))
    (doseq [b [1 2 3]] (gs/cast! m b))
    (is (= :open (gs/call! m :state?)))
    (sleep 120)
    (is (= :locked (gs/call! m :state?)))
    (gs/stop! m)))

(deftest a-wrong-code-keeps-it-locked
  (let [m (sm/start (->CodeLock [1 2 3] nil))]
    (doseq [b [1 9 3]] (gs/cast! m b))
    (is (= :locked (gs/call! m :state?)))))

(deftest postponed-events-are-retried-after-the-state-changes
  (let [log (atom [])
        m (sm/start (->CodeLock [1] log))]
    (gs/cast! m 1)
    (gs/cast! m 1)
    (is (= :open (gs/call! m :state?)))
    (sleep 120)
    (gs/call! m :state?)
    (let [casts (filterv #(= :cast (second %)) @log)]
      (is (= [[:locked :cast 1] [:open :cast 1] [:locked :cast 1]] casts)
          "the second cast was postponed while open, then retried when locked again"))))

(deftest stop!-runs-terminate
  (let [log (atom [])
        m (sm/start (->CodeLock [1] log))]
    (gs/stop! m :bye)
    (is (= [:terminate :bye :locked] (last @log)))
    (is (= :bye (act/exit-reason m 1000)))))

(defrecord Steps [log]
  sm/Machine
  (init [_] [:ok :a nil [[:next-event :internal :boot]]])
  (handle-event [_ type content state data]
    (swap! log conj [type content state])
    (cond
      (= [:internal :boot] [type content]) [:keep-state-and-data [[:next-event :internal 1] [:next-event :internal 2]]]
      (= :internal type) [:keep-state-and-data]
      (= :enter type) [:keep-state-and-data [[:timeout 30 :idle]]]
      (= :cast type) [:next-state content data]
      (= :timeout type) [:keep-state content]
      (= [:generic-timeout :g] type) [:keep-state [:generic content]]
      (and (vector? type) (= :call (first type)))
      (if (= :arm content)
        [:keep-state-and-data [[:reply (second type) :armed] [:generic-timeout :g 30 :ping]]]
        [:keep-state-and-data [[:reply (second type) [state data]]]])
      (= :info type) [:keep-state content]))
  (terminate [_ _ _ _] nil))

(deftest next-events-run-first-in-order
  (let [log (atom [])
        m (sm/start (->Steps log))]
    (gs/call! m :peek)
    (is (= [[:internal :boot :a] [:internal 1 :a] [:internal 2 :a]] (take 3 @log)))))

(deftest enter-calls-and-event-timeouts
  (let [log (atom [])
        m (sm/start (->Steps log) {:state-enter true})]
    (is (= [:enter :a :a] (first @log)) "the first state is entered")
    (gs/cast! m :b)
    (sleep 80)
    (is (some #{[:enter :a :b]} @log))
    (is (= [:b :idle] (gs/call! m :peek)) "the event timeout set on entry fired")))

(deftest any-event-cancels-the-event-timeout
  (let [log (atom [])
        m (sm/start (->Steps log) {:state-enter true})]
    (gs/cast! m :b)
    (dotimes [_ 4] (sleep 15) (act/! m :noise))
    (is (= [:b :noise] (gs/call! m :peek)))
    (sleep 60)
    (is (not-any? #(= :timeout (first %)) @log))))

(deftest a-generic-timeout-survives-a-state-change
  (let [log (atom [])
        m (sm/start (->Steps log))]
    (is (= :armed (gs/call! m :arm)))
    (gs/cast! m :c)
    (sleep 80)
    (is (= [:c [:generic :ping]] (gs/call! m :peek)))))

(deftest a-call-to-a-dead-machine-is-noproc
  (let [m (sm/start (->Steps (atom [])))]
    (gs/stop! m)
    (is (= :noproc (try (gs/call! m :peek) (catch Throwable e (:reason (ex-data e))))))))

(deftest a-bad-result-stops-the-machine
  (let [m (sm/start (reify sm/Machine
                      (init [_] [:ok :s nil])
                      (handle-event [_ _ _ _ _] :nonsense)
                      (terminate [_ _ _ _] nil)))]
    (gs/cast! m :x)
    (is (= [:bad-return-from-state-function :nonsense] (act/exit-reason m 1000)))))

(defrecord InitM [ret]
  sm/Machine
  (init [_] (ret))
  (handle-event [_ _ _ _ data] [:keep-state-and-data [[:reply-to-nobody]]])
  (terminate [_ _ _ _] nil))

(deftest statem-init-ignore-error-and-stop
  (let [a (atom nil)]
    (is (= :ignore (sm/start (->InitM (fn [] (reset! a (act/self)) :ignore)))))
    (is (= :normal (act/exit-reason @a 1000))))
  (let [a (atom nil)]
    (is (= :nope (try (sm/start (->InitM (fn [] (reset! a (act/self)) [:error :nope])))
                      (catch Throwable e (:reason (ex-data e))))))
    (is (= :normal (act/exit-reason @a 1000))))
  (let [a (atom nil)]
    (is (= :nope (try (sm/start (->InitM (fn [] (reset! a (act/self)) [:stop :nope])))
                      (catch Throwable e (:reason (ex-data e))))))
    (is (= :nope (act/exit-reason @a 1000))))
  (is (= [:bad-return-from-init [:s nil]]
         (try (sm/start (->InitM (fn [] [:s nil]))) (catch Throwable e (:reason (ex-data e)))))))

;; --- hibernation ------------------------------------------------------------

(defn- eventually [pred]
  (loop [i 0] (cond (pred) true (> i 200) false :else (do (sleep 10) (recur (inc i))))))

(defrecord Napper [log]
  sm/Machine
  (init [_] [:ok :awake 0])
  (handle-event [_ type content state n]
    (swap! log conj [state type content])
    (cond
      (= :nap content) [:next-state :asleep n [:hibernate]]
      (= :nap-with-timeout content) [:next-state :asleep n [:hibernate [:state-timeout 30 :ring]]]
      (and (vector? type) (= :call (first type))) [:keep-state n [[:reply (second type) [state n]]]]
      (= :state-timeout type) [:next-state :awake n]
      :else [:keep-state-and-data]))
  (terminate [_ _ _ _] nil))

(deftest a-hibernate-action-hibernates-the-machine
  (let [m (sm/start (->Napper (atom [])))]
    (gs/cast! m :nap)
    (is (eventually #(act/hibernating? m)))
    (is (= [:asleep 0] (gs/call! m :where)) "a call wakes it, in its state")))

(deftest a-hibernating-machine-still-sees-its-timeout
  (let [log (atom [])
        m (sm/start (->Napper log))]
    (gs/cast! m :nap-with-timeout)
    (is (eventually #(act/hibernating? m)))
    (is (eventually #(some #{[:asleep :state-timeout :ring]} @log)) "the state timeout fired and woke it")
    (is (= [:awake 0] (gs/call! m :where)))))

;; OTP takes one action alone where a list goes, and stops a machine whose
;; callback returns an action that is none
(defrecord Actions []
  sm/Machine
  (init [_] [:ok :s nil])
  (handle-event [_ type content _ _]
    (case content
      :single [:keep-state-and-data [:reply (second type) 42]]
      :bogus [:keep-state-and-data [[:reply (second type) 1] :bogus]]
      :junk :junk
      [:keep-state-and-data]))
  (terminate [_ _ _ _] nil))

(deftest one-action-alone-answers-the-call
  (let [m (sm/start (->Actions))]
    (is (= 42 (gs/call! m :single 1000)))
    (gs/stop! m)))

(deftest a-bad-action-stops-the-machine
  (let [m (sm/start (->Actions))]
    (is (thrown? Exception (gs/call! m :bogus 1000)))
    (is (= [:bad-action-from-state-function :bogus] (act/exit-reason m 1000)))))

(deftest a-bad-return-stops-the-machine
  (let [m (sm/start (->Actions))]
    (gs/cast! m :junk)
    (is (= [:bad-return-from-state-function :junk] (act/exit-reason m 1000)))))

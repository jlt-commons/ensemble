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

(defrecord SlowM [ms]
  sm/Machine
  (init [_] (sleep ms) [:ok :s nil])
  (handle-event [_ type _ _ _]
    (if (vector? type) [:keep-state-and-data [[:reply (second type) :here]]] [:keep-state-and-data]))
  (terminate [_ _ _ _] nil))

(deftest a-statem-init-that-takes-too-long-fails-with-timeout
  (is (= :timeout (try (sm/start (->SlowM 5000) {:timeout 100})
                       (catch Throwable e (:reason (ex-data e)))))))

(deftest hibernate-after-hibernates-an-idle-machine
  (let [m (sm/start (->SlowM 0) {:hibernate-after 30})]
    (is (eventually #(act/hibernating? m)))
    (is (= :here (gs/call! m :where)))
    (gs/stop! m)))

;; --- state_functions mode ---------------------------------------------------

(defrecord Light []
  sm/Machine
  (init [_] [:ok :off 0])
  (terminate [_ _ _ _] nil)
  sm/StateFunctions
  (state-functions [_]
    {:off (fn [type content n]
            (case type
              :cast [:next-state :on (inc n)]
              [:keep-state-and-data [[:reply (second type) [:off n]]]]))
     :on (fn [type content n]
           (case type
             :cast (if (= :break content) [:next-state :broken n] [:next-state :off n])
             [:keep-state-and-data [[:reply (second type) [:on n]]]]))}))

(deftest state-functions-call-the-state-s-own-fn
  (let [m (sm/start (->Light))]
    (is (= [:off 0] (gs/call! m :q)))
    (gs/cast! m :flip)
    (is (= [:on 1] (gs/call! m :q)))
    (gs/cast! m :flip)
    (is (= [:off 1] (gs/call! m :q)))
    (gs/stop! m)))

(deftest a-state-with-no-fn-stops-the-machine
  (let [m (sm/start (->Light))]
    (gs/cast! m :flip)
    (gs/cast! m :break)
    (gs/cast! m :flip)
    (is (= [:undefined-state-function :broken] (act/exit-reason m 1000)))))

;; --- repeat_state -------------------------------------------------------------

(defrecord Repeater [log]
  sm/Machine
  (init [_] [:ok :s 0])
  (handle-event [_ type content state n]
    (swap! log conj [type content])
    (cond
      (= :enter type) [:keep-state-and-data]
      (= [:cast :again] [type content]) [:repeat-state (inc n)]
      (= [:cast :again-same] [type content]) [:repeat-state-and-data]
      (= :cast type) [:keep-state-and-data [:postpone]]
      :else [:keep-state-and-data [[:reply (second type) n]]]))
  (terminate [_ _ _ _] nil))

(deftest repeat-state-runs-the-enter-call-again
  (let [log (atom [])
        m (sm/start (->Repeater log) {:state-enter true})]
    (gs/cast! m :held)
    (gs/cast! m :again)
    (is (= 1 (gs/call! m :n)))
    (gs/cast! m :again-same)
    (gs/call! m :n)
    (is (= [[:enter :s] [:cast :held] [:cast :again] [:enter :s] [:cast :again-same] [:enter :s]]
           (filterv #(not= :call (first %)) (map (fn [[t c]] [(if (vector? t) (first t) t) c]) @log)))
        "the enter call repeats, and the postponed event is not retried: the state did not change")
    (gs/stop! m)))

;; --- timeout cancel, update and abs -----------------------------------------

(defrecord Timers [log]
  sm/Machine
  (init [_] [:ok :s nil])
  (handle-event [_ type content _ _]
    (swap! log conj [type content])
    (case content
      :arm [:keep-state-and-data [[:generic-timeout :g 60 :first] [:state-timeout 60 :st]]]
      :cancel [:keep-state-and-data [[:generic-timeout :g :cancel] [:state-timeout :cancel]]]
      :update [:keep-state-and-data [[:generic-timeout :g :update :second]]]
      :update-none [:keep-state-and-data [[:state-timeout :update :now]]]
      :abs [:keep-state-and-data [[:generic-timeout :a (+ (act/now-ms) 40) :abs-fired {:abs true}]]]
      [:keep-state-and-data]))
  (terminate [_ _ _ _] nil))

(defn- fired
  "The timeouts that fired, as [kind content]: a generic one's type is
  [:generic-timeout name]."
  [log]
  (into [] (comp (map (fn [[t c]] [(if (vector? t) (first t) t) c]))
                 (filter #(contains? #{:state-timeout :generic-timeout} (first %))))
        @log))

(deftest a-cancel-stops-a-timer
  (let [log (atom [])
        m (sm/start (->Timers log))]
    (gs/cast! m :arm)
    (gs/cast! m :cancel)
    (sleep 150)
    (is (empty? (fired log)))
    (gs/stop! m)))

(deftest an-update-changes-the-content-but-not-the-deadline
  (let [log (atom [])
        m (sm/start (->Timers log))
        t0 (act/now-ms)]
    (gs/cast! m :arm)
    (sleep 30)
    (gs/cast! m :update)
    (is (eventually #(some #{[:generic-timeout :second]} (fired log))))
    (is (< (- (act/now-ms) t0) 85) "it fired at the first deadline, not 60ms after the update")
    (gs/stop! m)))

(deftest an-update-with-no-timer-fires-at-once
  (let [log (atom [])
        m (sm/start (->Timers log))]
    (gs/cast! m :update-none)
    (is (eventually #(some #{[:state-timeout :now]} (fired log))))
    (gs/stop! m)))

(deftest an-abs-timeout-fires-at-its-deadline
  (let [log (atom [])
        m (sm/start (->Timers log))]
    (gs/cast! m :abs)
    (is (eventually #(some #{[:generic-timeout :abs-fired]} (fired log))))
    (gs/stop! m)))

;; --- enter-loop ---------------------------------------------------------------

(deftest enter-loop-makes-an-actor-a-machine
  (let [m (act/spawn (fn [] (sm/enter-loop (->CodeLock [7] nil) {} :locked [])))]
    (is (= :locked (gs/call! m :state?)))
    (gs/cast! m 7)
    (is (= :open (gs/call! m :state?)))
    (gs/stop! m)
    (is (= :normal (act/exit-reason m 1000)))))

;; --- enter calls: repeat_state and the transition's event timeout ------------

(defrecord EnterRepeat [log]
  sm/Machine
  (init [_] [:ok :a nil])
  (handle-event [_ type content state _]
    (swap! log conj [type state])
    (cond
      (= :enter type) [:repeat-state-and-data]
      (= :cast type) [:next-state content nil [[:timeout 30 :quiet]]]
      (= :timeout type) [:keep-state-and-data]
      :else [:keep-state-and-data [[:reply (second type) state]]]))
  (terminate [_ _ _ _] nil))

(deftest an-enter-call-that-repeats-keeps-the-state
  (let [log (atom [])
        m (sm/start (->EnterRepeat log) {:state-enter true})]
    (is (= :a (gs/call! m :state 1000)) "the machine survives its enter call")
    (is (= 1 (count (filter #(= :enter (first %)) @log))) "the enter call runs once")
    (gs/stop! m)))

(deftest an-enter-call-keeps-the-transitions-event-timeout
  (let [log (atom [])
        m (sm/start (->EnterRepeat log) {:state-enter true})]
    (gs/cast! m :b)
    (sleep 150)
    (is (some #{[:timeout :b]} @log) "the event timeout set with the transition fires")
    (gs/stop! m)))

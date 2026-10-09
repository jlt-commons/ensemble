(ns ensemble.logger-test
  (:require [clojure.core.async :as a]
            [clojure.test :refer [deftest is]]
            [ensemble.actor :as act :refer [receive]]
            [ensemble.gen-server :as gs]
            [ensemble.gen-statem :as sm]
            [ensemble.logger :as log]
            [ensemble.supervisor :as sup]))

(defn- eventually [pred]
  (loop [i 0] (cond (pred) true (> i 200) false :else (do (a/<!! (a/timeout 10)) (recur (inc i))))))

(defmacro ^:private capturing
  "Run body with reports collected into the atom bound to sym."
  [sym & body]
  `(let [~sym (atom [])
         old# (log/set-handler! (fn [r#] (swap! ~sym conj r#)))]
     (try ~@body (finally (log/set-handler! old#)))))

(defn- of-kind [reports k] (filterv #(= k (:kind %)) @reports))

(deftest a-crash-is-reported
  (capturing rs
    (let [e (ex-info "boom" {})
          a (act/spawn (fn [] (throw e)) {:name ::crasher})]
      (act/exit-reason a 1000)
      (is (eventually #(seq (of-kind rs :crash-report))))
      (let [r (first (of-kind rs :crash-report))]
        (is (= :error (:level r)))
        (is (= a (:pid r)))
        (is (= ::crasher (:name r)))
        (is (= e (:reason r)))))))

(deftest a-normal-or-requested-exit-is-not-a-crash
  (capturing rs
    (act/exit-reason (act/spawn (fn [] :done)) 1000)
    (act/exit-reason (act/spawn (fn [] (act/exit! :shutdown))) 1000)
    (act/exit-reason (act/spawn (fn [] (act/exit! :boom))) 1000)
    (a/<!! (a/timeout 50))
    (is (empty? (of-kind rs :crash-report)))))

(defrecord Crashy []
  gs/Server
  (init [_] [:ok 0])
  (handle-call [_ req _ st] (if (= req :crash) (throw (ex-info "bad call" {})) [:reply st st]))
  (handle-cast [_ _ st] [:stop :shutdown st])
  (handle-info [_ _ st] [:noreply st])
  (handle-timeout [_ st] [:noreply st])
  (handle-continue [_ _ st] [:noreply st])
  (terminate [_ _ _] nil))

(deftest a-gen-server-that-terminates-abnormally-is-reported
  (capturing rs
    (let [s (gs/start (->Crashy))]
      (try (gs/call! s :crash 1000) (catch Throwable _ nil))
      (is (eventually #(seq (of-kind rs :gen-server-terminate))))
      (let [r (first (of-kind rs :gen-server-terminate))]
        (is (= s (:server r)))
        (is (= [:call :crash] (:last-message r)))
        (is (= 0 (:state r)))
        (is (= "bad call" (ex-message (:reason r))))))))

(deftest a-gen-server-stopping-with-shutdown-is-not-reported
  (capturing rs
    (let [s (gs/start (->Crashy))]
      (gs/cast! s :stop)
      (act/exit-reason s 1000)
      (a/<!! (a/timeout 50))
      (is (empty? (of-kind rs :gen-server-terminate))))))

(defrecord BadM []
  sm/Machine
  (init [_] [:ok :s 1])
  (handle-event [_ _ _ _ _] :nonsense)
  (terminate [_ _ _ _] nil))

(deftest a-gen-statem-that-terminates-abnormally-is-reported
  (capturing rs
    (let [m (sm/start (->BadM))]
      (gs/cast! m :x)
      (is (eventually #(seq (of-kind rs :gen-statem-terminate))))
      (let [r (first (of-kind rs :gen-statem-terminate))]
        (is (= m (:server r)))
        (is (= [:cast :x] (:last-event r)))
        (is (= :s (:state r)))
        (is (= [:bad-return-from-state-function :nonsense] (:reason r)))))))

(defn- dier [] (act/spawn-link (fn [] (receive [:die (act/exit! :boom)]))))

(deftest a-supervisor-reports-a-child-that-terminated
  (capturing rs
    (let [s (sup/start {:intensity 5} [{:id :w :start dier}])]
      (act/! (sup/child s :w) :die)
      (is (eventually #(seq (of-kind rs :supervisor-report))))
      (let [r (first (of-kind rs :supervisor-report))]
        (is (= :child-terminated (:context r)))
        (is (= :boom (:reason r)))
        (is (= :w (get-in r [:child :id]))))
      (sup/stop! s))))

(deftest a-supervisor-reports-giving-up
  (capturing rs
    (let [s (sup/start {:intensity 0} [{:id :w :start dier}])]
      (act/! (sup/child s :w) :die)
      (act/exit-reason s 1000)
      (is (eventually #(some (fn [r] (= :shutdown (:context r))) (of-kind rs :supervisor-report))))
      (is (= :reached-max-restart-intensity
             (:reason (first (filter #(= :shutdown (:context %)) (of-kind rs :supervisor-report)))))))))

(deftest a-supervisor-reports-a-failed-restart
  (capturing rs
    (let [n (atom 0)
          s (sup/start {:intensity 3}
                       [{:id :w :start (fn [] (if (zero? (swap! n inc)) nil (if (= 1 @n) (dier) [:error :nope])))}])]
      (act/! (sup/child s :w) :die)
      (is (eventually #(some (fn [r] (= :start-error (:context r))) (of-kind rs :supervisor-report))))
      (act/exit-reason s 2000))))

(deftest a-handler-that-throws-does-not-take-the-reporter-down
  (let [old (log/set-handler! (fn [_] (throw (ex-info "handler broke" {}))))]
    (try
      (let [a (act/spawn (fn [] (throw (ex-info "crash" {}))))]
        (is (instance? Throwable (act/exit-reason a 1000))))
      (finally (log/set-handler! old)))))

(defn- stubborn [_]
  ;; ignores :shutdown, so it is killed when its shutdown time is up
  (act/spawn-link (fn [] (act/trap-exit! true) (receive [:never nil]))))

(deftest a-simple-one-for-one-supervisor-reports-a-child-it-had-to-kill
  (capturing rs
    (let [s (sup/start {:strategy :simple-one-for-one} [{:id :tpl :start stubborn :shutdown 50}])]
      (sup/start-child! s [1])
      (sup/stop! s)
      (is (eventually #(some (fn [r] (= :shutdown-error (:context r))) (of-kind rs :supervisor-report))))
      (is (= :killed (:reason (first (filter #(= :shutdown-error (:context %)) (of-kind rs :supervisor-report)))))))))

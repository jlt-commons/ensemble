(ns ensemble.sys-test
  (:require [clojure.core.async :as a]
            [clojure.test :refer [deftest is]]
            [ensemble.actor :as act]
            [ensemble.gen-server :as gs]
            [ensemble.gen-statem :as sm]
            [ensemble.logger :as log]
            [ensemble.supervisor :as sup]
            [ensemble.sys :as sys]))

(defn- sleep [ms] (a/<!! (a/timeout ms)))

(defrecord Counter []
  gs/Server
  (init [_] [:ok {:n 0 :secret "pw"}])
  (handle-call [_ req _ st] (case req :n [:reply (:n st) st] :crash (throw (ex-info "boom" {}))))
  (handle-cast [_ _ st] [:noreply (update st :n inc)])
  (handle-info [_ _ st] [:noreply st])
  (handle-timeout [_ st] [:noreply st])
  (handle-continue [_ _ st] [:noreply st])
  (terminate [_ _ _] nil))

(defrecord Secret []
  gs/Server
  (init [_] [:ok {:n 0 :secret "pw"}])
  (handle-call [_ req _ st] (case req :crash (throw (ex-info "boom" {})) [:reply (:n st) st]))
  (handle-cast [_ _ st] [:noreply st])
  (handle-info [_ _ st] [:noreply st])
  (handle-timeout [_ st] [:noreply st])
  (handle-continue [_ _ st] [:noreply st])
  (terminate [_ _ _] nil)
  gs/FormatStatus
  (format-status [_ status] (update status :state dissoc :secret)))

(deftest get-state-and-replace-state
  (let [s (gs/start (->Counter))]
    (gs/cast! s :inc)
    (is (= {:n 1 :secret "pw"} (sys/get-state s)))
    (is (= {:n 10 :secret "pw"} (sys/replace-state! s #(assoc % :n 10))))
    (is (= 10 (gs/call! s :n)))
    (is (thrown? Throwable (sys/replace-state! s (fn [_] (throw (ex-info "no" {}))))))
    (is (= 10 (gs/call! s :n)) "a replace fn that throws leaves the state")
    (gs/stop! s)))

(deftest a-suspended-server-handles-only-system-messages
  (let [s (gs/start (->Counter))]
    (sys/suspend! s)
    (gs/cast! s :inc)
    (sleep 30)
    (is (= {:n 0 :secret "pw"} (sys/get-state s)))
    (is (= :suspended (:status (sys/get-status s))))
    (sys/resume! s)
    (is (= 1 (gs/call! s :n)))
    (gs/stop! s)))

(deftest get-status-describes-the-server
  (let [s (gs/start (->Counter) {:name ::counted})
        st (sys/get-status s)]
    (is (= s (:pid st)))
    (is (= ::counted (:name st)))
    (is (= :running (:status st)))
    (is (= {:n 0 :secret "pw"} (:state st)))
    (gs/stop! s)))

(deftest format-status-filters-status-and-reports
  (let [s (gs/start (->Secret))
        reports (atom [])
        old (log/set-handler! #(swap! reports conj %))]
    (try
      (is (= {:n 0} (:state (sys/get-status s))))
      (try (gs/call! s :crash) (catch Throwable _ nil))
      (act/exit-reason s 1000)
      (is (= {:n 0} (:state (first (filter #(= :gen-server-terminate (:kind %)) @reports)))))
      (finally (log/set-handler! old)))))

(deftest statistics-count-the-messages
  (let [s (gs/start (->Counter))]
    (is (= :no-statistics (sys/statistics s :get)))
    (sys/statistics s true)
    (gs/cast! s :inc)
    (gs/cast! s :inc)
    (gs/call! s :n)
    (let [stats (sys/statistics s :get)]
      (is (= 3 (:messages-in stats)))
      (is (<= (:start-time stats) (:current-time stats))))
    (sys/statistics s false)
    (is (= :no-statistics (sys/statistics s :get)))
    (gs/stop! s)))

(deftest trace-reports-each-event
  (let [s (gs/start (->Counter))
        reports (atom [])
        old (log/set-handler! #(swap! reports conj %))]
    (try
      (sys/trace! s true)
      (gs/cast! s :inc)
      (gs/call! s :n)
      (sys/trace! s false)
      (gs/cast! s :inc)
      (gs/call! s :n)
      (is (= [[:cast :inc] [:call :n]] (mapv :event (filter #(= :sys-trace (:kind %)) @reports))))
      (finally (log/set-handler! old)))
    (gs/stop! s)))

(deftest sys-terminate-stops-the-server
  (let [s (gs/start (->Counter))]
    (sys/suspend! s)
    (is (= :ok (sys/terminate! s :bye)))
    (is (= :bye (act/exit-reason s 1000)))))

;; --- a gen-statem and a supervisor answer too ------------------------------

(defrecord Toggle []
  sm/Machine
  (init [_] [:ok :off 0])
  (handle-event [_ _ _ state n] [:next-state (if (= :off state) :on :off) (inc n)])
  (terminate [_ _ _ _] nil))

(deftest a-gen-statem-answers-system-messages
  (let [m (sm/start (->Toggle))]
    (gs/cast! m :flip)
    (is (= [:on 1] (sys/get-state m)))
    (sys/replace-state! m (fn [[_ n]] [:off (+ n 10)]))
    (is (= [:off 11] (sys/get-state m)))
    (sys/suspend! m)
    (gs/cast! m :flip)
    (sleep 30)
    (is (= [:off 11] (sys/get-state m)))
    (sys/resume! m)
    (sleep 30)
    (is (= [:on 12] (sys/get-state m)))
    (gs/stop! m)))

(deftest a-supervisor-answers-system-messages
  (let [s (sup/start {} [])]
    (is (map? (sys/get-state s)))
    (is (= :running (:status (sys/get-status s))))
    (sup/stop! s)))

;; --- proc_lib: initial call and ancestors -----------------------------------

(deftest a-server-records-its-initial-call-and-ancestors
  (let [parent (act/spawn (fn []
                            (let [s (gs/start-link (->Counter))]
                              (act/! (act/self) [:info (act/process-info s :initial-call)
                                                 (act/process-info s :ancestors)])
                              (act/receive [[:info ic an] [s ic an]]))))
        [s ic an] (act/join parent 1000)]
    (is (= [Counter :init] ic) "OTP's {Module, init, 1}")
    (is (= [parent] an))))

(deftest a-crash-report-names-the-initial-call-and-ancestors
  (let [reports (atom [])
        old (log/set-handler! #(swap! reports conj %))]
    (try
      (let [p (act/spawn (fn []
                           (let [c (act/spawn (fn [] (throw (ex-info "x" {}))) {:initial-call [:my :fn]})]
                             (act/exit-reason c 1000))))]
        (act/join p 1000)
        (let [r (first (filter #(= :crash-report (:kind %)) @reports))]
          (is (= [:my :fn] (:initial-call r)))
          (is (= [p] (:ancestors r)))))
      (finally (log/set-handler! old)))))

(deftest a-gen-statem-refuses-a-replaced-state-of-the-wrong-shape
  (let [m (sm/start (->Toggle))]
    (is (thrown? Throwable (sys/replace-state! m (fn [_] {:not :a-pair}))))
    (is (thrown? Throwable (sys/replace-state! m (fn [_] nil))))
    (is (= [:off 0] (sys/get-state m)) "the machine is unharmed")
    (gs/stop! m)))

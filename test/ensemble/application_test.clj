(ns ensemble.application-test
  (:require [clojure.core.async :as a]
            [clojure.test :refer [deftest is]]
            [ensemble.actor :as act :refer [receive]]
            [ensemble.application :as app]
            [ensemble.supervisor :as sup]))

(defn- eventually [pred]
  (loop [i 0] (cond (pred) true (> i 200) false :else (do (a/<!! (a/timeout 10)) (recur (inc i))))))

(defn- worker [] (act/spawn-link (fn [] (receive [:die (act/exit! :boom)]))))

(defn- tree [] (sup/start {:intensity 0} [{:id :w :start worker}]))

(defn- spec [name & {:as more}]
  (merge {:name name :start (fn [] [(tree) name])} more))

(deftest start-and-stop-run-the-lifecycle
  (let [log (atom [])]
    (app/load! (spec ::a :stop (fn [st] (swap! log conj [:stop st]))))
    (is (= :ok (app/start! ::a)))
    (is (app/started? ::a))
    (is (thrown? Throwable (app/start! ::a)))
    (let [top (:top (get @@#'app/running ::a))]
      (is (= :ok (app/stop! ::a)))
      (is (false? (act/alive? top))))
    (is (= [[:stop ::a]] @log))
    (is (nil? (app/stop! ::a)))))

(deftest dependencies-must-be-running
  (app/load! (spec ::base))
  (app/load! (spec ::top :applications [::base]))
  (is (= [:not-started ::base] (try (app/start! ::top) (catch Throwable e (:reason (ex-data e))))))
  (is (= [::base ::top] (app/ensure-all-started! ::top)))
  (is (= [] (app/ensure-all-started! ::top)))
  (app/stop! ::top) (app/stop! ::base))

(deftest a-temporary-app-whose-tree-dies-is-only-reported
  (app/load! (spec ::temp))
  (app/load! (spec ::bystander))
  (app/start! ::temp) (app/start! ::bystander)
  (let [top (:top (get @@#'app/running ::temp))]
    (act/! (sup/child top :w) :die)
    (is (eventually #(not (app/started? ::temp))))
    (is (some #(= ::temp (first %)) (app/exits)))
    (is (app/started? ::bystander)))
  (app/stop! ::bystander))

(deftest a-permanent-app-whose-tree-dies-stops-everything
  (app/load! (spec ::perm :type :permanent))
  (app/load! (spec ::victim))
  (app/start! ::victim) (app/start! ::perm)
  (let [top (:top (get @@#'app/running ::perm))]
    (act/! (sup/child top :w) :die)
    (is (eventually #(not (app/started? ::victim))))))

(deftest applications-stop-in-reverse-start-order
  (let [log (atom [])
        names (mapv #(keyword "ensemble.application-test" (str "chain" %)) (range 8))]
    (doseq [[i n] (map-indexed vector names)]
      (app/load! (spec n :applications (if (pos? i) [(nth names (dec i))] [])
                       :type (if (= i 7) :permanent :temporary)
                       :stop (fn [_] (swap! log conj i)))))
    (app/ensure-all-started! (peek names))
    (let [top (:top (get @@#'app/running (peek names)))]
      (act/! (sup/child top :w) :die)
      (is (eventually #(= 7 (count @log))))
      (is (= [6 5 4 3 2 1 0] @log)))))

;; --- env --------------------------------------------------------------------

(deftest an-application-has-an-env
  (app/load! (spec ::env-app :env {:port 80 :host "h"}))
  (is (= 80 (app/get-env ::env-app :port)))
  (is (nil? (app/get-env ::env-app :missing)))
  (is (= :dflt (app/get-env ::env-app :missing :dflt)))
  (app/set-env! ::env-app :port 8080)
  (is (= 8080 (app/get-env ::env-app :port)))
  (app/unset-env! ::env-app :host)
  (is (= {:port 8080} (app/get-all-env ::env-app)))
  (app/load! (spec ::env-app :env {:port 80}))
  (is (= 8080 (app/get-env ::env-app :port)) "a set value outlives a reload, as OTP keeps it")
  (is (= {:port 80} (app/get-key ::env-app :env)) "get-key reads the spec as loaded"))

;; --- start phases and prep-stop ------------------------------------------------

(deftest start-phases-run-in-order-after-start
  (let [log (atom [])]
    (app/load! (spec ::phased
                     :start-phases [[:init {:a 1}] [:go nil]]
                     :start-phase (fn [phase type args] (swap! log conj [phase type args]) :ok)))
    (app/start! ::phased)
    (is (= [[:init :normal {:a 1}] [:go :normal nil]] @log))
    (app/stop! ::phased)))

(deftest a-failing-start-phase-fails-the-start
  (app/load! (spec ::bad-phase
                   :start-phases [[:init nil]]
                   :start-phase (fn [_ _ _] [:error :nope])))
  (is (= [:bad-start-phase :init [:error :nope]]
         (try (app/start! ::bad-phase) (catch Throwable e (:reason (ex-data e))))))
  (is (not (app/started? ::bad-phase))))

(deftest prep-stop-runs-before-the-tree-stops
  (let [log (atom [])
        top (atom nil)]
    (app/load! (spec ::prepped
                     :prep-stop (fn [st]
                                  (swap! log conj [:prep st (act/alive? @top)])
                                  [:prepped st])
                     :stop (fn [st] (swap! log conj [:stop st]))))
    (app/start! ::prepped)
    (reset! top (:top (get @@#'app/running ::prepped)))
    (app/stop! ::prepped)
    (is (= [[:prep ::prepped true] [:stop [:prepped ::prepped]]] @log))))

;; --- included and optional applications ---------------------------------------

(deftest an-included-application-is-not-started-on-its-own
  (app/load! (spec ::inc-child))
  (app/load! (spec ::inc-parent :included-applications [::inc-child]))
  (app/start! ::inc-parent)
  (is (= [:included ::inc-child ::inc-parent]
         (try (app/start! ::inc-child) (catch Throwable e (:reason (ex-data e))))))
  (app/stop! ::inc-parent)
  (is (= :ok (app/start! ::inc-child)) "once its includer has stopped it may start")
  (app/stop! ::inc-child))

(deftest an-included-application-must-be-loaded
  (app/load! (spec ::inc-missing :included-applications [::never-loaded]))
  (is (= [:not-loaded ::never-loaded]
         (try (app/start! ::inc-missing) (catch Throwable e (:reason (ex-data e)))))))

(deftest an-optional-dependency-may-be-absent
  (app/load! (spec ::opt-user :applications [::opt-dep ::not-there]
                   :optional-applications [::opt-dep ::not-there]))
  (app/load! (spec ::opt-dep))
  (is (= [:not-started ::opt-dep]
         (try (app/start! ::opt-user) (catch Throwable e (:reason (ex-data e)))))
      "loaded, it must be running first")
  (is (= [::opt-dep ::opt-user] (app/ensure-all-started! ::opt-user)))
  (app/stop! ::opt-user) (app/stop! ::opt-dep))

;; --- ensure-all-started! rolls back --------------------------------------------

(deftest ensure-all-started-stops-what-it-started-on-a-failure
  (let [log (atom [])]
    (app/load! (spec ::rb-a :stop (fn [_] (swap! log conj :a))))
    (app/load! (spec ::rb-b :applications [::rb-a] :stop (fn [_] (swap! log conj :b))))
    (app/load! {:name ::rb-c :applications [::rb-b] :start (fn [] (throw (ex-info "no" {})))})
    (is (thrown? Throwable (app/ensure-all-started! ::rb-c)))
    (is (= [:b :a] @log))
    (is (not (app/started? ::rb-a)))))

;; --- failures around start and stop -------------------------------------------

(deftest a-tree-dead-on-start-leaves-a-consistent-record
  (app/load! (spec ::stillborn
                   :start (fn [] (let [t (tree)] (act/exit! t :kill) (act/exit-reason t 1000) t))))
  (try (app/start! ::stillborn) (catch Throwable _ nil))
  (is (eventually #(not (app/started? ::stillborn))) "a dead tree is not running")
  (is (not (some #{::stillborn} (app/which-applications))))
  (app/stop! ::stillborn))

(deftest a-throwing-prep-stop-still-stops-the-tree
  (let [log (atom [])]
    (app/load! (spec ::bad-prep
                     :prep-stop (fn [_] (throw (ex-info "prep" {})))
                     :stop (fn [st] (swap! log conj [:stop st]))))
    (app/start! ::bad-prep)
    (let [top (:top (get @@#'app/running ::bad-prep))]
      (is (thrown? Throwable (app/stop! ::bad-prep)) "stop! reports the failure")
      (is (false? (act/alive? top)) "the tree is stopped all the same")
      (is (= [[:stop ::bad-prep]] @log) ":stop runs with the state prep-stop was given"))))

(deftest stop-all-gets-past-a-throwing-prep-stop
  (app/load! (spec ::first))
  (app/load! (spec ::bad-prep2 :prep-stop (fn [_] (throw (ex-info "prep" {})))))
  (app/start! ::first)
  (app/start! ::bad-prep2)
  (#'app/stop-all!)
  (is (not (app/started? ::first)) "the applications after the failing one stop too"))

(deftest reloading-a-spec-takes-its-new-env
  (app/load! (spec ::reloaded :env {:a 1 :b 1}))
  (app/set-env! ::reloaded :b 2)
  (app/load! (spec ::reloaded :env {:a 10 :b 10}))
  (is (= 10 (app/get-env ::reloaded :a)) "a value from the old spec gives way to the new")
  (is (= 2 (app/get-env ::reloaded :b)) "a value set-env! gave is kept"))

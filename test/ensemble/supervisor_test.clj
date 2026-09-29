(ns ensemble.supervisor-test
  (:require [clojure.core.async :as a]
            [clojure.test :refer [deftest is testing]]
            [ensemble.actor :as act :refer [receive]]
            [ensemble.gen-server :as gs]
            [ensemble.supervisor :as sup]))

(defn- sleep [ms] (a/<!! (a/timeout ms)))

(defn- eventually
  "Poll pred for up to ~2s."
  [pred]
  (loop [i 0]
    (cond
      (pred) true
      (> i 200) false
      :else (do (sleep 10) (recur (inc i))))))

(defn- worker
  "A child start fn: log [:start id], then run until told.  :die exits
  abnormally, :quit normally.  Traps exits when trap?, logging the shutdown."
  ([log id] (worker log id false))
  ([log id trap?]
   (fn []
     (swap! log conj [:start id])
     ;; trapping from the start: a supervisor may stop the child before its
     ;; body has run a step
     (act/spawn-link
      (fn []
        (loop []
          (receive
           [:die (act/exit! :boom)]
           [:quit :ok]
           [[:EXIT _ r] (do (swap! log conj [:stop id r]) (act/exit! r))]
           [_ (recur)])))
      {:trap trap?}))))

(defn- starts [log id] (count (filter #(= [:start id] %) @log)))

(defn- ids [s] (mapv :id (sup/which-children s)))

(deftest children-start-in-order
  (let [log (atom [])
        s (sup/start {} [{:id :a :start (worker log :a)}
                         {:id :b :start (worker log :b)}])]
    (is (= [[:start :a] [:start :b]] @log))
    (is (= [:a :b] (ids s)))
    (sup/stop! s)))

(deftest one-for-one-restarts-only-the-failed-child
  (let [log (atom [])
        s (sup/start {:intensity 5} [{:id :a :start (worker log :a)}
                                     {:id :b :start (worker log :b)}])
        b (sup/child s :b)]
    (act/! (sup/child s :a) :die)
    (is (eventually #(= 2 (starts log :a))))
    (is (= 1 (starts log :b)))
    (is (= b (sup/child s :b)))
    (sup/stop! s)))

(deftest one-for-all-stops-the-others-in-reverse-then-restarts-in-order
  (let [log (atom [])
        s (sup/start {:strategy :one-for-all :intensity 5}
                     [{:id :a :start (worker log :a true)}
                      {:id :b :start (worker log :b true)}
                      {:id :c :start (worker log :c true)}])
        old-c (sup/child s :c)]
    (reset! log [])
    (act/! (sup/child s :b) :die)
    (is (eventually #(= 5 (count @log))))
    (is (= [[:stop :c :shutdown] [:stop :a :shutdown]
            [:start :a] [:start :b] [:start :c]]
           @log))
    (is (false? (act/alive? old-c)))
    (sup/stop! s)))

(deftest rest-for-one-restarts-the-failed-and-later
  (let [log (atom [])
        s (sup/start {:strategy :rest-for-one :intensity 5}
                     [{:id :a :start (worker log :a)}
                      {:id :b :start (worker log :b)}
                      {:id :c :start (worker log :c)}])
        old-c (sup/child s :c)]
    (act/! (sup/child s :b) :die)
    (is (eventually #(and (= 2 (starts log :b)) (= 2 (starts log :c)))))
    (is (= 1 (starts log :a)))
    (is (= :shutdown (act/exit-reason old-c 1000)))
    (sup/stop! s)))

(deftest exceeding-the-intensity-shuts-the-supervisor-down
  (let [log (atom [])
        s (sup/start {:intensity 2 :period 60} [{:id :a :start (worker log :a)}])]
    (act/! (sup/child s :a) :die)
    (is (eventually #(= 2 (starts log :a))))
    (act/! (sup/child s :a) :die)
    (is (eventually #(= 3 (starts log :a))))
    (let [last-child (sup/child s :a)]
      (act/! last-child :die)
      (is (= :shutdown (act/exit-reason s 1000))))
    (is (= 3 (starts log :a)))))

(deftest the-default-intensity-is-one-in-five-seconds
  (let [log (atom [])
        s (sup/start {} [{:id :a :start (worker log :a)}])]
    (act/! (sup/child s :a) :die)
    (is (eventually #(= 2 (starts log :a))))
    (act/! (sup/child s :a) :die)
    (is (= :shutdown (act/exit-reason s 1000)))))

(deftest transient-children-restart-only-on-abnormal-exit
  (let [log (atom [])
        s (sup/start {:intensity 5} [{:id :a :start (worker log :a) :restart :transient}])]
    (act/! (sup/child s :a) :die)
    (is (eventually #(= 2 (starts log :a))))
    (act/! (sup/child s :a) :quit)
    (is (eventually #(nil? (sup/child s :a))))
    (is (= [:a] (ids s)))
    (is (= 2 (starts log :a)))
    (sup/stop! s)))

(deftest temporary-children-are-never-restarted-and-are-dropped
  (let [log (atom [])
        s (sup/start {:intensity 5} [{:id :a :start (worker log :a) :restart :temporary}])]
    (act/! (sup/child s :a) :die)
    (is (eventually #(empty? (ids s))))
    (is (= 1 (starts log :a)))
    (sup/stop! s)))

(deftest permanent-children-restart-even-on-a-normal-exit
  (let [log (atom [])
        s (sup/start {:intensity 5} [{:id :a :start (worker log :a)}])]
    (act/! (sup/child s :a) :quit)
    (is (eventually #(= 2 (starts log :a))))
    (sup/stop! s)))

(deftest a-temporary-sibling-is-stopped-but-not-restarted
  (let [log (atom [])
        s (sup/start {:strategy :one-for-all :intensity 5}
                     [{:id :a :start (worker log :a)}
                      {:id :t :start (worker log :t) :restart :temporary}])]
    (act/! (sup/child s :a) :die)
    (is (eventually #(= 2 (starts log :a))))
    (is (= [:a] (ids s)))
    (sup/stop! s)))

(deftest terminate-restart-and-delete-a-child
  (let [log (atom [])
        s (sup/start {} [{:id :a :start (worker log :a true)}])
        a1 (sup/child s :a)]
    (is (= :ok (sup/terminate-child! s :a)))
    (is (= :shutdown (act/exit-reason a1 1000)))
    (is (some #{[:stop :a :shutdown]} @log))
    (is (= [{:id :a :actor nil :type :worker :restart :permanent}] (sup/which-children s)))
    (let [a2 (sup/restart-child! s :a)]
      (is (act/alive? a2))
      (is (thrown? Throwable (sup/delete-child! s :a)))
      (sup/terminate-child! s :a)
      (is (= :ok (sup/delete-child! s :a)))
      (is (= [] (ids s))))
    (sup/stop! s)))

(deftest start-child-rejects-a-duplicate-id
  (let [log (atom [])
        s (sup/start {} [])]
    (is (act/alive? (sup/start-child! s {:id :a :start (worker log :a)})))
    (is (thrown? Throwable (sup/start-child! s {:id :a :start (worker log :a)})))
    (is (= {:specs 1 :active 1 :supervisors 0 :workers 1} (sup/count-children s)))
    (sup/stop! s)))

(deftest a-failing-child-start-fails-the-supervisor-start
  (let [log (atom [])]
    (is (thrown? Throwable
                 (sup/start {} [{:id :a :start (worker log :a true)}
                                {:id :b :start (fn [] (throw (ex-info "no" {})))}])))
    (is (some #{[:stop :a :shutdown]} @log))))

(deftest a-child-that-ignores-shutdown-is-killed-after-its-timeout
  (let [s (sup/start {} [{:id :stubborn :shutdown 50
                          :start (fn [] (act/spawn-link
                                         (fn [] (act/trap-exit!)
                                           (loop [] (receive [_ (recur)])))))}])
        c (sup/child s :stubborn)]
    (sup/terminate-child! s :stubborn)
    (is (= :killed (act/exit-reason c 1000)))
    (sup/stop! s)))

(deftest brutal-kill-kills-at-once
  (let [log (atom [])
        s (sup/start {} [{:id :a :shutdown :brutal-kill :start (worker log :a true)}])
        c (sup/child s :a)]
    (sup/terminate-child! s :a)
    (is (= :killed (act/exit-reason c 1000)))
    (is (not-any? #(= :stop (first %)) @log))
    (sup/stop! s)))

(defrecord Terminator [log id]
  gs/Server
  (init [_] nil)
  (handle-call [_ _ _ st] [:reply :ok st])
  (handle-cast [_ _ st] [:noreply st])
  (handle-info [_ _ st] [:noreply st])
  (handle-timeout [_ st] [:noreply st])
  (terminate [_ reason _] (swap! log conj [id reason])))

(deftest stopping-the-tree-terminates-gen-server-children-in-reverse-order
  (let [log (atom [])
        spec (fn [id] {:id id :start #(gs/start-link (->Terminator log id) {:trap true})})
        s (sup/start {} [(spec :a) (spec :b) (spec :c)])]
    (sup/stop! s)
    (is (= [[:c :shutdown] [:b :shutdown] [:a :shutdown]] @log))
    (is (false? (act/alive? s)))))

(deftest supervisors-nest
  (let [log (atom [])
        inner {:id :inner :type :supervisor
               :start #(sup/start-link {:intensity 0} [{:id :w :start (worker log :w)}])}
        s (sup/start {:intensity 5} [inner])
        inner1 (sup/child s :inner)]
    (act/! (sup/child inner1 :w) :die)
    (is (= :shutdown (act/exit-reason inner1 1000)))
    (is (eventually #(let [i (sup/child s :inner)] (and i (not= i inner1)))))
    (is (= 2 (starts log :w)))
    (sup/stop! s)))

(deftest a-supervisor-dies-with-its-parent-and-takes-the-tree
  (let [log (atom [])
        got (promise)
        parent (act/spawn (fn []
                            (deliver got (sup/start-link {} [{:id :a :start (worker log :a true)}]))
                            (receive [:die (act/exit! :parent-gone)])))
        s @got
        c (sup/child s :a)]
    (act/! parent :die)
    (is (= :parent-gone (act/exit-reason s 1000)))
    (is (= :shutdown (act/exit-reason c 1000)))))

;; --- what a supervisor accepts ----------------------------------------------

(defn- start-error [flags specs]
  (try (sup/start flags specs) nil
       (catch clojure.lang.ExceptionInfo e (:reason (ex-data e)))))

(deftest bad-flags-and-specs-are-refused-before-anything-starts
  (let [log (atom [])]
    (is (= [:supervisor-data [:invalid-strategy :round-robin]]
           (start-error {:strategy :round-robin} [{:id :a :start (worker log :a)}])))
    (is (= [:start-spec [:duplicate-child-name :a]]
           (start-error {} [{:id :a :start (worker log :a)} {:id :a :start (worker log :a)}])))
    (is (= [:start-spec [:invalid-restart-type :sometimes]]
           (start-error {} [{:id :a :start (worker log :a)} {:id :b :start (worker log :b) :restart :sometimes}])))
    (is (empty? @log) "no child started")))

(deftest a-bad-spec-given-later-is-refused
  (let [log (atom [])
        s (sup/start {} [])]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"invalid-shutdown"
                          (sup/start-child! s {:id :a :start (worker log :a) :shutdown -1})))
    (is (empty? (sup/which-children s)))
    (sup/stop! s)))

(deftest get-childspec-has-the-defaults-filled-in
  (let [log (atom [])
        start (worker log :a)
        s (sup/start {} [{:id :a :start start}])]
    (is (= {:id :a :start start :restart :permanent :shutdown 5000 :type :worker :significant false}
           (sup/get-childspec s :a)))
    (sup/stop! s)))

;; --- simple_one_for_one -------------------------------------------------------

(defn- dyn-worker
  "A template start fn: a child started with args n logs [:start n]."
  [log]
  (fn [n]
    (swap! log conj [:start n])
    (act/spawn-link (fn [] (loop [] (receive [:die (act/exit! :boom)] [_ (recur)]))))))

(deftest simple-one-for-one-starts-children-from-its-template
  (let [log (atom [])
        s (sup/start {:strategy :simple-one-for-one :intensity 5} [{:id :tpl :start (dyn-worker log)}])]
    (is (empty? (sup/which-children s)) "it starts nothing itself")
    (let [c1 (sup/start-child! s [1])
          c2 (sup/start-child! s [2])]
      (is (= [[:start 1] [:start 2]] @log))
      (is (= [nil nil] (ids s)) "dynamic children have no id")
      (is (= {:specs 1 :active 2 :supervisors 0 :workers 2} (sup/count-children s)))
      (testing "a child that dies is restarted alone, with its own args"
        (act/! c1 :die)
        (is (eventually #(= 2 (count (filter #{[:start 1]} @log)))))
        (is (= 1 (count (filter #{[:start 2]} @log))))
        (is (some #{c2} (map :actor (sup/which-children s)))))
      (testing "a dynamic child is stopped by its actor, and forgotten"
        (sup/terminate-child! s c2)
        (is (= 1 (count (sup/which-children s)))))
      (testing "a template's children cannot be restarted or deleted by id"
        (is (thrown-with-msg? clojure.lang.ExceptionInfo #"simple-one-for-one" (sup/restart-child! s :tpl)))
        (is (thrown-with-msg? clojure.lang.ExceptionInfo #"simple-one-for-one" (sup/delete-child! s :tpl))))
      (sup/stop! s))))

(deftest simple-one-for-one-takes-one-template
  (is (= [:start-spec [:bad-start-spec []]] (start-error {:strategy :simple-one-for-one} []))))

;; --- auto_shutdown --------------------------------------------------------------

(deftest a-significant-child-ending-shuts-its-supervisor-down
  (let [log (atom [])
        s (sup/start {:auto-shutdown :any-significant}
                     [{:id :a :start (worker log :a) :restart :transient :significant true}
                      {:id :b :start (worker log :b)}])]
    (act/! (sup/child s :a) :quit)
    (is (= :shutdown (act/exit-reason s)))))

(deftest a-significant-child-that-is-restarted-shuts-nothing-down
  (let [log (atom [])
        s (sup/start {:auto-shutdown :any-significant :intensity 5}
                     [{:id :a :start (worker log :a) :restart :transient :significant true}])]
    (act/! (sup/child s :a) :die)
    (is (eventually #(= 2 (starts log :a))))
    (is (act/alive? s))
    (sup/stop! s)))

(deftest all-significant-waits-for-the-last-one
  (let [log (atom [])
        s (sup/start {:auto-shutdown :all-significant}
                     [{:id :a :start (worker log :a) :restart :temporary :significant true}
                      {:id :b :start (worker log :b) :restart :temporary :significant true}])]
    (act/! (sup/child s :a) :quit)
    (sleep 50)
    (is (act/alive? s) "one significant child is still running")
    (act/! (sup/child s :b) :die)
    (is (= :shutdown (act/exit-reason s)))))

(deftest terminating-a-significant-child-does-not-shut-it-down
  (let [log (atom [])
        s (sup/start {:auto-shutdown :any-significant}
                     [{:id :a :start (worker log :a) :restart :transient :significant true}])]
    (sup/terminate-child! s :a)
    (sleep 50)
    (is (act/alive? s) "the supervisor stopped it, so it is no shutdown of its own")
    (sup/stop! s)))

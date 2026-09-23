(ns ensemble.actor-test
  (:require [clojure.core.async :as a]
            [clojure.test :refer [deftest is testing]]
            [ensemble.actor :as act]))

(deftest send-and-receive
  (let [a (act/spawn (fn [] (act/receive [[:hello x] x])))]
    (act/! a [:hello 42])
    (is (= 42 (act/join a)))))

(deftest send-returns-actor
  (let [a (act/spawn (fn [] (act/receive [_ :got])))]
    (is (= a (act/! a :anything)))
    (is (= :got (act/join a)))))

(deftest selective-retains-skipped
  (let [a (act/spawn (fn []
                       [(act/receive [[:b x] x])
                        (act/receive [[:a y] y])]))]
    (act/! a [:a 1])
    (act/! a [:b 2])
    (is (= [2 1] (act/join a)))))

(deftest request-response-uses-self-as-reply-to
  (let [server (act/spawn
                (fn [] (act/receive [[from m] (act/! from [(act/self) (str m "!!!")])])))
        client (act/spawn
                (fn []
                  (act/! server [(act/self) "hi"])
                  (act/receive [[_ r] r])))]
    (is (= "hi!!!" (act/join client)))))

(deftest else-takes-next
  (let [a (act/spawn (fn [] (act/receive [[:stop] :stopped]
                                         [:else :other])))]
    (act/! a [:whatever 1])
    (is (= :other (act/join a)))))

(deftest else-not-needed-when-match
  (let [a (act/spawn (fn [] (act/receive [[:stop] :stopped]
                                         [:else :other])))]
    (act/! a [:stop])
    (is (= :stopped (act/join a)))))

(deftest after-times-out
  (let [a (act/spawn (fn [] (act/receive [[:msg x] x]
                                         [:after 50 :timed-out])))]
    (is (= :timed-out (act/join a)))))

(deftest after-yields-to-a-late-message
  (let [a (act/spawn (fn [] (act/receive [[:msg x] x]
                                         [:after 2000 :timed-out])))]
    (act/! a [:msg :arrived])
    (is (= :arrived (act/join a)))))

(deftest empty-pattern-tuple-matches-empty-message
  (let [a (act/spawn (fn [] (act/receive [[] :empty])))]
    (act/! a [])
    (is (= :empty (act/join a)))))

(deftest state-is-per-actor
  (let [a (act/spawn (fn []
                       (act/set-state! (act/self) 10)
                       (act/receive
                        [[:bump n]
                         (do (act/set-state! (act/self)
                                             (+ (act/state (act/self)) n))
                             (act/state (act/self)))]))
                   {:state 0})]
    (act/! a [:bump 5])
    (is (= 15 (act/join a)))))

(deftest actors-are-isolated
  (let [a (act/spawn (fn [] (act/receive [[:x v] v])))
        b (act/spawn (fn [] (act/receive [[:x v] v])))]
    (act/! a [:x :a-only])
    (act/! b [:x :b-only])
    (is (= :a-only (act/join a)))
    (is (= :b-only (act/join b)))))

(deftest done?-before-and-after
  (let [a (act/spawn (fn [] (act/receive [_ :done])))]
    (is (false? (act/done? a)))
    (act/! a :go)
    (act/join a)
    (is (true? (act/done? a)))))

(deftest join-rethrows
  (let [a (act/spawn (fn [] (throw (ex-info "boom" {}))))]
    (is (thrown? Throwable (act/join a)))))

(deftest register-and-whereis
  (let [a (act/spawn (fn [] (act/receive [_ :ok])) {:name :svc})]
    (is (= a (act/whereis :svc)))
    (act/! (act/whereis :svc) :hi)
    (is (= :ok (act/join a)))))

(deftest spawn-returns-and-body-sees-actor
  (let [a (act/spawn (fn [] (act/self)))]
    (is (= a (act/join a)))))

(deftest bang-variadic-packs-args
  (let [a (act/spawn (fn [] (act/receive [m m])))]
    (act/! a 3 4)
    (is (= [3 4] (act/join a)))))

(deftest bangbang-variadic-packs-args
  (let [a (act/spawn (fn [] (act/receive [m m])))]
    (act/!! a 3 4)
    (is (= [3 4] (act/join a)))))

(deftest receive-timed-returns-the-message
  (let [a (act/spawn (fn [] (act/receive-timed 500)))]
    (act/! a :hello)
    (is (= :hello (act/join a)))))

(deftest receive-timed-nil-on-timeout
  (let [a (act/spawn (fn [] (act/receive-timed 30)))]
    (is (nil? (act/join a)))))

(deftest receive-timed-interleaves-values-and-nil
  (let [a (act/spawn (fn [] [(act/receive-timed 50)
                             (act/receive-timed 50)
                             (act/receive-timed 50)]))]
    (act/! a 1)
    (a/<!! (a/timeout 20))
    (act/! a 2)
    (a/<!! (a/timeout 100))
    (act/! a 3)
    (is (= [1 2 nil] (act/join a)))))

(deftest join-within-timeout-returns-value
  (let [a (act/spawn (fn [] :done))]
    (is (= :done (act/join a 500)))))

(deftest join-times-out
  (let [a (act/spawn (fn [] (act/receive [_ :never])))]
    (is (thrown? Throwable (act/join a 30)))))

(deftest watch-notifies-on-normal-exit
  (let [watched (act/spawn (fn [] :ok))
        watcher (act/spawn (fn []
                             (act/watch! watched)
                             (act/receive [[:exit _ a cause] [a cause]])))]
    (is (= [watched nil] (act/join watcher)))))

(deftest watch-notifies-with-cause-on-abnormal-exit
  (let [watched (act/spawn (fn [] (throw (ex-info "died" {}))))
        watcher (act/spawn (fn []
                             (act/watch! watched)
                             (act/receive [[:exit _ _ cause] (some? cause)])))]
    (is (true? (act/join watcher)))))

(deftest watch-of-dead-actor-still-notifies
  (let [watched (act/spawn (fn [] :ok))
        watcher (act/spawn (fn []
                             (act/join watched)
                             (act/watch! watched)
                             (act/receive [[:exit _ a _] a])))]
    (is (= watched (act/join watcher)))))

(deftest unwatch-suppresses-notification
  (let [watched (act/spawn (fn [] (act/receive [_ :ok])))
        watcher (act/spawn (fn []
                             (let [ref (act/watch! watched)]
                               (act/unwatch! ref)
                               (act/! watched :bye)
                               (act/receive-timed 200))))]
    (is (nil? (act/join watcher)))))

(deftest vref-derefs-its-value
  (let [v (act/vref 42)]
    (is (= 42 @v))))

(deftest maketag-is-a-number
  (is (number? (act/maketag)))
  (is (not= (act/maketag) (act/maketag))))

(deftest register-normalizes-string-and-keyword
  (let [a (act/spawn (fn [] (act/receive [_ :ok])) {:name "svc"})]
    (is (= a (act/whereis :svc)))
    (is (= a (act/whereis "svc")))))

(deftest register-one-arity-registers-the-current-actor
  (let [got (promise)
        a (act/spawn (fn []
                       (act/register! :me)
                       (deliver got (act/whereis :me))
                       (act/receive [_ :ok])))]
    (is (= a @got))))

(deftest unregister-removes-the-actor
  (let [a (act/spawn (fn [] (act/receive [_ :ok])) {:name :gone})]
    (is (= a (act/whereis :gone)))
    (act/unregister! a)
    (is (nil? (act/whereis :gone)))))

(deftest whereis-with-timeout-returns-nil-when-absent
  (is (nil? (act/whereis :nothing-here 20))))

(deftest whereis-with-timeout-finds-a-registered-actor
  (let [a (act/spawn (fn [] (act/receive [_ :ok])) {:name :late})]
    (is (= a (act/whereis :late 100)))))

(deftest mailbox-of-returns-the-mailbox
  (let [a (act/spawn (fn [] (act/receive [_ :ok])))]
    (is (some? (act/mailbox-of a)))))

(deftest register-accepts-a-symbol-name
  (let [a (act/spawn (fn [] (act/receive [_ :ok])) {:name 'sym})]
    (is (= a (act/whereis 'sym)))))

(deftest names-must-be-string-keyword-or-symbol
  (is (thrown? Throwable (act/whereis 42))))

(deftest a-watchers-death-clears-its-watch
  (let [refp (promise)
        long-lived (act/spawn (fn [] (act/receive [_ :never])))
        watcher (act/spawn (fn []
                             (deliver refp (act/watch! long-lived))
                             :bye))]
    (act/join watcher)
    (is (not (contains? @@#'ensemble.actor/watches @refp)))))

(deftest many-watchers-all-notified
  (let [watched (act/spawn (fn [] (act/receive [_ :ok])))
        watchers (doall (for [_ (range 3)]
                          (act/spawn (fn []
                                       (act/watch! watched)
                                       (act/receive [[:exit _ _ _] :notified])))))]
    (act/! watched :go)
    (is (= [:notified :notified :notified] (mapv act/join watchers)))))

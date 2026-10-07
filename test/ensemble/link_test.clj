(ns ensemble.link-test
  (:require [clojure.core.async :as a]
            [clojure.test :refer [deftest is]]
            [ensemble.actor :as act :refer [receive]]))

(defn- sleep [ms] (a/<!! (a/timeout ms)))

(defn- waiter [] (act/spawn (fn [] (receive [:die (act/exit! :boom)] [:quit :ok]))))

(deftest a-normal-exit-leaves-a-linked-actor-alone
  (let [a (waiter)
        b (act/spawn (fn [] (act/link! a) (act/! a :quit) (receive [:ping :alive])))]
    (is (= :normal (act/exit-reason a 1000)))
    (sleep 20)
    (act/! b :ping)
    (is (= :alive (act/join b 1000)))))

(deftest an-abnormal-exit-kills-a-linked-actor-with-the-same-reason
  (let [a (waiter)
        b (act/spawn (fn [] (act/link! a) (act/! a :die) (receive [_ :never])))]
    (is (= :boom (act/exit-reason b 1000)))))

(deftest a-crash-propagates-the-throwable
  (let [e (ex-info "crash" {})
        a (act/spawn (fn [] (receive [_ (throw e)])))
        b (act/spawn (fn [] (act/link! a) (act/! a :go) (receive [_ :never])))]
    (is (= e (act/exit-reason b 1000)))))

(deftest links-cascade
  (let [a (waiter)
        b (act/spawn (fn [] (act/link! a) (receive [_ :never])))
        c (act/spawn (fn [] (act/link! b) (receive [_ :never])))]
    (sleep 20)
    (act/! a :die)
    (is (= :boom (act/exit-reason b 1000)))
    (is (= :boom (act/exit-reason c 1000)))))

(deftest a-trapping-actor-receives-exit-and-lives
  (let [a (waiter)
        b (act/spawn (fn []
                       (act/trap-exit!)
                       (act/link! a)
                       (act/! a :die)
                       (receive [[:EXIT from reason] [(= from a) reason]])))]
    (is (= [true :boom] (act/join b 1000)))))

(deftest a-trapping-actor-receives-a-normal-exit-too
  (let [a (waiter)
        b (act/spawn (fn []
                       (act/trap-exit!)
                       (act/link! a)
                       (act/! a :quit)
                       (receive [[:EXIT _ reason] reason])))]
    (is (= :normal (act/join b 1000)))))

(deftest a-trapper-stops-the-cascade
  (let [a (waiter)
        b (act/spawn (fn [] (act/trap-exit!) (act/link! a) (receive [[:EXIT _ _] (receive [:quit :ok])])))
        c (act/spawn (fn [] (act/link! b) (receive [:quit :ok])))]
    (sleep 20)
    (act/! a :die)
    (sleep 20)
    (is (act/alive? b))
    (is (act/alive? c))
    (act/! b :quit) (act/! c :quit)))

(deftest trap-exit-returns-the-old-flag-and-can-be-turned-off
  (let [a (waiter)
        b (act/spawn (fn []
                       (let [old [(act/trap-exit! true) (act/trap-exit! false)]]
                         (act/link! a)
                         (act/! a :die)
                         (receive [_ old]))))]
    (is (= :boom (act/exit-reason b 1000)))))

(deftest exit-kill-cannot-be-trapped
  (let [b (act/spawn (fn [] (receive [_ :trapped])) {:trap true})]
    (act/exit! b :kill)
    (is (= :killed (act/exit-reason b 1000)))))

(deftest killed-reaches-links-as-a-trappable-reason
  (let [victim (act/spawn (fn [] (receive [_ :never])))
        trapper (act/spawn (fn [] (act/trap-exit!) (act/link! victim)
                             (receive [[:EXIT _ r] r])))
        mortal (act/spawn (fn [] (act/link! victim) (receive [_ :never])))]
    (sleep 20)
    (act/exit! victim :kill)
    (is (= :killed (act/join trapper 1000)))
    (is (= :killed (act/exit-reason mortal 1000)))))

(deftest a-process-dying-with-kill-sends-an-ordinary-kill
  (let [a (act/spawn (fn [] (receive [_ (act/exit! :kill)])))
        b (act/spawn (fn [] (act/trap-exit!) (act/link! a) (act/! a :go)
                       (receive [[:EXIT _ r] r])))]
    (is (= :kill (act/join b 1000)))))

(deftest exit-2-kills-a-non-trapping-actor
  (let [b (act/spawn (fn [] (receive [_ :never])))]
    (sleep 10)
    (act/exit! b :shutdown)
    (is (= :shutdown (act/exit-reason b 1000)))))

(deftest exit-2-normal-is-ignored-by-a-non-trapping-actor
  (let [b (act/spawn (fn [] (receive [:ping :alive])))]
    (sleep 10)
    (act/exit! b :normal)
    (sleep 20)
    (act/! b :ping)
    (is (= :alive (act/join b 1000)))))

(deftest exit-2-reaches-a-trapper-as-a-message-from-the-sender
  ;; trapping from spawn: s may signal before b's body has run a step
  (let [b (act/spawn (fn [] (receive [[:EXIT from r] [from r]])) {:trap true})
        s (act/spawn (fn [] (act/exit! b :bye) :sent))]
    (is (= :sent (act/join s 1000)))
    (is (= [s :bye] (act/join b 1000)))))

(deftest exit-2-normal-to-self-exits
  (let [a (act/spawn (fn [] (act/exit! (act/self) :normal) :kept-running))]
    (is (= :normal (act/exit-reason a 1000)))))

(deftest unlink-stops-the-signal
  (let [a (waiter)
        b (act/spawn (fn [] (act/link! a) (act/unlink! a) (act/! a :die)
                       (receive [:ping :alive] [:after 1000 :timeout])))]
    (is (= :boom (act/exit-reason a 1000)))
    (act/! b :ping)
    (is (= :alive (act/join b 1000)))))

(deftest linking-a-dead-actor-is-noproc
  (let [a (act/spawn (fn [] :done))]
    (act/join a 1000)
    (let [b (act/spawn (fn [] (act/link! a) (receive [_ :never])))
          c (act/spawn (fn [] (act/trap-exit!) (act/link! a) (receive [[:EXIT _ r] r])))]
      (is (= :noproc (act/exit-reason b 1000)))
      (is (= :noproc (act/join c 1000))))))

(deftest spawn-link-links-before-the-body-runs
  (let [parent (act/spawn (fn []
                            (act/trap-exit!)
                            (let [child (act/spawn-link (fn [] (act/exit! :instant)))]
                              (receive [[:EXIT child r] r]))))]
    (is (= :instant (act/join parent 1000)))))

(deftest a-dying-parent-takes-its-spawn-linked-child
  (let [child (promise)
        parent (act/spawn (fn []
                            (deliver child (act/spawn-link (fn [] (receive [_ :never]))))
                            (receive [_ (act/exit! :parent-died)])))]
    (sleep 10)
    (act/! parent :go)
    (is (= :parent-died (act/exit-reason @child 1000)))))

;; --- exit signals land wherever the actor is --------------------------------

(deftest a-busy-actor-dies-of-an-exit-signal
  (let [a (act/spawn (fn [] (loop [i 0] (recur (inc i)))))]
    (sleep 20)
    (act/exit! a :stop)
    (is (= :stop (act/exit-reason a 2000)))))

(deftest an-actor-parked-on-its-own-channel-dies-of-a-link
  (let [b (act/spawn (fn [] (receive [_ (act/exit! :boom)])))
        a (act/spawn (fn [] (act/link! b) (a/<!! (a/chan))))]
    (sleep 20)
    (act/! b :go)
    (is (= :boom (act/exit-reason a 2000)))))

(deftest a-swallowed-kill-still-kills
  (let [a (act/spawn (fn []
                       (try (loop [] (recur)) (catch Throwable _ :swallowed))
                       (receive [_ :still-alive] [:after 50 :still-alive])))]
    (sleep 20)
    (act/exit! a :kill)
    (is (= :killed (act/exit-reason a 2000)))))

(deftest a-trapping-busy-actor-is-not-interrupted
  (let [done (atom false)
        a (act/spawn (fn []
                       (dotimes [_ 2000000] nil)
                       (reset! done true)
                       (receive [[:EXIT _ r] r]))
                     {:trap true})]
    (sleep 5)
    (act/exit! a :boom)
    (is (= :boom (act/join a 5000)))
    (is @done)))

(deftest an-exit-signal-stays-ahead-of-a-later-message
  ;; signals from one actor to another arrive in the order they were sent, as
  ;; in Erlang: the [:EXIT] of an exit! comes before a message sent after it,
  ;; even when the receiver was not in a receive as both arrived
  (let [go (promise)
        r (act/spawn (fn []
                       (act/trap-exit!)
                       @go
                       [(receive [m m]) (receive [m m])]))
        s (act/spawn (fn []
                       (act/exit! r :hello)
                       (act/! r :ping)
                       (deliver go true)
                       (receive [:never nil])))]
    (is (= [[:EXIT s :hello] :ping] (act/join r 1000)))
    (act/exit! s :kill)))

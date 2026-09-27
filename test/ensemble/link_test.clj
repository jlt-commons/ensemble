(ns ensemble.link-test
  (:require [clojure.test :refer [deftest is testing]]
            [ensemble :as e]
            [ensemble.actor :as act]))

(defn- exit-reason-of
  "Join actor and return the exit reason it died with, or :survived if it
  returned normally."
  [actor]
  (try
    (act/join actor)
    :survived
    (catch Throwable t
      (let [d (ex-data t)]
        (if (and (map? d) (contains? d :ensemble.actor/exit))
          (:ensemble.actor/exit d)
          t)))))

(deftest public-reexports-resolve
  (is (fn? e/link!))
  (is (fn? e/unlink!))
  (is (fn? e/trap-exit!))
  (is (fn? e/exit!)))

(deftest link-propagates-a-normal-exit-to-nobody
  (let [bpx (promise)
        a   (act/spawn (fn [] (act/link! @bpx) :ok))
        b   (act/spawn (fn [] (act/link! a)
                         (act/receive [[:ping x] x]
                                      [:after 1000 :timeout])))]
    (deliver bpx b)
    (is (= :ok (act/join a)))
    (act/! b [:ping :alive])
    (is (= :alive (act/join b)))))

(deftest link-kills-the-other-on-abnormal-exit
  (let [a (act/spawn (fn [] (act/receive [_ (act/exit! :boom)])))
        b (act/spawn (fn [] (act/link! a)
                       (act/receive [_ :never])))]
    (act/! a :die)
    (is (= :boom (exit-reason-of b)))))

(deftest link-cascade-reaches-transitive-links
  (let [a (act/spawn (fn [] (act/receive [_ (act/exit! :boom)])))
        b (act/spawn (fn [] (act/link! a)
                       (act/receive [_ :never])))
        c (act/spawn (fn [] (act/link! b)
                       (act/receive [_ :never])))]
    (act/! a :die)
    (is (= :boom (exit-reason-of b)))
    (is (= :boom (exit-reason-of c)))))

(deftest trap-exit-delivers-the-signal-as-a-message
  (let [a (act/spawn (fn [] (act/receive [_ (act/exit! :boom)])))
        b (act/spawn (fn []
                       (act/trap-exit!)
                       (act/link! a)
                       (act/receive [[:EXIT from reason] [from reason]])))]
    (act/! a :die)
    (let [[from reason] (act/join b)]
      (is (= (:pid a) from))
      (is (= :boom reason)))))

(deftest a-trapping-actor-can-unset-trapping
  (let [a (act/spawn (fn [] (act/receive [_ (act/exit! :boom)])))
        b (act/spawn (fn []
                       (act/trap-exit!)
                       (act/trap-exit! false)
                       (act/link! a)
                       (act/receive [_ :never])))]
    (act/! a :die)
    (is (= :boom (exit-reason-of b)))))

(deftest kill-is-never-trappable
  (let [a (act/spawn (fn [] (act/receive [_ (act/exit! :killed)])))
        b (act/spawn (fn []
                       (act/trap-exit!)
                       (act/link! a)
                       (act/receive [[:EXIT _ _] :trapped]
                                    [:after 1000 :timeout])))]
    (act/! a :die)
    (is (= :killed (exit-reason-of b)))))

(deftest unlink-stops-the-signal
  (let [ready (promise)
        a (act/spawn (fn [] (act/receive [_ (act/exit! :boom)])))
        b (act/spawn (fn []
                       (act/link! a)
                       (act/unlink! a)
                       (deliver ready true)
                       (act/receive [[:ping x] x]
                                    [:after 1000 :timeout])))]
    @ready
    (act/! a :die)
    (act/! b [:ping :alive])
    (is (= :alive (act/join b)))))

(deftest linking-a-dead-actor-signals-at-once
  (let [a (act/spawn (fn [] (act/exit! :gone)))]
    (is (= :gone (exit-reason-of a)))
    (let [b (act/spawn (fn [] (act/link! a)
                         (act/receive [_ :never])))]
      (is (= :gone (exit-reason-of b))))))

(deftest exit-normal-is-a-quiet-stop
  (let [a (act/spawn (fn [] (act/exit! :normal)))]
    (is (nil? (act/join a)))
    (is (true? (act/done? a)))))

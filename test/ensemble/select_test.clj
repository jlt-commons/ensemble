(ns ensemble.select-test
  (:require [clojure.test :refer [deftest is]]
            [ensemble.mailbox :as mb]
            [ensemble.pattern :as pat]
            [ensemble.select :as sel]))

(defn- mb-of [xs] (reduce mb/enqueue [:Empty] xs))
(defn- pats [forms] (mapv pat/compile-form forms))

(deftest none-when-empty
  (is (= [:None] (sel/find-first-of [:Empty] (pats ['[:a x]])))))

(deftest none-when-nothing-matches
  (is (= [:None] (sel/find-first-of (mb-of [[:a 1]]) (pats ['[:z x]])))))

(deftest picks-oldest-match-and-keeps-rest
  (let [m (mb-of [[:a 1] [:b 2] [:c 3]])
        r (sel/find-first-of m (pats ['[:b x]]))]
    (is (= :Take (first r)))
    (is (= 0 (nth r 1)))
    (is (= [:b 2] (nth r 2)))
    (is (= [[:a 1] [:c 3]] (vec (mb/msgs (nth r 3)))))))

(deftest clause-order-decides-index
  (let [r (sel/find-first-of (mb-of [[:b 2]]) (pats ['[:a x] '[:b y]]))]
    (is (= 1 (nth r 1)))
    (is (= [:b 2] (nth r 2)))))

(deftest oldest-message-wins-over-clause-order
  (let [r (sel/find-first-of (mb-of [[:b 2] [:a 1]]) (pats ['[:a x] '[:b y]]))]
    (is (= [:b 2] (nth r 2)))
    (is (= 1 (nth r 1)))
    (is (= [[:a 1]] (vec (mb/msgs (nth r 3)))))))

(deftest keeps-skipped-nil-message
  (let [r (sel/find-first-of (mb-of [nil [:b 2] :tail]) (pats ['[:b y]]))]
    (is (= [:b 2] (nth r 2)))
    (is (= [nil :tail] (vec (mb/msgs (nth r 3)))))))

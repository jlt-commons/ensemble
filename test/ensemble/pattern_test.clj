(ns ensemble.pattern-test
  (:require [clojure.test :refer [deftest is]]
            [ensemble.pattern :as pat]
            [ensemble.match :as match]))

(deftest compiles-atoms
  (is (= [:Wild] (pat/compile-form '_)))
  (is (= [:Bind 'x] (pat/compile-form 'x)))
  (is (= [:Lit :stop] (pat/compile-form :stop)))
  (is (= [:Lit 42] (pat/compile-form 42)))
  (is (= [:Lit "s"] (pat/compile-form "s"))))

(deftest compiles-tuples
  (is (= [:Nil] (pat/compile-form [])))
  (is (= [:Cons [:Lit :pair] [:Cons [:Bind 'x] [:Cons [:Bind 'y] [:Nil]]]]
         (pat/compile-form '[:pair x y])))
  (is (= [:Cons [:Lit :data]
                [:Cons [:Cons [:Lit :ok] [:Cons [:Bind 'v] [:Nil]]] [:Nil]]]
         (pat/compile-form '[:data [:ok v]]))))

(deftest compiled-patterns-capture
  (let [p (pat/compile-form '[:pair x y])]
    (is (= {'x 1 'y 2} (match/capture p [:pair 1 2])))
    (is (nil? (match/capture p [:pair 1]))))
  (let [p (pat/compile-form '[:data [:ok v]])]
    (is (= {'v 9} (match/capture p [:data [:ok 9]])))
    (is (nil? (match/capture p [:data [:err 9]])))))

(deftest bound-syms-lists-binders
  (is (= [] (pat/bound-syms '_)))
  (is (= '[x] (pat/bound-syms 'x)))
  (is (= '[x y] (vec (pat/bound-syms '[:pair x y]))))
  (is (= '[v] (vec (pat/bound-syms '[:data [:ok v]])))))

(ns ensemble.timers-proof
  "How ensemble.timers-spec is proved: facts about clojure.core over a
  table of any length, for a cancel of a timer that is not there and a
  look when no timer is due."
  (:require [writ.spec :refer [proof-of lemma hint]]))

(proof-of ensemble.timers-spec)

;; no timer has ref: looking for its deadline finds none
(lemma no-timer-has-the-deadline-of-one-that-is-not-there
  (forall [t (Vec Timer), ref Any]
    (=> (not-any? (fn [x] (case (first x) :Timer (let [[_ r] x] (= ref r)))) t)
        (= (some (fn [x] (case (first x) :Timer (let [[_ r at] x] (when (= r ref) at)))) t) nil))))

;; no timer has ref: a filter on the others keeps them all
(lemma a-filter-that-keeps-every-timer-keeps-the-table
  (forall [t (Vec Timer), ref Any]
    (=> (not-any? (fn [x] (case (first x) :Timer (let [[_ r] x] (= ref r)))) t)
        (= (filter (fn [x] (case (first x) :Timer (let [[_ r] x] (not= r ref)))) t) t))))

;; no timer is due: a look keeps every timer, and fires none
(lemma with-nothing-due-every-timer-waits
  (forall [t (Vec Timer), now Int]
    (=> (not-any? (fn [x] (case (first x) :Timer (let [[_ _ at] x] (<= at now)))) t)
        (= (filter (fn [x] (not (case (first x) :Timer (let [[_ _ at] x] (<= at now))))) t) t))))

(lemma with-nothing-due-no-timer-fires
  (forall [t (Vec Timer), now Int]
    (=> (not-any? (fn [x] (case (first x) :Timer (let [[_ _ at] x] (<= at now)))) t)
        (= (filter (fn [x] (case (first x) :Timer (let [[_ _ at] x] (<= at now)))) t) ()))))

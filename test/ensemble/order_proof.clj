(ns ensemble.order-proof
  "How ensemble.order-spec is proved: two facts about clojure.core that
  relate restart-plan's keep to the docs' filter and map."
  (:require [writ.spec :refer [proof-of lemma hint]]))

(proof-of ensemble.order-spec)

;; the children stopped: the ids of all but the failed one
(lemma keeping-the-others-is-removing-the-failed-one
  (forall [id Nat, xs (Vec (Tuple Nat Keyword))]
    (= (keep (fn [c] (when (not= id (first c)) (first c))) xs)
       (map first (remove #(= id (first %)) xs)))))

;; the children started: the failed one, and all that are not temporary
(lemma keeping-the-restarted-is-filtering-them
  (forall [id Nat, xs (Vec (Tuple Nat Keyword))]
    (= (keep (fn [c] (when (or (= id (first c)) (not= :temporary (second c))) (first c))) xs)
       (for [c xs :when (or (= id (first c)) (not= :temporary (second c)))] (first c)))))

;; once the failed child is removed, no id left is its
(lemma removing-the-failed-one-leaves-no-child-with-its-id
  (forall [id Nat, xs (Vec (Tuple Nat Keyword))]
    (not (some #{id} (map first (remove #(= id (first %)) xs))))))

;; a child with the id is among those the strategy starts, whichever
;; position it is at, and from its own position on
(lemma a-child-with-the-id-is-started
  (forall [xs (Vec (Tuple Nat Keyword)), i Nat]
    (=> (< i (count xs))
        (some #{(first (nth xs i))}
              (for [c xs :when (or (= (first (nth xs i)) (first c)) (not= :temporary (second c)))] (first c))))))

(hint a-child-present-has-an-index {:induct cs :vary [i]})
(hint a-child-with-the-id-is-started {:induct xs :vary [i]})
(hint the-restarted-from-a-child-include-it {:induct xs})

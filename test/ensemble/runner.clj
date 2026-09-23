(ns ensemble.runner
  "Runs ensemble's writ specs and behaviour tests.

  Specs are the files under test/ensemble named *_spec.clj: each is checked
  with writ.spec/check, which runs the static, law and proof gates.  Behaviour
  tests are the *_test.clj files, run under clojure.test -- the actor layer is
  effectful (it spawns fibers and blocks on channels), so it is exercised here
  rather than by a law.

  Discovery is by directory scan, so adding a spec or test file is enough."
  (:require [clojure.test :as t]
            [clojure.string :as str]
            [jolt.fs :as fs]
            [writ.spec :as spec]))

(defn- path->ns
  "test/ensemble/foo_spec.clj -> ensemble.foo-spec."
  [p]
  (-> (str p)
      (str/replace #"^.*?\btest/" "")
      (str/replace #"\.clj$" "")
      (str/replace "_" "-")
      (str/replace "/" ".")
      symbol))

(defn- discovered [suffix]
  (->> (fs/list-dir "test/ensemble" suffix)
       (map path->ns)
       sort))

(defn- run-specs [nses]
  (reduce
   (fn [acc n]
     (print (str "  spec " n " ... ")) (flush)
     (let [r (spec/check n)
           ok (:ok r)]
       (println (if ok "ok" "FAIL"))
       (when-not ok
         (println "    " (:message r))
         (when (seq (:gaps r)) (println "    gaps:" (:gaps r)))
         (when (seq (:rejected r)) (println "    rejected:" (:rejected r))))
       (if ok acc (inc acc))))
   0
   nses))

(defn- run-tests [nses]
  (doseq [n nses] (require n))
  (reduce
   (fn [acc n]
     (print (str "  test " n " ... ")) (flush)
     (let [t0 (System/currentTimeMillis)
           s  (t/run-tests n)]
       (println (str (- (System/currentTimeMillis) t0) "ms"
                     (when (pos? (+ (:fail s 0) (:error s 0)))
                       (str "  <-- " (:fail s 0) " fail " (:error s 0) " error"))))
       (merge-with + (dissoc acc :type) (dissoc s :type))))
   {:test 0 :pass 0 :fail 0 :error 0}
   nses))

(defn -main [& _]
  (let [specs (discovered "*_spec.clj")
        tests (discovered "*_test.clj")
        bad-specs (run-specs specs)
        {:keys [test pass fail error] :or {test 0 pass 0 fail 0 error 0}}
        (run-tests tests)]
    (println (str "\nSpecs: " (count specs) ", failing " bad-specs "."))
    (println (str "Ran " test " tests. " pass " assertions passed, "
                  fail " failures, " error " errors."))
    (flush)
    (System/exit (if (pos? (+ bad-specs fail error)) 1 0))))

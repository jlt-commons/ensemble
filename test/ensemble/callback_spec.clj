(ns ensemble.callback-spec
  "Contract for ensemble.callback: what a gen_server callback's return value
  means, from the gen_server docs.

  handle_call may return {reply, Reply, State}, {reply, Reply, State,
  Timeout}, {noreply, State}, {noreply, State, Timeout}, {stop, Reason,
  Reply, State} or {stop, Reason, State}.  handle_cast, handle_info and the
  timeout callback may return the noreply and {stop, Reason, State} forms
  only.  A Timeout is a non-negative integer or infinity.  Anything else is a
  bad return value, and the server stops with {bad_return_value, Ret}."
  (:require [writ.spec :refer [spec data ann refine graph law calls]]
            [ensemble.callback :as cb]
            [ensemble.gen-server :as gs]))

(spec ensemble.callback {:require :proved})

(data Step
  (Reply Any Any Any)
  (Continue Any Any)
  (Stop Any Bool Any Any)
  (Bad Any))

(refine Kind [k Keyword] (contains? #{:call :other} k))

(ann timeout? [Any -> Bool])
(ann interpret [Kind Any -> Step])

;; --- the state graph ----------------------------------------------------

(refine Replies   [s Step] (= :Reply (first s)))
(refine Continues [s Step] (= :Continue (first s)))
(refine Stops     [s Step] (= :Stop (first s)))
(refine Bad       [s Step] (= :Bad (first s)))

(graph callback
  {:states {:ret Any, :reply Replies, :continue Continues, :stop Stops, :bad Bad}
   :edges  {:ret {[interpret Kind _] #{:reply :continue :stop :bad}}}})

;; --- timeouts -----------------------------------------------------------

(law timeouts
  (and (timeout? nil) (timeout? :infinity) (timeout? 0) (timeout? 5000)
       (not (timeout? -1)) (not (timeout? :forever)) (not (timeout? "10"))))

(law a-non-negative-integer-is-a-timeout
  (forall [n Nat] (timeout? n)))

(law a-negative-integer-is-not
  (forall [n Nat] (not (timeout? (- -1 n)))))

;; --- handle_call's returns ----------------------------------------------

(law reply
  (forall [r Any, st Any] (= (interpret :call [:reply r st]) [:Reply r st nil])))

(law reply-with-a-timeout
  (forall [r Any, st Any, t Nat] (= (interpret :call [:reply r st t]) [:Reply r st t])))

(law infinity-is-no-timeout
  (forall [r Any, st Any] (= (interpret :call [:reply r st :infinity]) [:Reply r st nil])))

(law stop-with-a-reply
  (forall [why Any, r Any, st Any] (= (interpret :call [:stop why r st]) [:Stop why true r st])))

;; --- the returns every callback may give --------------------------------

(law noreply
  (forall [k Kind, st Any] (= (interpret k [:noreply st]) [:Continue st nil])))

(law noreply-with-a-timeout
  (forall [k Kind, st Any, t Nat] (= (interpret k [:noreply st t]) [:Continue st t])))

(law stop
  (forall [k Kind, why Any, st Any] (= (interpret k [:stop why st]) [:Stop why false nil st])))

;; --- and what is bad ----------------------------------------------------

(law only-a-call-can-reply
  (forall [r Any, st Any] (= (interpret :other [:reply r st]) [:Bad [:reply r st]])))

(law only-a-call-can-stop-with-a-reply
  (forall [why Any, r Any, st Any]
    (= (interpret :other [:stop why r st]) [:Bad [:stop why r st]])))

(law a-bad-timeout-is-bad
  (forall [k Kind, st Any] (= (interpret k [:noreply st :soon]) [:Bad [:noreply st :soon]])))

(law a-non-tagged-value-is-bad
  (forall [k Kind, n Int] (= (interpret k n) [:Bad n])))

(law wrong-arities-are-bad
  (forall [k Kind, x Any]
    (and (= (interpret k [:noreply]) [:Bad [:noreply]])
         (= (interpret k [:stop x]) [:Bad [:stop x]])
         (= (interpret :call [:reply x]) [:Bad [:reply x]]))))

;; --- the server reads every return through interpret ---------------------

(calls gs/dispatch {:through [cb/interpret]})

(ns ensemble.callback-spec
  "Contract for ensemble.callback: what a gen_server callback's return value
  means, from the gen_server docs.

  init returns {ok, State} or {ok, State, Action}: initialization succeeded.
  {stop, Reason}: it failed, and the process exits with Reason.  {error,
  Reason} or ignore: it failed, and the process exits with normal.  start
  answers {error, Reason} for {stop, Reason}, {error, Reason} or a crash,
  and ignore for ignore.  Anything else is {bad_return_value, Ret}.

  handle_call may return {reply, Reply, State}, {reply, Reply, State,
  Action}, {noreply, State}, {noreply, State, Action}, {stop, Reason,
  Reply, State} or {stop, Reason, State}.  handle_cast, handle_info,
  handle_continue and the timeout callback may return the noreply and
  {stop, Reason, State} forms only.  Anything else is a bad return value,
  and the server stops with {bad_return_value, Ret}.

  An Action is a Timeout (a non-negative integer or infinity), hibernate,
  or {continue, Continue}: handle_continue(Continue, State) runs
  immediately after the callback, before another message is taken."
  (:require [writ.spec :refer [spec data ann refine graph law calls]]
            [ensemble.callback :as cb]
            [ensemble.gen-server :as gs]))

(spec ensemble.callback {:require :proved})

;; what the server does once a callback has returned: wait for a message
;; (with a timeout, or none), hibernate, or run handle_continue
(data Next (Wait Any) (Hibernate) (Cont Any))

(data Init
  (Start Any Next)   ; run with this state
  (Ignore)           ; start answers ignore, the process exits normal
  (Fail Any Any))    ; start answers {error, Reason}; the process exits with the second

(data Step
  (Reply Any Any Next)
  (NoReply Any Next)
  (Stop Any Bool Any Any)
  (Bad Any))

(refine Kind [k Keyword] (contains? #{:call :other} k))

(ann timeout? [Any -> Bool])
(ann interpret-init [Any -> Init])
(ann interpret [Kind Any -> Step])

;; --- the state graph ----------------------------------------------------

(refine Replies   [s Step] (= :Reply (first s)))
(refine NoReplies [s Step] (= :NoReply (first s)))
(refine Stops     [s Step] (= :Stop (first s)))
(refine Bad       [s Step] (= :Bad (first s)))

(graph callback
  {:states {:ret Any, :reply Replies, :noreply NoReplies, :stop Stops, :bad Bad}
   :edges  {:ret {[interpret Kind _] #{:reply :noreply :stop :bad}}}
   ;; a reply is a handle-call's alone: generated returns rarely pair one
   ;; with :call
   :witnesses {[:ret :reply] [[:reply 1 2] :call]}})

(refine Starts  [i Init] (= :Start (first i)))
(refine Ignores [i Init] (= :Ignore (first i)))
(refine Fails   [i Init] (= :Fail (first i)))

(graph init
  {:states {:ret Any, :start Starts, :ignore Ignores, :fail Fails}
   :edges  {:ret {[interpret-init _] #{:start :ignore :fail}}}
   ;; a generated return is seldom :ignore or an [:ok state]
   :witnesses {[:ret :start] [[:ok 1]], [:ret :ignore] [:ignore]}})

;; --- timeouts -----------------------------------------------------------

(law timeouts
  (and (timeout? nil) (timeout? :infinity) (timeout? 0) (timeout? 5000)
       (not (timeout? -1)) (not (timeout? :forever)) (not (timeout? "10"))))

(law a-non-negative-integer-is-a-timeout
  (forall [n Nat] (timeout? n)))

(law a-negative-integer-is-not
  (forall [n Nat] (not (timeout? (- -1 n)))))

;; --- init ------------------------------------------------------------------

(law init-ok
  (forall [st Any] (= (interpret-init [:ok st]) [:Start st [:Wait nil]])))

(law init-ok-with-a-timeout
  (forall [st Any, t Nat] (= (interpret-init [:ok st t]) [:Start st [:Wait t]])))

(law init-ok-with-infinity-waits-for-ever
  (forall [st Any] (= (interpret-init [:ok st :infinity]) [:Start st [:Wait nil]])))

(law init-ok-and-hibernate
  (forall [st Any] (= (interpret-init [:ok st :hibernate]) [:Start st [:Hibernate]])))

(law init-ok-and-continue
  (forall [st Any, c Any] (= (interpret-init [:ok st [:continue c]]) [:Start st [:Cont c]])))

(law init-ignore-exits-normal
  (= (interpret-init :ignore) [:Ignore]))

(law init-error-fails-and-exits-normal
  (forall [why Any] (= (interpret-init [:error why]) [:Fail why :normal])))

(law init-stop-fails-and-exits-with-its-reason
  (forall [why Any] (= (interpret-init [:stop why]) [:Fail why why])))

(law a-bare-state-is-a-bad-init
  (forall [n Int] (= (interpret-init n) [:Fail [:bad-return-value n] [:bad-return-value n]])))

(law a-bad-init-action-is-bad
  (forall [st Any]
    (= (interpret-init [:ok st :soon]) [:Fail [:bad-return-value [:ok st :soon]] [:bad-return-value [:ok st :soon]]])))

(law wrong-init-arities-are-bad
  (forall [x Any]
    (and (= (interpret-init [:ok]) [:Fail [:bad-return-value [:ok]] [:bad-return-value [:ok]]])
         (= (interpret-init [:stop]) [:Fail [:bad-return-value [:stop]] [:bad-return-value [:stop]]])
         (= (interpret-init [:ignore x]) [:Fail [:bad-return-value [:ignore x]] [:bad-return-value [:ignore x]]]))))

;; --- handle_call's returns ----------------------------------------------

(law reply
  (forall [r Any, st Any] (= (interpret :call [:reply r st]) [:Reply r st [:Wait nil]])))

(law reply-with-a-timeout
  (forall [r Any, st Any, t Nat] (= (interpret :call [:reply r st t]) [:Reply r st [:Wait t]])))

(law infinity-is-no-timeout
  (forall [r Any, st Any] (= (interpret :call [:reply r st :infinity]) [:Reply r st [:Wait nil]])))

(law stop-with-a-reply
  (forall [why Any, r Any, st Any] (= (interpret :call [:stop why r st]) [:Stop why true r st])))

;; the Action of a reply
(law reply-and-hibernate
  (forall [r Any, st Any] (= (interpret :call [:reply r st :hibernate]) [:Reply r st [:Hibernate]])))

(law reply-and-continue
  (forall [r Any, st Any, c Any] (= (interpret :call [:reply r st [:continue c]]) [:Reply r st [:Cont c]])))

;; --- the returns every callback may give --------------------------------

(law noreply
  (forall [k Kind, st Any] (= (interpret k [:noreply st]) [:NoReply st [:Wait nil]])))

(law noreply-with-a-timeout
  (forall [k Kind, st Any, t Nat] (= (interpret k [:noreply st t]) [:NoReply st [:Wait t]])))

(law stop
  (forall [k Kind, why Any, st Any] (= (interpret k [:stop why st]) [:Stop why false nil st])))

(law noreply-and-hibernate
  (forall [k Kind, st Any] (= (interpret k [:noreply st :hibernate]) [:NoReply st [:Hibernate]])))

(law noreply-and-continue
  (forall [k Kind, st Any, c Any] (= (interpret k [:noreply st [:continue c]]) [:NoReply st [:Cont c]])))

;; --- and what is bad ----------------------------------------------------

(law only-a-call-can-reply
  (forall [r Any, st Any] (= (interpret :other [:reply r st]) [:Bad [:reply r st]])))

(law only-a-call-can-stop-with-a-reply
  (forall [why Any, r Any, st Any]
    (= (interpret :other [:stop why r st]) [:Bad [:stop why r st]])))

(law a-bad-action-is-bad
  (forall [k Kind, st Any]
    (and (= (interpret k [:noreply st :soon]) [:Bad [:noreply st :soon]])
         (= (interpret k [:noreply st [:continue]]) [:Bad [:noreply st [:continue]]]))))

(law a-non-tagged-value-is-bad
  (forall [k Kind, n Int] (= (interpret k n) [:Bad n])))

(law wrong-arities-are-bad
  (forall [k Kind, x Any]
    (and (= (interpret k [:noreply]) [:Bad [:noreply]])
         (= (interpret k [:stop x]) [:Bad [:stop x]])
         (= (interpret :call [:reply x]) [:Bad [:reply x]]))))

;; --- the server reads every return through interpret ---------------------

(calls gs/dispatch {:through [cb/interpret]})
(calls gs/run {:through [cb/interpret-init]})
;; a :hibernate action, or an idle hibernate-after, hibernates the process
(calls gs/run-loop {:through [ensemble.actor/hibernate! ensemble.actor/passivate!]})

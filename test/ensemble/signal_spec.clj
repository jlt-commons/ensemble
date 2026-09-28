(ns ensemble.signal-spec
  "Contract for ensemble.signal: what a process does with an exit signal,
  following the Erlang reference manual's rules for receiving exit signals.

  - An :exit signal (exit/2) with reason :kill kills the receiver with reason
    :killed, trapping or not.  That is the only untrappable signal.
  - Otherwise a trapping receiver turns the signal into an
    [:EXIT from reason] message and lives.
  - A non-trapping receiver dies with the reason, unless it is :normal.
  - A :normal signal is ignored by a non-trapping receiver -- except one it
    sent itself with exit/2, which exits it.
  - A link signal carrying :kill (a process that died with reason kill) is
    not special: it is an ordinary abnormal reason.

  And the reasons OTP treats as an orderly stop: :normal, :shutdown and
  [:shutdown term]."
  (:require [writ.spec :refer [spec data ann refine graph law calls]]
            [ensemble.actor :as act]
            [ensemble.signal :as sig]))

(spec ensemble.signal {:require :proved})

(data Action (Ignore) (Deliver Any) (Die Any))

(refine Kind [k Keyword] (contains? #{:link :exit} k))

(ann normal? [Any -> Bool])
(ann on-signal [Bool Kind Any Bool -> Action])
(ann shutdown? [Any -> Bool])

;; --- the state graph ----------------------------------------------------

(refine Dies     [a Action] (= :Die (first a)))
(refine Delivers [a Action] (= :Deliver (first a)))
(refine Ignores  [a Action] (= :Ignore (first a)))

;; a signal's reason, as it reaches a process, decides one of three fates
(graph exit-signal
  {:start  [:reason :boom]
   :states {:reason Any, :die Dies, :deliver Delivers, :ignore Ignores}
   :edges  {:reason {[on-signal Bool Kind _ Bool] #{:die :deliver :ignore}}}})

;; --- the rules, one by one ----------------------------------------------

(law exit-kill-is-untrappable
  (forall [t Bool, s Bool] (= (on-signal t :exit :kill s) [:Die :killed])))

(law a-trapper-receives-every-other-signal
  (forall [k Kind, r Any, s Bool]
    (=> (not (and (= :exit k) (= :kill r)))
        (= (on-signal true k r s) [:Deliver r]))))

(law a-non-trapper-dies-with-an-abnormal-reason
  (forall [k Kind, r Any, s Bool]
    (=> (and (not= :normal r) (not (and (= :exit k) (= :kill r))))
        (= (on-signal false k r s) [:Die r]))))

(law a-non-trapper-ignores-normal-from-a-link
  (forall [s Bool] (= (on-signal false :link :normal s) [:Ignore])))

(law a-non-trapper-ignores-normal-from-another
  (= (on-signal false :exit :normal false) [:Ignore]))

(law normal-sent-to-self-exits
  (= (on-signal false :exit :normal true) [:Die :normal]))

(law a-link-kill-is-ordinary
  (and (= (on-signal true :link :kill false) [:Deliver :kill])
       (= (on-signal false :link :kill false) [:Die :kill])))

(law killed-is-trappable
  (forall [k Kind, s Bool] (= (on-signal true k :killed s) [:Deliver :killed])))

;; --- the reasons --------------------------------------------------------

(law only-normal-is-normal
  (forall [r Any] (= (normal? r) (= :normal r))))

(law orderly-stops
  (and (shutdown? :normal) (shutdown? :shutdown) (shutdown? [:shutdown :why])
       (not (shutdown? :boom)) (not (shutdown? :killed))
       (not (shutdown? [:shutdown])) (not (shutdown? [:other :why]))))

(law shutdown-is-exactly-the-orderly-reasons
  (forall [r Any]
    (= (shutdown? r)
       (or (= :normal r) (= :shutdown r)
           (and (vector? r) (= 2 (count r)) (= :shutdown (first r)))))))

;; --- the runtime acts through on-signal ---------------------------------

(calls act/handle-signal! {:through [sig/on-signal]})
(calls act/receive-match {:through [act/drain-signals! sig/on-signal]})
(calls act/link! {:through [act/drain-signals!]})
(calls act/exit! {:through [act/drain-signals!]})

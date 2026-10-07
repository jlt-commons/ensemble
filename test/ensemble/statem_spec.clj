(ns ensemble.statem-spec
  "Contract for ensemble.statem: the pure transition rules of OTP's
  gen_statem, from its docs.  The gen-statem runtime makes every decision
  here.

  init returns {ok, State, Data} or {ok, State, Data, Actions}: the
  machine starts in State.  {stop, Reason}: start fails with Reason, and
  the process exits with it.  {error, Reason} or ignore: start answers
  {error, Reason} or ignore, and the process exits normal.  Anything else
  is {bad_return_value, Ret}.

  A callback returns one of

      [:next-state s d]        [:next-state s d actions]
      [:keep-state d]          [:keep-state d actions]
      [:keep-state-and-data]   [:keep-state-and-data actions]
      [:stop reason]           [:stop reason d]
      [:stop-and-reply reason replies]   [:stop-and-reply reason replies d]

  and anything else is a bad return value.  A state enter call may not
  change the state, postpone, or insert events.

  Actions: :postpone, [:next-event type content], [:reply from value],
  [:timeout ms content] (the event timeout), [:state-timeout ms content],
  [:generic-timeout name ms content].  A time of nil or :infinity cancels.
  :hibernate or [:hibernate bool]: hibernate before waiting for the next
  event; of several, the last decides.

  The event queue:
  - A postponed event is kept; while the state stays the same it is not
    retried.  When the state changes, every postponed event is retried,
    oldest first, before any event not yet handled.
  - Events inserted with :next-event are handled before any other event,
    including retried postponed ones, in the order the actions list them.

  Timeouts:
  - The event timeout is cancelled by any event: a transition clears it,
    and only a :timeout action of that transition sets it again.
  - A state timeout is cancelled when the state changes, unless the same
    transition sets a new one.  An action for it replaces the old one.
  - A generic timeout, per name, lives until it fires or an action for that
    name replaces or cancels it."
  (:require [writ.spec :refer [spec data ann refine graph law calls]]))

(spec ensemble.statem {:require :proved})

(data Result
  (Transition Any Any (Vec Any))
  (Stop Any (Vec Any) Any)
  (Bad Any))

(data Init
  (Start Any Any (Vec Any))
  (Ignore)
  (Fail Any Any))

(data Actions (Actions Bool (Vec Any) (Vec Any) (Vec Any)))

(data Queues (Queues (Vec Any) (Vec Any)))

(refine Kind [k Keyword] (contains? #{:event :enter} k))

(ann init-of [Any -> Init])
(ann result-of [Kind Any Any Any -> Result])
(ann actions-of [(Vec Any) -> Actions])
(ann hibernate? [(Vec Any) -> Bool])
(ann next-queues [Bool Bool Any (Vec Any) (Vec Any) (Vec Any) -> Queues])
(ann next-timers [Bool (Map Any Any) (Vec Any) -> (Map Any Any)])

;; --- the state graph ----------------------------------------------------

(refine Transitions [r Result] (= :Transition (first r)))
(refine Stops       [r Result] (= :Stop (first r)))
(refine Bad         [r Result] (= :Bad (first r)))

;; a callback's return is a transition, a stop or a bad value; a transition's
;; actions split into what the runtime does; the queues and timers move on
(graph transition
  {:states {:ret Any, :transition Transitions, :stop Stops, :bad Bad,
            :actions (Vec Any), :parsed Actions}
   :edges  {:ret     {[result-of Kind _ Any Any] #{:transition :stop :bad}}
            :actions {[actions-of] #{:parsed}}}
   :tested {:ret "a return's actions may be a list of any length, which needs induction"
            :actions "an actions list of unknown length needs induction"}})

;; --- the spec's vocabulary ----------------------------------------------

(def ev [:cast :e])
(def from {:alias :caller})

(defn model-queues
  "The docs' queue rules."
  [changed? postpone? event postponed inserted queue]
  (let [kept (if postpone? (conj (vec postponed) event) (vec postponed))]
    (if changed?
      [:Queues [] (vec (concat inserted kept queue))]
      [:Queues kept (vec (concat inserted queue))])))

;; --- callback results ---------------------------------------------------

(law next-state
  (forall [s Any, d Any]
    (and (= (result-of :event [:next-state s d] :old :d0) [:Transition s d []])
         (= (result-of :event [:next-state s d [:postpone]] :old :d0) [:Transition s d [:postpone]]))))

(law keep-state-keeps-the-state
  (forall [st Any, d Any]
    (and (= (result-of :event [:keep-state d] st :d0) [:Transition st d []])
         (= (result-of :event [:keep-state-and-data] st d) [:Transition st d []])
         (= (result-of :event [:keep-state-and-data [[:reply from 1]]] st d)
            [:Transition st d [[:reply from 1]]]))))

(law stop-forms
  (forall [why Any, d Any]
    (and (= (result-of :event [:stop why] :s d) [:Stop why [] d])
         (= (result-of :event [:stop why :d1] :s d) [:Stop why [] :d1])
         (= (result-of :event [:stop-and-reply why [[:reply from 1]]] :s d)
            [:Stop why [[:reply from 1]] d])
         (= (result-of :event [:stop-and-reply why [] :d1] :s d) [:Stop why [] :d1]))))

(law anything-else-is-bad
  (and (= (result-of :event :oops :s :d) [:Bad :oops])
       (= (result-of :event [:next-state :s] :s :d) [:Bad [:next-state :s]])
       (= (result-of :event [:keep-state :d :not-actions] :s :d) [:Bad [:keep-state :d :not-actions]])))

(law an-enter-call-cannot-change-state
  (= (result-of :enter [:next-state :other :d] :s :d) [:Bad [:next-state :other :d]]))

(law an-enter-call-may-name-its-own-state
  (= (result-of :enter [:next-state :s :d1] :s :d) [:Transition :s :d1 []]))

(law an-enter-call-cannot-postpone-or-insert
  (and (= (first (result-of :enter [:keep-state :d [:postpone]] :s :d)) :Bad)
       (= (first (result-of :enter [:keep-state :d [[:next-event :internal 1]]] :s :d)) :Bad)))

;; --- actions ------------------------------------------------------------

(law no-actions (= (actions-of []) [:Actions false [] [] []]))

(law hibernate-is-an-action-of-its-own
  (and (= (actions-of [:hibernate]) [:Actions false [] [] []])
       (= (actions-of [[:hibernate true]]) [:Actions false [] [] []])))

(law no-hibernate-action-does-not-hibernate
  (forall [ms Nat, c Any] (not (hibernate? [[:timeout ms c] :postpone]))))

(law a-hibernate-action-hibernates
  (forall [ms Nat, c Any]
    (and (hibernate? [:hibernate])
         (hibernate? [[:timeout ms c] [:hibernate true]]))))

(law the-last-hibernate-action-decides
  (and (not (hibernate? [:hibernate [:hibernate false]]))
       (hibernate? [[:hibernate false] :hibernate])))

(law actions-split-in-order
  (= (actions-of [[:next-event :internal 1] [:reply from :r] :postpone
                  [:next-event :cast 2] [:state-timeout 100 :st]])
     [:Actions true [[:internal 1] [:cast 2]] [[from :r]] [[:state 100 :st]]]))

(law each-timeout-kind
  (= (actions-of [[:timeout 5 :a] [:state-timeout 6 :b] [:generic-timeout :g 7 :c]])
     [:Actions false [] [] [[:event 5 :a] [:state 6 :b] [[:generic :g] 7 :c]]]))

(law infinity-is-a-cancel
  (= (actions-of [[:timeout :infinity :a]]) [:Actions false [] [] [[:event nil :a]]]))

;; --- the queue ----------------------------------------------------------

(law same-state-keeps-postponed-events-waiting
  (= (next-queues false true ev [[:cast :p]] [] [[:cast :q]])
     [:Queues [[:cast :p] ev] [[:cast :q]]]))

(law a-state-change-retries-postponed-events-first-oldest-first
  (= (next-queues true false ev [[:cast :p1] [:cast :p2]] [] [[:cast :q]])
     [:Queues [] [[:cast :p1] [:cast :p2] [:cast :q]]]))

(law an-event-postponed-by-a-state-change-is-retried-too
  (= (next-queues true true ev [[:cast :p]] [] [[:cast :q]])
     [:Queues [] [[:cast :p] ev [:cast :q]]]))

(law inserted-events-come-before-everything
  (= (next-queues true false ev [[:cast :p]] [[:internal 1] [:internal 2]] [[:cast :q]])
     [:Queues [] [[:internal 1] [:internal 2] [:cast :p] [:cast :q]]]))

(law next-queues-is-the-docs
  (forall [changed Bool, postpone Bool, e Any, postponed (Vec Any), inserted (Vec Any), queue (Vec Any)]
    (= (next-queues changed postpone e postponed inserted queue)
       (model-queues changed postpone e postponed inserted queue))))

(defn occurrences [x xs] (count (filter #(= x %) xs)))

;; the queues after hold what was there before and the postponed event,
;; each as often: every value occurs in them as many times
(law no-event-is-lost-or-duplicated
  (forall [changed Bool, postpone Bool, e Any, postponed (Vec Any), inserted (Vec Any), queue (Vec Any), x Any]
    (let [[_ p q] (next-queues changed postpone e postponed inserted queue)]
      (= (occurrences x (concat p q))
         (occurrences x (concat postponed inserted queue (if postpone [e] [])))))))

;; --- timers -------------------------------------------------------------

(law a-transition-clears-the-event-timeout
  (forall [changed Bool]
    (= (next-timers changed {:event [5 :a]} []) {})))

(law a-timeout-action-sets-the-event-timeout
  (forall [changed Bool]
    (= (next-timers changed {} [[:event 5 :a]]) {:event [5 :a]})))

(law a-state-change-cancels-the-state-timeout
  (= (next-timers true {:state [5 :a]} []) {}))

(law the-same-state-keeps-it
  (= (next-timers false {:state [5 :a]} []) {:state [5 :a]}))

(law a-new-state-timeout-survives-its-own-state-change
  (= (next-timers true {:state [5 :a]} [[:state 9 :b]]) {:state [9 :b]}))

(law generic-timeouts-survive-state-changes
  (forall [changed Bool]
    (= (next-timers changed {[:generic :g] [5 :a]} []) {[:generic :g] [5 :a]})))

(law a-cancel-removes-the-timer
  (forall [changed Bool]
    (and (= (next-timers changed {[:generic :g] [5 :a]} [[[:generic :g] nil :x]]) {})
         (= (next-timers false {:state [5 :a]} [[:state nil :x]]) {}))))

(law the-last-action-for-a-timer-wins
  (= (next-timers false {} [[:state 1 :a] [:state 2 :b]]) {:state [2 :b]}))

;; --- init --------------------------------------------------------------------

(refine Starts  [i Init] (= :Start (first i)))
(refine Ignores [i Init] (= :Ignore (first i)))
(refine Fails   [i Init] (= :Fail (first i)))

(graph init
  {:states {:ret Any, :start Starts, :ignore Ignores, :fail Fails}
   :edges  {:ret {[init-of _] #{:start :ignore :fail}}}
   ;; a generated return is seldom :ignore or an [:ok state data]
   :witnesses {[:ret :start] [[:ok 1 2]], [:ret :ignore] [:ignore]}})

(law init-ok-starts-in-the-state
  (forall [st Any, d Any] (= (init-of [:ok st d]) [:Start st d []])))

(law init-ok-with-actions
  (forall [st Any, d Any, as (Vec Any)] (= (init-of [:ok st d as]) [:Start st d as])))

(law init-ignore
  (= (init-of :ignore) [:Ignore]))

(law init-error-exits-normal
  (forall [why Any] (= (init-of [:error why]) [:Fail why :normal])))

(law init-stop-exits-with-its-reason
  (forall [why Any] (= (init-of [:stop why]) [:Fail why why])))

(law a-bare-state-and-data-is-a-bad-init
  (forall [st Any, d Any]
    (=> (not (contains? #{:ok :stop :error} st))
        (= (init-of [st d]) [:Fail [:bad-return-value [st d]] [:bad-return-value [st d]]]))))

(law init-actions-must-be-a-list
  (forall [st Any, d Any, k Keyword]
    (= (init-of [:ok st d k]) [:Fail [:bad-return-value [:ok st d k]] [:bad-return-value [:ok st d k]]])))

;; --- the runtime decides through these ----------------------------------

(calls ensemble.gen-statem/run {:through [ensemble.statem/init-of]})


(calls ensemble.gen-statem/transition!
  {:through [ensemble.statem/result-of ensemble.statem/actions-of ensemble.statem/hibernate?
             ensemble.statem/next-queues ensemble.statem/next-timers]})

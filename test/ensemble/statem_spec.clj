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
      [:repeat-state d]        [:repeat-state d actions]
      [:repeat-state-and-data] [:repeat-state-and-data actions]
      [:stop reason]           [:stop reason d]
      [:stop-and-reply reason replies]   [:stop-and-reply reason replies d]

  and anything else stops the machine with {bad_return_from_state_function,
  Ret}.  A repeat keeps the state as keep does, and runs the state enter
  call again as if the state had been entered anew: [:Repeat s d actions].  A state enter call may not change the state
  ({bad_state_enter_return_from_state_function, Ret}), postpone, or insert
  events ({bad_state_enter_action_from_state_function, Action}).

  Actions: :postpone or [:postpone bool], [:next-event type content],
  [:reply from value], [:timeout ms content] (the event timeout), a bare
  time ms (short for [:timeout ms ms]), [:state-timeout ms content],
  [:generic-timeout name ms content].  A time of nil or :infinity cancels,
  as does [:timeout :cancel], [:state-timeout :cancel] or
  [:generic-timeout name :cancel].  [:timeout :update content] (and the
  same for the others) changes a running timer's content and leaves its
  time; with no such timer it starts one of time 0.  An options map after
  the content, {:abs true}, makes the time an absolute deadline on the
  monotonic clock, kept as [:abs t].
  :hibernate or [:hibernate bool]: hibernate before waiting for the next
  event; of several, the last decides.  Where a list of actions goes, one
  action alone may go instead, as OTP's [action()] | action().  An action
  that is none of these stops the machine with
  {bad_action_from_state_function, Action}; a reply of stop-and-reply that
  is not a reply, with {bad_reply_action_from_state_function, Action}.

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
  (Repeat Any Any (Vec Any))
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
(ann first-of [(Set Keyword) (List Any) -> (Opt (Vec Any))])
(ann next-queues [Bool Bool Any (Vec Any) (Vec Any) (Vec Any) -> Queues])
(ann next-timers [Bool (Map Any Any) (Vec Any) -> (Map Any Any)])
(ann restarted [Bool (Map Any Any) (Vec Any) -> (Set Any)])
(ann enter-timers [(Map Any Any) (Vec Any) -> (Map Any Any)])
(ann enter-restarted [(Map Any Any) (Vec Any) -> (Set Any)])

;; --- the state graph ----------------------------------------------------

(refine Transitions [r Result] (contains? #{:Transition :Repeat} (first r)))
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
            :actions "an actions list of unknown length needs induction"}
   ;; a generated return is seldom a well-formed stop
   :witnesses {[:ret :stop] [[:stop :why] :event :s :d]}})

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

(law repeat-keeps-the-state
  (forall [st Any, d Any, ms Nat]
    (and (= (result-of :event [:repeat-state d] st :d0) [:Repeat st d []])
         (= (result-of :event [:repeat-state d [ms]] st :d0) [:Repeat st d [ms]])
         (= (result-of :event [:repeat-state-and-data] st d) [:Repeat st d []])
         (= (result-of :event [:repeat-state-and-data :postpone] st d) [:Repeat st d [:postpone]]))))

(law an-enter-call-may-repeat
  (= (result-of :enter [:repeat-state :d1] :s :d) [:Repeat :s :d1 []]))

(law a-repeat-takes-only-actions
  (and (= (result-of :event [:repeat-state :d [:bogus]] :s :d)
          [:Bad [:bad-action-from-state-function :bogus]])
       (= (result-of :event [:repeat-state] :s :d)
          [:Bad [:bad-return-from-state-function [:repeat-state]]])
       (= (result-of :enter [:repeat-state-and-data :postpone] :s :d)
          [:Bad [:bad-state-enter-action-from-state-function :postpone]])))

(law stop-forms
  (forall [why Any, d Any]
    (and (= (result-of :event [:stop why] :s d) [:Stop why [] d])
         (= (result-of :event [:stop-and-reply why [:reply from 1]] :s d)
            [:Stop why [[:reply from 1]] d])
         (= (result-of :event [:stop why :d1] :s d) [:Stop why [] :d1])
         (= (result-of :event [:stop-and-reply why [[:reply from 1]]] :s d)
            [:Stop why [[:reply from 1]] d])
         (= (result-of :event [:stop-and-reply why [] :d1] :s d) [:Stop why [] :d1]))))

(law anything-else-is-a-bad-return
  (and (= (result-of :event :oops :s :d) [:Bad [:bad-return-from-state-function :oops]])
       (= (result-of :event [:next-state :s] :s :d)
          [:Bad [:bad-return-from-state-function [:next-state :s]]])
       (= (result-of :event [:stop :a :b :c] :s :d)
          [:Bad [:bad-return-from-state-function [:stop :a :b :c]]])))

(law one-reply-alone-is-a-list-of-one
  (forall [st Any, d Any, v Any]
    (= (result-of :event [:keep-state-and-data [:reply from v]] st d)
       [:Transition st d [[:reply from v]]])))

(law one-keyword-action-alone-is-a-list-of-one
  (forall [st Any, d Any]
    (and (= (result-of :event [:keep-state d :postpone] st :d0) [:Transition st d [:postpone]])
         (= (result-of :event [:keep-state-and-data [:hibernate true]] st d)
            [:Transition st d [[:hibernate true]]]))))

(law one-timeout-alone-is-a-list-of-one
  (and (= (result-of :event [:keep-state-and-data [:state-timeout 100 :c]] :s :d)
          [:Transition :s :d [[:state-timeout 100 :c]]])
       (= (result-of :event [:keep-state-and-data [:timeout 0 :c]] :s :d)
          [:Transition :s :d [[:timeout 0 :c]]])
       (= (result-of :event [:keep-state-and-data [:generic-timeout :g :infinity :c]] :s :d)
          [:Transition :s :d [[:generic-timeout :g :infinity :c]]])))

(law one-bare-time-alone-is-a-list-of-one
  (forall [st Any, d Any, ms Nat]
    (= (result-of :event [:keep-state-and-data ms] st d) [:Transition st d [ms]])))

(law a-bad-action-stops-the-machine
  (forall [st Any, d Any, k Keyword]
    (=> (not (contains? #{:postpone :hibernate :infinity} k))
        (and (= (result-of :event [:keep-state d k] st d)
                [:Bad [:bad-action-from-state-function k]])
             (= (result-of :event [:next-state st d [[:reply from 1] k]] st d)
                [:Bad [:bad-action-from-state-function k]])))))

(law malformed-actions-are-bad
  (and (= (result-of :event [:keep-state-and-data [nil]] :s :d)
          [:Bad [:bad-action-from-state-function nil]])
       (= (result-of :event [:keep-state-and-data [[:hibernate 1]]] :s :d)
          [:Bad [:bad-action-from-state-function [:hibernate 1]]])
       (= (result-of :event [:keep-state-and-data [[:postpone true :x]]] :s :d)
          [:Bad [:bad-action-from-state-function [:postpone true :x]]])
       (= (result-of :event [:keep-state-and-data [[:hibernate true :x]]] :s :d)
          [:Bad [:bad-action-from-state-function [:hibernate true :x]]])
       (= (result-of :event [:keep-state-and-data [[:reply from]]] :s :d)
          [:Bad [:bad-action-from-state-function [:reply from]]])
       (= (result-of :event [:keep-state-and-data [[:generic-timeout :g -1 :x]]] :s :d)
          [:Bad [:bad-action-from-state-function [:generic-timeout :g -1 :x]]]) (= (result-of :event [:keep-state-and-data [[:postpone :yes]]] :s :d)
          [:Bad [:bad-action-from-state-function [:postpone :yes]]])
       (= (result-of :event [:keep-state-and-data [[:timeout -1 :x]]] :s :d)
          [:Bad [:bad-action-from-state-function [:timeout -1 :x]]])
       (= (result-of :event [:keep-state-and-data [[:next-event :bogus 1]]] :s :d)
          [:Bad [:bad-action-from-state-function [:next-event :bogus 1]]])
       (= (result-of :event [:keep-state-and-data [[:timeout :cancel :x]]] :s :d)
          [:Bad [:bad-action-from-state-function [:timeout :cancel :x]]])
       (= (result-of :event [:keep-state-and-data [[:state-timeout :update]]] :s :d)
          [:Bad [:bad-action-from-state-function [:state-timeout :update]]])
       (= (result-of :event [:keep-state-and-data [[:timeout 5 :x {:abs :yes}]]] :s :d)
          [:Bad [:bad-action-from-state-function [:timeout 5 :x {:abs :yes}]]])
       (= (result-of :event [:keep-state-and-data [[:generic-timeout :g :cancel :x]]] :s :d)
          [:Bad [:bad-action-from-state-function [:generic-timeout :g :cancel :x]]])
       (= (result-of :event [:keep-state-and-data [[:generic-timeout :g :never]]] :s :d)
          [:Bad [:bad-action-from-state-function [:generic-timeout :g :never]]])
       (= (result-of :event [:keep-state-and-data [[:generic-timeout :g]]] :s :d)
          [:Bad [:bad-action-from-state-function [:generic-timeout :g]]])
       (= (result-of :event [:keep-state-and-data [[:generic-timeout :g 1 :x {:abs 1}]]] :s :d)
          [:Bad [:bad-action-from-state-function [:generic-timeout :g 1 :x {:abs 1}]]])
       (= (result-of :event [:keep-state-and-data [[:generic-timeout :g 1 :x {} :y]]] :s :d)
          [:Bad [:bad-action-from-state-function [:generic-timeout :g 1 :x {} :y]]])))

(law cancel-update-and-abs-are-timer-actions
  (= (result-of :event [:keep-state-and-data [[:timeout :cancel] [:state-timeout :update :c]
                                              [:generic-timeout :g :cancel] [:generic-timeout :g :update :c]
                                              [:timeout 5 :x {:abs true}] [:state-timeout 5 :x {}]
                                              [:generic-timeout :g -5 :x {:abs true}]]] :s :d)
     [:Transition :s :d [[:timeout :cancel] [:state-timeout :update :c]
                         [:generic-timeout :g :cancel] [:generic-timeout :g :update :c]
                         [:timeout 5 :x {:abs true}] [:state-timeout 5 :x {}]
                         [:generic-timeout :g -5 :x {:abs true}]]]))

(law stop-and-reply-takes-only-replies
  (= (result-of :event [:stop-and-reply :why [[:reply from 1] :postpone]] :s :d)
     [:Bad [:bad-reply-action-from-state-function :postpone]]))

(law an-enter-call-cannot-change-state
  (= (result-of :enter [:next-state :other :d] :s :d)
     [:Bad [:bad-state-enter-return-from-state-function [:next-state :other :d]]]))

(law an-enter-call-may-name-its-own-state
  (= (result-of :enter [:next-state :s :d1] :s :d) [:Transition :s :d1 []]))

(law an-enter-call-cannot-postpone-or-insert
  (and (= (result-of :enter [:keep-state :d [:postpone]] :s :d)
          [:Bad [:bad-state-enter-action-from-state-function :postpone]])
       (= (result-of :enter [:keep-state :d [[:next-event :internal 1]]] :s :d)
          [:Bad [:bad-state-enter-action-from-state-function [:next-event :internal 1]]])))

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

(law cancel-update-and-abs-parse
  (= (actions-of [[:timeout :cancel] [:state-timeout :update :c] [:generic-timeout :g :cancel]
                  [:generic-timeout :g :update :c] [:timeout 5 :x {:abs true}]
                  [:state-timeout 6 :y {:abs false}] [:generic-timeout :g 7 :z {:abs true}]
                  [:timeout :infinity :w {:abs true}]])
     [:Actions false [] []
      [[:event nil nil] [:state :update :c] [[:generic :g] nil nil] [[:generic :g] :update :c]
       [:event [:abs 5] :x] [:state 6 :y] [[:generic :g] [:abs 7] :z] [:event nil :w]]]))

(law infinity-is-a-cancel
  (= (actions-of [[:timeout :infinity :a]]) [:Actions false [] [] [[:event nil :a]]]))

(law a-bare-time-is-an-event-timeout
  (and (= (actions-of [5]) [:Actions false [] [] [[:event 5 5]]])
       (= (actions-of [0]) [:Actions false [] [] [[:event 0 0]]])
       (= (actions-of [:infinity]) [:Actions false [] [] [[:event nil :infinity]]])))

(law postpone-may-say-whether
  (and (= (actions-of [[:postpone true]]) [:Actions true [] [] []])
       (= (actions-of [[:postpone false]]) [:Actions false [] [] []])))

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

(law an-update-changes-only-the-content
  (forall [changed Bool]
    (= (next-timers changed {[:generic :g] [5 :a]} [[[:generic :g] :update :b]])
       {[:generic :g] [5 :b]})))

(law an-update-with-no-timer-starts-one-of-time-0
  (and (= (next-timers true {:state [5 :a]} [[:state :update :b]]) {:state [0 :b]})
       (= (next-timers false {:event [5 :a]} [[:event :update :b]]) {:event [0 :b]})))

(law an-abs-time-is-kept
  (= (next-timers false {} [[:state [:abs 9] :a]]) {:state [[:abs 9] :a]}))

;; which timers a transition starts anew, so the runtime knows which to
;; arm from now and which keep their deadline
(law restarted-names-what-the-actions-start
  (and (= (restarted false {:state [5 :a]} [[:event 3 :e]]) #{:event})
       (= (restarted false {:state [5 :a]} [[:state :update :b]]) #{})
       (= (restarted true {:state [5 :a]} [[:state :update :b]]) #{:state})
       (= (restarted false {:state [5 :a]} [[:state 1 :b] [:state :update :c]]) #{:state})
       (= (restarted false {} [[:state 1 :b] [:state nil nil]]) #{})))

(law restarted-is-within-the-timers
  (every? (fn [[changed ts acts]] (every? (set (keys (next-timers changed ts acts))) (restarted changed ts acts)))
          [[false {} [[:event 1 :a] [:state :update :b]]]
           [true {:state [1 :a]} [[:state :update :b] [[:generic :g] nil nil]]]
           [false {[:generic :g] [1 :a]} [[[:generic :g] 2 :b] [[:generic :g] nil nil]]]]))

;; an enter call runs before any new event, so the transition's event
;; timeout stands; the enter call's own actions apply on top of it
(law an-enter-call-keeps-the-event-timeout
  (and (= (enter-timers {:event [5 :a]} []) {:event [5 :a]})
       (= (enter-timers {:event [5 :a] :state [3 :s]} [[:state 9 :b]]) {:event [5 :a] :state [9 :b]})
       (= (enter-timers {:event [5 :a]} [[:event nil nil]]) {})))

(law an-enter-call-restarts-what-it-sets
  (and (= (enter-restarted {:event [5 :a]} []) #{})
       (= (enter-restarted {:event [5 :a]} [[:event :update :b]]) #{})
       (= (enter-restarted {} [[:event :update :b]]) #{:event})
       (= (enter-restarted {:event [5 :a]} [[:state 1 :b]]) #{:state})))

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
   :witnesses {[:ret :start] [[:ok 1 2]], [:ret :ignore] [:ignore]}
   :tested {:ret "an init's actions may be a list of any length, which needs induction"}})

(law init-ok-starts-in-the-state
  (forall [st Any, d Any] (= (init-of [:ok st d]) [:Start st d []])))

(law init-ok-with-actions
  (forall [st Any, d Any, ms Nat, v Any]
    (and (= (init-of [:ok st d [[:state-timeout ms v] [:next-event :internal v]]])
            [:Start st d [[:state-timeout ms v] [:next-event :internal v]]])
         (= (init-of [:ok st d [:next-event :internal v]]) [:Start st d [[:next-event :internal v]]]))))

(law init-ignore
  (= (init-of :ignore) [:Ignore]))

(law init-error-exits-normal
  (forall [why Any] (= (init-of [:error why]) [:Fail why :normal])))

(law init-error-takes-one-reason
  (= (init-of [:error :a :b]) [:Fail [:bad-return-from-init [:error :a :b]] [:bad-return-from-init [:error :a :b]]]))

(law init-stop-exits-with-its-reason
  (forall [why Any] (= (init-of [:stop why]) [:Fail why why])))

(law a-bare-state-and-data-is-a-bad-init
  (forall [st Any, d Any]
    (=> (not (contains? #{:ok :stop :error} st))
        (= (init-of [st d]) [:Fail [:bad-return-from-init [st d]] [:bad-return-from-init [st d]]]))))

(law init-actions-must-be-actions
  (forall [st Any, d Any, k Keyword]
    (=> (not (contains? #{:postpone :hibernate :infinity} k))
        (= (init-of [:ok st d k])
           [:Fail [:bad-action-from-state-function k] [:bad-action-from-state-function k]]))))

;; --- the runtime decides through these ----------------------------------

(calls ensemble.gen-statem/run {:through [ensemble.statem/init-of]})


(calls ensemble.gen-statem/transition!
  {:through [ensemble.statem/result-of ensemble.statem/actions-of ensemble.statem/hibernate?
             ensemble.statem/next-queues ensemble.statem/next-timers]})

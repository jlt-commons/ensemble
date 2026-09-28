(ns ensemble.actor
  "Erlang processes on jolt fibers.

  An actor is a fiber with a mailbox, an exit-signal queue, a set of links and
  a set of monitors.  Sending is a swap! onto the mailbox followed by a ring of
  a doorbell channel the receiving fiber parks on, so a send never blocks the
  sender.  Receive is selective (ensemble.select): the first message a clause
  takes is removed and everything else stays in place.

  The mailbox is in two parts, as Erlang's is split by its save pointer: the
  messages a receive has looked at and left, oldest first, which only the
  actor itself touches, and behind them the queue senders append to.  A
  receive scans the first part, then takes new messages off the queue one at
  a time, leaving each one no clause wants at the end of the first part.  So
  taking the oldest message costs the same however many wait behind it.

  Exit signals travel apart from messages, in the signal queue, and what each
  one does is decided by ensemble.signal/on-signal: die, turn it into an
  [:EXIT from reason] message (when trapping exits), or ignore it.  A signal
  the target must die of also interrupts its fiber (jolt.fibers/interrupt!),
  so it dies wherever it is -- in a receive, in a long computation, parked on
  a channel of its own -- as an Erlang process does.  The rest wait in the
  queue, which is drained whenever the actor enters or wakes in a receive.
  A kill cannot be caught for good: an actor whose body swallows it dies of
  it at its next receive, or as its body returns.

  An actor is a map; treat it as opaque.  Two actors are equal when they are
  the same process.  It prints as #<actor pid>."
  (:require [clojure.core.async :as a]
            [jolt.fibers :as fib]
            [ensemble.pattern :as pattern]
            [ensemble.select :as select]
            [ensemble.signal :as sig]))

(def ^:dynamic *actor* nil)

(defn self
  "The actor this code is running as, or nil outside an actor."
  []
  *actor*)

(defn actor?
  "True when x is an actor."
  [x]
  (and (map? x) (contains? x ::pid)))

(defmethod print-method ::actor [x ^java.io.Writer w]
  (.write w (str "#<actor " (::pid x) ">")))

(defonce ^:private counter (atom 0))

(defn make-ref
  "A unique reference (Erlang's make_ref), for tagging a request so its reply
  can be told apart from every other message."
  []
  {::ref (swap! counter inc)})

;; process table and registry -------------------------------------------

(defonce ^:private procs (atom {}))

(defonce ^:private registry (atom {}))

(defn- norm-name
  "A registry key from a name: a keyword, symbol or string, all normalised to
  the same key."
  [nm]
  (if (or (string? nm) (keyword? nm) (symbol? nm))
    (keyword (clojure.core/name nm))
    (throw (ex-info "a name is a keyword, symbol or string" {:reason :badarg :name nm}))))

(defn- open?
  "Links and monitors are an open collection until the actor exits, then
  ::closed, so a link or monitor racing the exit sees one or the other."
  [coll]
  (not= ::closed coll))

(defn alive?
  "True until the actor has exited (Erlang's is_process_alive)."
  [actor]
  (open? @(::links actor)))

(defn whereis
  "The live actor registered under nm, or nil."
  [nm]
  (let [a (get @registry (norm-name nm))]
    (when (and a (alive? a)) a)))

(defn register!
  "Register actor under nm, so ! and whereis accept the name.  As in Erlang it
  throws (reason :badarg) when the name is taken by a live actor, when the
  actor is dead, or when the actor already has a name.  The name is released
  when the actor exits.  Returns the actor."
  [nm actor]
  (let [k (norm-name nm)]
    (swap! registry
           (fn [r]
             (let [holder (get r k)]
               (cond
                 (and holder (alive? holder))
                 (throw (ex-info "name already registered" {:reason :badarg :name nm}))
                 (not (alive? actor))
                 (throw (ex-info "cannot register a dead actor" {:reason :badarg :name nm}))
                 (some (fn [[k2 v]] (and (not= k k2) (= v actor) (alive? v))) r)
                 (throw (ex-info "actor already has a name" {:reason :badarg :name nm}))
                 :else (assoc r k actor)))))
    actor))

(defn unregister!
  "Release the name nm.  Returns nil."
  [nm]
  (swap! registry dissoc (norm-name nm))
  nil)

(defn registered
  "The names of the live registered actors."
  []
  (vec (keep (fn [[k v]] (when (alive? v) k)) @registry)))

(defn resolve-dest
  "An actor from an actor or a registered name.  Sending to a name nobody
  holds throws, as Erlang's Name ! Msg does."
  [dest]
  (if (actor? dest)
    dest
    (or (whereis dest)
        (throw (ex-info "no actor registered under name" {:reason :badarg :name dest})))))

(def ^:private empty-queue clojure.lang.PersistentQueue/EMPTY)

;; sending --------------------------------------------------------------

(defn- ring! [actor] (a/offer! (::bell actor) true))

(defn- enqueue!
  "Append msg to the actor's mailbox.  A message to a dead actor is dropped."
  [actor msg]
  (when (alive? actor)
    (swap! (::inbox actor) conj msg)
    (ring! actor)))

(defn !
  "Send msg to dest, an actor or a registered name.  Never blocks.  Returns
  msg.  A message to a dead actor is silently dropped; a message to a name
  nobody holds throws."
  [dest msg]
  (enqueue! (resolve-dest dest) msg)
  msg)

;; exit signals ---------------------------------------------------------

(defn- exit-ex
  "The throwable an actor dies by when it exits with reason."
  [reason]
  (ex-info (str "exit: " (pr-str reason)) {::exit reason}))

(defn reason-of
  "The exit reason a body's throwable carries: the reason of an exit, or the
  throwable itself for a crash."
  [e]
  (let [d (ex-data e)]
    (if (and (map? d) (contains? d ::exit)) (::exit d) e)))

(defn- linked-to?
  [actor from]
  (let [ls @(::links actor)]
    (and (open? ls) (contains? ls (::pid from)))))

(defn- kill!
  "Make actor die of reason now, wherever it is: claim its death once, then
  interrupt its fiber.  The claim is what a swallowed interrupt is checked
  against later."
  [actor reason]
  (let [[old _] (swap-vals! (::life actor) (fn [l] (if (= :live l) [:dying reason] l)))]
    (when (= :live old)
      (fib/interrupt! @(::fiber actor) (exit-ex reason)))))

(defn- dying-of
  "The reason actor was killed with, or nil."
  [actor]
  (let [l @(::life actor)] (when (vector? l) (second l))))

(defn- signal!
  "Queue an exit signal at actor and wake it.  When the signal kills it --
  on-signal says so for the actor's trap flag now -- interrupt it too, so it
  does not live on until its next receive.  A signal an actor sends itself
  is acted on by the sender, at once, so it is left to the queue."
  [actor s]
  (when (alive? actor)
    (swap! (::signals actor) conj s)
    (ring! actor)
    (when (and (not= (:from s) actor)
               (or (not= :link (:kind s)) (not (:checked s)) (linked-to? actor (:from s))))
      (let [act (sig/on-signal @(::trapping actor) (:kind s) (:reason s) false)]
        (when (= :Die (first act))
          (kill! actor (second act)))))))

(defn- handle-signal!
  "Act on one exit signal.  A link signal from an actor no longer linked (it
  was unlinked while the signal was in flight) is dropped, as in Erlang, and
  one that is acted on removes the link."
  [actor {:keys [kind from reason checked]}]
  (when (or (not= :link kind) (not checked) (linked-to? actor from))
    (when (= :link kind)
      (swap! (::links actor) (fn [ls] (if (open? ls) (disj ls (::pid from)) ls))))
    (let [act (sig/on-signal @(::trapping actor) kind reason (= from actor))]
      (case (first act)
        :Die     (throw (exit-ex (second act)))
        :Deliver (enqueue! actor [:EXIT from (second act)])
        :Ignore  nil))))

(defn- drain-signals!
  "Act on every queued exit signal, oldest first.  An actor already killed
  dies here, even if its body caught the kill."
  [actor]
  (when-let [reason (dying-of actor)]
    (throw (exit-ex reason)))
  (let [[ss _] (swap-vals! (::signals actor) (constantly []))]
    (doseq [s ss] (handle-signal! actor s))))

(defn exit!
  "(exit! reason) exits the current actor with reason, as Erlang's exit/1: it
  throws, so a try in the body can catch it.  :normal is a clean exit.

  (exit! actor reason) sends actor an exit signal, as Erlang's exit/2.  An
  actor that is not trapping exits dies with reason, unless reason is
  :normal, which it ignores (but an actor sending :normal to itself exits).
  A trapping actor receives [:EXIT sender reason] instead.  :kill cannot be
  trapped: the actor dies with :killed."
  ([reason] (throw (exit-ex reason)))
  ([actor reason]
   (let [actor (resolve-dest actor)]
     (signal! actor {:kind :exit :from (self) :reason reason})
     (when (= actor (self)) (drain-signals! actor))
     true)))

(defn trap-exit!
  "Set whether the current actor traps exits.  Trapping, an exit signal
  arrives as an [:EXIT from reason] message instead of killing the actor;
  :kill sent by exit! still kills.  Returns the previous setting."
  ([] (trap-exit! true))
  ([on]
   (let [[old _] (swap-vals! (::trapping (self)) (constantly (boolean on)))]
     old)))

(defn link!
  "Link the current actor to other.  When either exits, the other gets a link
  signal with the reason (see exit!).  Links are bidirectional and idempotent.
  Linking to a dead actor sends the current actor a noproc signal: it dies
  with :noproc, or receives [:EXIT other :noproc] when trapping.  Returns
  true."
  [other]
  (let [me (self)
        other (resolve-dest other)]
    (when-not (= me other)
      (swap! (::links me) (fn [ls] (if (open? ls) (conj ls (::pid other)) ls)))
      (let [[before _] (swap-vals! (::links other)
                                   (fn [ls] (if (open? ls) (conj ls (::pid me)) ls)))]
        (when-not (open? before)
          (swap! (::links me) (fn [ls] (if (open? ls) (disj ls (::pid other)) ls)))
          (signal! me {:kind :link :from other :reason :noproc :checked false})
          (drain-signals! me))))
    true))

(defn unlink!
  "Remove the link between the current actor and other.  A link signal from
  other already on its way is dropped.  Returns true."
  [other]
  (let [me (self)
        other (resolve-dest other)
        drop-pid (fn [pid] (fn [ls] (if (open? ls) (disj ls pid) ls)))]
    (swap! (::links me) (drop-pid (::pid other)))
    (swap! (::links other) (drop-pid (::pid me)))
    true))

;; monitors -------------------------------------------------------------

(defn- add-monitor!
  "Watch actor under ref, calling (notify reason) once when it exits.  An
  actor that is already dead notifies at once with :noproc."
  [actor ref notify]
  (let [[before _] (swap-vals! (::monitors actor)
                               (fn [ms] (if (open? ms) (assoc ms ref notify) ms)))]
    (when-not (open? before) (notify :noproc))
    ref))

(defn- drop-monitor!
  [actor ref]
  (swap! (::monitors actor) (fn [ms] (if (open? ms) (dissoc ms ref) ms))))

(defn monitor!
  "Monitor actor from the current actor.  Returns a ref.  When actor exits,
  the current actor receives [:DOWN ref :process actor reason]; if it is
  already dead the reason is :noproc.  A monitor is one-way: the monitoring
  actor is never affected by the exit.  Each call makes a new monitor."
  [actor]
  (let [me (self)
        actor (resolve-dest actor)
        ref (make-ref)
        watching (::monitoring me)]
    (swap! watching assoc ref actor)
    ;; the entry in watching is a one-shot claim: the DOWN is sent only by
    ;; whoever turns it to ::firing, and demonitor! only by removing it, so
    ;; a demonitor that wins knows no DOWN will ever come
    (add-monitor! actor ref
                  (fn [reason]
                    (let [[before _] (swap-vals! watching
                                                 (fn [w] (if (actor? (get w ref)) (assoc w ref ::firing) w)))]
                      (when (actor? (get before ref))
                        (enqueue! me [:DOWN ref :process actor reason])
                        (swap! watching dissoc ref)))))))

(defn- flush!
  "Remove every mailbox message pred accepts."
  [actor pred]
  (swap! (::saved actor) (fn [ms] (into [] (remove pred) ms)))
  (swap! (::inbox actor) (fn [q] (into empty-queue (remove pred) q))))

(defn demonitor!
  "Stop the monitor ref.  No DOWN message is sent after this returns; with
  {:flush true} one already delivered is removed from the mailbox too.
  Returns true."
  ([ref] (demonitor! ref {}))
  ([ref {:keys [flush]}]
   (let [me (self)
         watching (::monitoring me)
         [before _] (swap-vals! watching (fn [w] (if (actor? (get w ref)) (dissoc w ref) w)))
         target (get before ref)]
     (if (actor? target)
       ;; claimed before the DOWN was: none is in the mailbox or coming
       (drop-monitor! target ref)
       (do
         ;; the target is sending the DOWN right now: let it land, so the
         ;; flush sees it (the window is two swaps, it never parks)
         (while (= ::firing (get @watching ref)) (fib/yield))
         (when flush
           (flush! me (fn [m] (and (vector? m) (= :DOWN (first m)) (= ref (second m))))))))
     true)))

(defn on-exit!
  "Call (f reason) once when actor exits, or at once with :noproc if it has
  already.  The hook for code off any actor (a thread waiting on a reply)
  that has no mailbox to receive a DOWN in.  Returns a ref for cancel-exit!."
  [actor f]
  (add-monitor! (resolve-dest actor) (make-ref) f))

(defn cancel-exit!
  "Cancel an on-exit! hook."
  [actor ref]
  (drop-monitor! (resolve-dest actor) ref)
  nil)

;; aliases --------------------------------------------------------------

(defn alias!
  "A fresh alias for the current actor (Erlang's alias/0): a ref that
  send-alias! delivers to this actor only while the alias is active.  A
  gen-server call tags its request with one, so a reply that arrives after
  the call gave up is dropped instead of lingering in the mailbox."
  []
  (let [me (self)
        ref (assoc (make-ref) ::owner (::pid me))]
    (swap! (::aliases me) conj ref)
    ref))

(defn unalias!
  "Deactivate an alias of the current actor.  Returns true if it was active."
  [ref]
  (let [[before _] (swap-vals! (::aliases (self)) disj ref)]
    (contains? before ref)))

(defn send-alias!
  "Send msg to the actor owning alias, if the alias is still active; otherwise
  drop it.  Returns msg."
  [alias msg]
  (when-let [owner (get @procs (::owner alias))]
    (when (contains? @(::aliases owner) alias)
      (enqueue! owner msg)))
  msg)

;; spawning and exiting -------------------------------------------------

(defn- settle!
  "An actor's last act: close its links and monitors, release its name, and
  tell everyone linked to or monitoring it that it exited with reason."
  [me reason]
  (let [[links _] (swap-vals! (::links me) (constantly ::closed))
        [mons _]  (swap-vals! (::monitors me) (constantly ::closed))]
    (swap! registry (fn [r] (into {} (remove (fn [[_ v]] (= v me))) r)))
    (doseq [[ref target] @(::monitoring me) :when (actor? target)] (drop-monitor! target ref))
    (swap! procs dissoc (::pid me))
    (doseq [pid links]
      (when-let [other (get @procs pid)]
        (signal! other {:kind :link :from me :reason reason :checked true})))
    (doseq [[_ notify] mons] (notify reason))))

(defn- new-actor [pid]
  (with-meta
    {::pid pid
     ::saved (atom [])
     ::inbox (atom empty-queue)
     ::signals (atom [])
     ::bell (a/chan (a/dropping-buffer 1))
     ::done (promise)
     ::links (atom #{})
     ::monitors (atom {})
     ::monitoring (atom {})
     ::aliases (atom #{})
     ::state (atom nil)
     ::life (atom :live)
     ::fiber (promise)
     ::trapping (atom false)}
    {:type ::actor}))

(defn spawn
  "Start an actor running (f) on a fiber and return it.  The actor exits
  :normal when f returns, with reason when f calls (exit! reason), and with
  the throwable when f throws.  Options:

      :name     register the actor under this name before it runs
      :link     link it to the current actor before it runs (spawn_link)
      :trap     start it trapping exits
      :state    the actor's initial state (see state)

  A link or name set here is in place before the body's first step, so an
  immediate crash cannot slip past it."
  ([f] (spawn f {}))
  ([f {:keys [name link trap] :as opts}]
   (let [me (new-actor (swap! counter inc))
         parent (self)
         go (promise)]
     (when trap (reset! (::trapping me) true))
     (reset! (::state me) (:state opts))
     (when name (register! name me))
     (swap! procs assoc (::pid me) me)
     (when (and link parent)
       (swap! (::links me) conj (::pid parent))
       (swap! (::links parent) (fn [ls] (if (open? ls) (conj ls (::pid me)) ls))))
     (deliver
      (::fiber me)
      (fib/spawn
       (fn []
         @go
         (binding [*actor* me]
           ;; the body runs interruptible; what follows it -- telling the
           ;; links and monitors -- must not be torn by a late kill, so it
           ;; runs masked, and a kill arriving then is simply too late
           (fib/masked
            (fn []
              (let [r (try
                        (fib/unmasked (fn [] (drain-signals! me) {:ok (f)}))
                        (catch Throwable e {:err e}))
                    killed (dying-of me)
                    reason (cond killed killed
                                 (contains? r :ok) :normal
                                 :else (reason-of (:err r)))]
                (settle! me reason)
                (deliver (::done me)
                         (if (sig/normal? reason)
                           [:ok (:ok r)]
                           [:err (or (:err r) (exit-ex reason)) reason])))))))))
     (deliver go true)
     me)))

(defn spawn-link
  "spawn with :link true."
  ([f] (spawn f {:link true}))
  ([f opts] (spawn f (assoc opts :link true))))

(defn spawn-monitor
  "Spawn an actor and monitor it from the current actor.  Returns
  [actor ref]."
  ([f] (spawn-monitor f {}))
  ([f opts]
   (let [go (promise)
         a (spawn (fn [] @go (f)) opts)
         ref (monitor! a)]
     (deliver go true)
     [a ref])))

;; state ----------------------------------------------------------------

(defn state
  "The current actor's state, or with an argument, that actor's state (for
  observing it from outside, as Erlang's sys:get_state)."
  ([] @(::state (self)))
  ([actor] @(::state (resolve-dest actor))))

(defn set-state!
  "Replace the current actor's state.  Returns the new state."
  [v]
  (reset! (::state (self)) v))

(defn update-state!
  "Apply (f state & args) to the current actor's state.  Returns the new
  state."
  [f & args]
  (apply swap! (::state (self)) f args))

;; receive --------------------------------------------------------------

(defn always
  "The guard of a clause with none: accepts every binding."
  [_ _]
  true)

(defn- take-new!
  "Take messages off me's queue, oldest first, until one satisfies a clause:
  [clause-index msg env] for it, or nil once the queue is empty.  Each one
  none wants joins the saved messages, in order.  Only the actor pops its
  queue, so the head it peeks is the head it pops."
  [me pats ok?]
  (let [inbox (::inbox me)]
    (loop []
      (let [q @inbox]
        (when-not (empty? q)
          (let [m (peek q)]
            (swap! inbox pop)
            (let [r (select/clause-of pats ok? m)]
              (if (= :Hit (first r))
                (let [[_ k env] r] [k m env])
                (do (swap! (::saved me) conj m)
                    (recur))))))))))

(defn receive-match
  "Block until a message matches one of the compiled patterns pats whose guard
  (ok? clause-index bindings) accepts it.  Returns [clause-index msg env], or
  [:timeout] when timeout-ms (nil: wait forever) elapses first.  Exit signals
  are acted on before each look at the mailbox."
  ([pats timeout-ms] (receive-match pats always timeout-ms))
  ([pats ok? timeout-ms]
   (let [me (or (self) (throw (ex-info "receive outside an actor" {})))
         deadline (when timeout-ms (+ (System/currentTimeMillis) timeout-ms))]
     (loop [start 0]
       (drain-signals! me)
       (let [saved @(::saved me)
             r (select/scan saved pats ok? start)]
         (if (= :Take (first r))
           (let [[_ i k env] r]
             ;; an emptied subvec is let go, not kept to grow on
             (reset! (::saved me) (let [s (select/without saved i)] (if (empty? s) [] s)))
             [k (nth saved i) env])
           (or (take-new! me pats ok?)
               (let [scanned (count @(::saved me))]
                 (if deadline
                   (let [left (- deadline (System/currentTimeMillis))]
                     (if (pos? left)
                       (do (a/alts!! [(::bell me) (a/timeout left)]) (recur scanned))
                       [:timeout]))
                   (do (a/<!! (::bell me)) (recur scanned)))))))))))

(defn- guard-fn
  "The ok? fn for a receive's clauses: each guard evaluated with its clause's
  bindings in scope.  A guard that throws counts as false, as in Erlang."
  [clauses]
  (if (not-any? :guard clauses)
    `always
    `(fn [k# ~'env]
       (try
         (boolean
          (case k#
            ~@(mapcat (fn [i c]
                        [i (if (:guard c)
                             `(let [~@(mapcat (fn [s] [s `(get ~'env '~s)]) (:syms c))]
                                ~(:guard c))
                             true)])
                      (range) clauses)))
         (catch Throwable _# false)))))

(defn- pinned-pattern
  "Compiled-pattern code: quoted data, with each [:Pin s] replaced by a
  literal of the local s's value."
  [p]
  (cond
    (and (vector? p) (= :Pin (first p))) `[:Lit ~(second p)]
    (and (vector? p) (= :Cons (first p))) `[:Cons ~(pinned-pattern (nth p 1)) ~(pinned-pattern (nth p 2))]
    :else `(quote ~p)))

(defmacro receive
  "Selective receive over the current actor's mailbox.

  Each clause is [pattern & body], or [pattern :when guard & body].  A pattern
  is a tuple [:tag x ...], a symbol (binds the message), _ (wildcard) or a
  literal.  A symbol already bound where the receive is written matches its
  value instead of binding (Erlang's rule), and a name repeated in a pattern
  must match equal values.  The guard sees the clause's bindings; a message
  whose guard is false or throws is left for another clause.  Two special
  clauses:

      [:else & body]      matches the next message whatever it is
      [:after ms & body]  runs body if nothing matches within ms (nil: never)

  Returns the value of the chosen clause's body."
  [& clauses]
  (let [pinned (set (filter symbol? (keys &env)))
        parsed (mapv (fn [c]
                       (let [pat (first c) more (rest c)]
                         (cond
                           (= pat :after) {:kind :after :timeout (first more) :body (rest more)}
                           :else
                           (let [pat (if (= pat :else) '_ pat)
                                 [guard body] (if (= :when (first more))
                                                [(second more) (drop 2 more)]
                                                [nil more])]
                             {:kind :match :pattern pat :guard guard :body body
                              :syms (pattern/bound-syms pat pinned)}))))
                     clauses)
        ms    (filterv #(= :match (:kind %)) parsed)
        after (first (filter #(= :after (:kind %)) parsed))
        pats  (mapv #(pinned-pattern (pattern/compile-form (:pattern %) pinned)) ms)
        r     (gensym "r")
        env   (gensym "env")]
    `(let [~r (receive-match ~pats ~(guard-fn ms) ~(:timeout after))
           ~env (nth ~r 2 nil)]
       (case (nth ~r 0)
         :timeout ~(if after `(do ~@(:body after)) nil)
         ~@(mapcat (fn [i c]
                     [i `(let [~@(mapcat (fn [s] [s `(get ~env '~s)]) (:syms c))]
                           ~@(:body c))])
                   (range) ms)))))

;; joining from outside ---------------------------------------------------

(defn- outcome [[tag v]]
  (if (= :ok tag) v (throw v)))

(defn join
  "Block until actor exits.  Return its body's value on a :normal exit;
  otherwise rethrow what it died by.  With timeout-ms, throw if it is still
  running then.  Not an Erlang operation: it is for code outside the actor
  world, a test or a REPL, to wait on one."
  ([actor] (outcome @(::done actor)))
  ([actor timeout-ms]
   (let [r (deref (::done actor) timeout-ms ::timeout)]
     (if (= ::timeout r)
       (throw (ex-info "join timed out" {:timeout-ms timeout-ms}))
       (outcome r)))))

(defn exit-reason
  "Block until actor exits and return its exit reason: :normal, the reason it
  exited with, or the throwable it died by.  With timeout-ms, nil if it is
  still running then."
  ([actor] (let [[tag _ reason] @(::done actor)] (if (= :ok tag) :normal reason)))
  ([actor timeout-ms]
   (let [r (deref (::done actor) timeout-ms ::timeout)]
     (when-not (= ::timeout r)
       (let [[tag _ reason] r] (if (= :ok tag) :normal reason))))))

;; actors as state transitions --------------------------------------------

(defn spawn-actor
  "Spawn an actor defined by how it changes state: (handler state msg)
  returns the next state.  The actor takes each message in arrival order,
  runs the handler, and keeps what it returns as its state, observable
  through (state actor).  Effects -- sending to other actors, replying --
  happen in the handler; (exit! reason) in the handler stops the actor.
  Exit signals behave as for any actor, so a trapping actor (:trap true)
  sees [:EXIT from reason] as an ordinary message.  opts are spawn's, with
  init as the initial :state."
  ([handler init] (spawn-actor handler init {}))
  ([handler init opts]
   (spawn (fn []
            (loop []
              (let [msg (receive [m m])]
                (set-state! (handler (state) msg))
                (recur))))
          (assoc opts :state init))))

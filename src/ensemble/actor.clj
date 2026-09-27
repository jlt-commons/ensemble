(ns ensemble.actor
  "Erlang-style actors on jolt fibers.

  An actor is a fiber with a mailbox and a state cell.  The mailbox is stored
  as plain data (ensemble.mailbox) in an atom, so a send from any fiber is a
  swap! and never blocks; a promise-free doorbell channel wakes the receiving
  fiber.  Receive is selective: it scans the mailbox and takes the first
  message a clause matches, leaving everything else in place.

  An actor is a map:

      {:fiber   f     the carrier fiber running the body
       :mailbox atom  the mailbox as ensemble.mailbox data
       :doorbell atom a channel the receiving fiber parks on
       :state   atom  the actor's current state
       :done    promise  [:ok v] or [:err e] once the body returns
       :name    the registered name, or nil
       :pid     the process id, a link-table key
       :links   atom  the set of linked actors
       :trapping atom true when exit signals arrive as messages
       ::actor  true     marks the map as an actor}

  ::actor is what actor? checks, so a plain map that happens to carry a
  :mailbox key is not mistaken for an actor.

  Spawning runs the body on a fiber with *actor* bound, so the body can call
  self, receive and ! as the actor."
  (:require [clojure.core.async :as a]
            [jolt.fibers :as fib]
            [ensemble.match :as match]
            [ensemble.mailbox :as mb]
            [ensemble.pattern :as pattern]
            [ensemble.select :as select]))

(def ^:dynamic *actor* nil)

(defn self
  "The actor this code is running as, or nil outside an actor."
  []
  *actor*)

(defonce ^:private registry (atom {}))

(defonce ^:private pids (atom 0))

(defn- next-pid
  "A fresh process id, so the links table can key on distinct actors."
  []
  (swap! pids inc))

(defn- exit-reason
  "The reason an actor settles with: :normal when its body returns, else the
  reason carried by a link signal, or the throwable itself."
  [ok? err]
  (if ok?
    :normal
    (let [d (ex-data err)]
      (if (and (map? d) (contains? d ::exit)) (::exit d) err))))

(declare ^:private drop-watches-of!)
(declare ^:private notify-watchers!)
(declare ^:private propagate-exit!)

(defn- actor? [x]
  (and (map? x) (contains? x ::actor)))

(defn- norm-name
  "A registry key from a name.  Accepts a string, keyword or symbol; anything
  else is a caller error, so throw rather than store under a nonsense key."
  [nm]
  (if (or (string? nm) (keyword? nm) (symbol? nm))
    (clojure.core/name nm)
    (throw (ex-info "name must be a string, keyword or symbol" {:name nm}))))

(defn vref
  "Wrap a value in an IDeref so it can be deref'd."
  [x]
  (reify clojure.lang.IDeref (deref [_] x)))

(defn maketag
  "A random, probably-unique identifier (Erlang's makeref)."
  []
  (rand-int 1000000000))

(defn register!
  "Publish an actor so whereis finds it.  The name may be a string or keyword;
  both normalise to the same string key.  Arities mirror pulsar: (register!
  nm actor) registers a specific actor, (register! nm) registers the current
  actor, (register! actor) registers that actor under its own :name, and
  (register!) registers the current actor under its :name."
  ([nm actor]
   (swap! registry assoc (norm-name nm) actor)
   actor)
  ([actor-or-name]
   (if (actor? actor-or-name)
     (let [nm (:name actor-or-name)]
       (when nm (swap! registry assoc (norm-name nm) actor-or-name))
       actor-or-name)
     (register! actor-or-name (self))))
  ([]
   (register! (:name (self)) (self))))

(defn whereis
  "The actor registered under nm, or nil.  With a timeout in milliseconds,
  poll until it appears or the timeout elapses (returns nil on timeout)."
  ([nm]
   (get @registry (norm-name nm)))
  ([nm timeout-ms]
   (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
     (loop []
       (or (get @registry (norm-name nm))
           (when (< (System/currentTimeMillis) deadline)
             (a/<!! (a/timeout 5))
             (recur)))))))

(defn unregister!
  "Remove an actor from the registry.  Accepts the actor itself or a name;
  with no argument, unregisters the current actor."
  ([x]
   (swap! registry dissoc (norm-name (if (actor? x) (:name x) x)))
   nil)
  ([]
   (unregister! (self))))

(defn mailbox-of
  "The mailbox of actor."
  [actor]
  (:mailbox actor))

(defn- new-doorbell [] (atom (a/chan (a/dropping-buffer 1))))

(defn- signal! [actor] (a/>!! @(:doorbell actor) true))

(defn- actor-obj [fiber mailbox doorbell state done name pid]
  {:fiber fiber :mailbox mailbox :doorbell doorbell
   :state state :done done :name name :pid pid
   :links (atom #{}) :trapping (atom false) ::actor true})

(defn spawn
  "Start an actor running f on a fiber.  f is called with no arguments and its
  return value settles the actor.  Options:

      :state  the actor's initial state (default nil)
      :name   a name to register the actor under

  Returns the actor."
  ([f] (spawn f {}))
  ([f opts]
   (let [mbox (atom [:Empty])
         bell (new-doorbell)
         st   (atom (:state opts))
         done (promise)
         me   (promise)
         fiber (fib/spawn
                (fn []
                  (binding [*actor* @me]
                    (let [r (try {:ok (f)} (catch Throwable e {:err e}))
                          ok? (contains? r :ok)
                          reason (exit-reason ok? (:err r))
                          clean? (or ok? (= :normal reason))]
                      (deliver done (if clean? [:ok (:ok r)] [:err (:err r)]))
                      (drop-watches-of! @me)
                      (propagate-exit! @me reason)
                      (notify-watchers! @me (when-not clean? (:err r)))
                      (if clean? (:ok r) (throw (:err r)))))))
         actor (actor-obj fiber mbox bell st done (:name opts) (next-pid))]
     (deliver me actor)
     (when (:name opts) (register! (:name opts) actor))
     actor)))

(defn- send! [actor msg]
  (swap! (:mailbox actor) mb/enqueue msg)
  (signal! actor)
  actor)

(defn !
  "Send msg to actor.  Returns actor.  Never blocks the sender.

  With more than one message argument they are packed into a vector, matching
  pulsar: (! a 1 2) sends [1 2]."
  ([actor msg] (send! actor msg))
  ([actor arg & args] (send! actor (into [arg] args))))

(defn !!
  "Synchronous send.  Identical to ! here: a send is a completed swap! before it
  returns, so there is no weaker guarantee to strengthen.  Kept for parity with
  pulsar's sendSync.  Packs multiple arguments into a vector like !."
  ([actor msg] (send! actor msg))
  ([actor arg & args] (send! actor (into [arg] args))))

(defn- claim-of
  "Atomically take the first message matching one of pats out of the mailbox.
  Returns [pat-idx msg] or nil.  Only the receiving fiber removes messages, so
  the scan it already did agrees with the one swap! re-runs."
  [mbox-atom pats]
  (let [taken (volatile! nil)]
    (swap! mbox-atom
           (fn [m]
             (let [r (select/find-first-of m pats)]
               (if (= :Take (first r))
                 (do (vreset! taken [(nth r 1) (nth r 2)]) (nth r 3))
                 m))))
    @taken))

(defn- exit-signal?
  "A pending exit signal delivered by a link: [:EXIT from-pid reason]."
  [m]
  (and (vector? m) (= 3 (count m)) (= :EXIT (nth m 0))))

(defn- claim-exit
  "Atomically take the first exit signal the actor must act on out of the
  mailbox, leaving the rest.  A trapping actor keeps ordinary signals (they
  arrive as messages); :killed is never trappable, so it is always taken.
  Returns the signal or nil."
  [mbox-atom trapping?]
  (let [taken (volatile! nil)]
    (swap! mbox-atom
           (fn [m]
             (let [xs (vec (mb/msgs m))
                   i  (first (keep-indexed
                              (fn [i x]
                                (when (and (exit-signal? x)
                                           (or (not trapping?) (= :killed (nth x 2))))
                                  i))
                              xs))]
               (if (some? i)
                 (do (vreset! taken (nth xs i))
                     (reduce mb/enqueue [:Empty]
                             (concat (subvec xs 0 i) (subvec xs (inc i)))))
                 m))))
    @taken))

(defn- enforce-exit!
  "Before taking an ordinary message, a linked exit signal kills the actor.  A
  trapping actor keeps it as a message; a non-trapping actor throws, carrying
  the reason so the settle path forwards it to the actor's own links."
  [actor]
  (when-let [sig (claim-exit (:mailbox actor) @(:trapping actor))]
    (throw (ex-info "linked exit" {::exit (nth sig 2)}))))

(defn- fatal-exit?
  "True when a just-claimed message is an exit signal the actor must die from
  rather than handle.  Guards the race where a catch-all pattern would
  otherwise swallow an exit signal that arrived after enforce-exit! ran."
  [actor m]
  (and (exit-signal? m)
       (or (not @(:trapping actor)) (= :killed (nth m 2)))))

(defn receive-match
  "Block until a mailbox message matches one of pats.  Returns [pat-idx msg env]
  with msg the message and env the captured bindings, or [:timeout {}] when
  timeout-ms elapses first.  With timeout-ms nil it waits forever."
  [actor pats timeout-ms]
  (loop []
    (enforce-exit! actor)
    (if-let [t (claim-of (:mailbox actor) pats)]
      (let [idx (nth t 0)
            msg (nth t 1)]
        (if (fatal-exit? actor msg)
          (throw (ex-info "linked exit" {::exit (nth msg 2)}))
          [idx msg (match/capture (nth pats idx) msg)]))
      (let [bell @(:doorbell actor)]
        (if (some? timeout-ms)
          (let [[v _] (a/alts!! [bell (a/timeout timeout-ms)])]
            (if (nil? v) [:timeout {}] (recur)))
          (do (a/<!! bell) (recur)))))))

(defn receive-timed
  "Wait up to timeout-ms for the next message and return it, or nil if none
  arrives.  Takes the message whatever it is, without matching."
  [timeout-ms]
  (let [r (receive-match (self) [[:Wild]] timeout-ms)]
    (when-not (= :timeout (nth r 0))
      (nth r 1))))

(defn- emit-body [pat body]
  (let [syms (pattern/bound-syms pat)]
    (if (seq syms)
      `(let [~@(mapcat (fn [s] [s `(get ~'env (quote ~s))]) syms)] ~@body)
      `(do ~@body))))

(defmacro receive
  "Selective receive over the current actor's mailbox.

  Each clause is [pattern & body].  A pattern is a tuple pattern [:tag x ...],
  a symbol (binds the whole message), _ (wildcard) or a literal.  Two special
  clause patterns:

      [:else & body]    catch-all: matches the next message whatever it is
      [:after ms & body]  if no message matches within ms milliseconds, run body

  Returns the value of the matched clause's body, or of the :after body on
  timeout.  The patterns of a receive are compiled once, at macro expansion."
  [& clauses]
  (let [parsed (mapv (fn [c]
                       (let [pat (first c) body (rest c)]
                         (cond
                           (= pat :after) {:kind :after :timeout (first body) :body (rest body)}
                           (= pat :else)  {:kind :match :pattern '_ :body body}
                           :else          {:kind :match :pattern pat :body body})))
                     clauses)
        ms    (filterv #(= :match (:kind %)) parsed)
        after (first (filter #(= :after (:kind %)) parsed))
        pats  (mapv #(pattern/compile-form (:pattern %)) ms)]
    `(let [~'r (receive-match (self) (quote ~pats) ~(:timeout after))]
       (cond
         (nil? ~'r) nil
         (= :timeout (nth ~'r 0)) ~(if after (emit-body '_ (:body after)) nil)
         :else (let [~'env (nth ~'r 2)]
                 (case (nth ~'r 0)
                   ~@(mapcat (fn [i c] [i (emit-body (:pattern c) (:body c))])
                             (range) ms)
                   nil))))))

(defn state
  "The actor's current state."
  [actor]
  @(:state actor))

(defn set-state!
  "Replace the actor's state."
  [actor v]
  (reset! (:state actor) v))

(defn done?
  "True once the actor's body has returned or thrown."
  [actor]
  (let [s (fib/state (:fiber actor))]
    (or (= :done s) (= :dead s))))

(defn join
  "Block until the actor settles; return its value, or rethrow what it threw.
  With a timeout in milliseconds, throw if the actor has not settled by then."
  ([actor]
   (let [[tag v] @(:done actor)]
     (if (= :ok tag) v (throw v))))
  ([actor timeout-ms]
   (let [c (a/chan 1)]
     (fib/spawn (fn [] (a/>!! c @(:done actor))))
     (let [[r _] (a/alts!! [c (a/timeout timeout-ms)])]
       (if (nil? r)
         (throw (ex-info "join timed out" {:timeout-ms timeout-ms}))
         (let [[tag v] r]
           (if (= :ok tag) v (throw v))))))))

(defonce ^:private watches (atom {}))

(defn- settled? [actor]
  (realized? (:done actor)))

(defn- done-cause [actor]
  (let [[tag v] @(:done actor)]
    (when (= :err tag) v)))

(defn- claim-watch!
  "Atomically take ref out of the watch table, returning true only for the
  caller that removed it, so two racers never both notify."
  [ref]
  (let [prev (volatile! nil)]
    (swap! watches (fn [ws] (vreset! prev ws) (dissoc ws ref)))
    (contains? @prev ref)))

(defn- notify-watchers!
  "Tell everyone watching actor that it settled.  A racing watch! claims the
  same ref with the same atomic dissoc, so at most one of the two notifies."
  [actor cause]
  (doseq [[ref w] @watches
          :when (= (:watched w) actor)]
    (when (claim-watch! ref)
      (! (:watcher w) [:exit ref actor cause]))))

(defn- drop-watches-of!
  "Forget every watch held by actor; it is settling and can no longer receive
  them."
  [actor]
  (swap! watches
         (fn [ws] (into {} (remove (fn [[_ w]] (= (:watcher w) actor)) ws)))))

(defn watch!
  "Watch actor from the current actor.  Returns a ref.  When actor settles the
  watching actor receives [:exit ref actor cause], where cause is nil for a
  normal exit and the throwable otherwise.  The watch is asymmetric: the
  watcher is not affected by the watched actor's death, and watching an
  already-dead actor still notifies.  No fiber is parked per watch: the watched
  actor fans out on settle, and a dying watcher drops its own watches."
  ([actor] (watch! (self) actor))
  ([watcher actor]
   (let [ref (gensym "watch")]
     (swap! watches assoc ref {:watcher watcher :watched actor})
     (when (settled? actor)
       (when (claim-watch! ref)
         (! watcher [:exit ref actor (done-cause actor)])))
     ref)))

(defn unwatch!
  "Stop watching the actor identified by ref; no exit message is sent."
  [ref]
  (swap! watches dissoc ref)
  nil)

;; links and exit signals ------------------------------------------------

(defn- done-reason
  "The reason an actor settled with.  Only call once it is settled."
  [actor]
  (let [[tag v] @(:done actor)]
    (exit-reason (= :ok tag) (when (= :err tag) v))))

(defn- signal-exit!
  "Send the exit signal of a dead actor to a linked survivor, unless the exit
  was normal (benign) or the survivor has already settled."
  [dead alive]
  (when-not (settled? alive)
    (let [reason (done-reason dead)]
      (when (not= :normal reason)
        (! alive [:EXIT (:pid dead) reason])))))

(defn- propagate-exit!
  "On settling abnormally, forward the exit signal to every actor linked to
  actor.  The reason is carried unchanged, so a cascade of links all die with
  the original reason; a trapping actor stops the cascade.  The reason is
  passed in rather than read back from :done, because a settling actor has not
  delivered :done yet and reading it here would deadlock."
  [actor reason]
  (when (not= :normal reason)
    (doseq [other @(:links actor)]
      (when-not (settled? other)
        (! other [:EXIT (:pid actor) reason])))))

(defn link!
  "Link the current actor and other.  If either exits abnormally the other is
  sent the exit signal and dies too, unless it traps exits.  Links are mutual
  and idempotent.  Returns other.  If other has already exited abnormally, the
  current actor is signalled at once."
  ([other] (link! (self) other))
  ([a b]
   (when-not (= (:pid a) (:pid b))
     (swap! (:links a) conj b)
     (swap! (:links b) conj a)
     (when (settled? a) (signal-exit! a b))
     (when (settled? b) (signal-exit! b a)))
   b))

(defn unlink!
  "Remove the link between the current actor and other.  Returns other."
  ([other] (unlink! (self) other))
  ([a b]
   (swap! (:links a) disj b)
   (swap! (:links b) disj a)
   b))

(defn trap-exit!
  "Make the current actor trap exits.  With trapping on, an incoming exit signal
  arrives as an ordinary [:EXIT from reason] message instead of killing the
  actor; :killed is still never trappable.  (trap-exit! false) stops trapping.
  Returns the actor."
  ([] (trap-exit! true))
  ([on] (reset! (:trapping (self)) (boolean on)) (self)))

(defn exit!
  "Exit the current actor with reason.  :normal is a quiet stop (join returns
  nil, linked actors are unaffected); any other reason is abnormal and
  propagates to the actor's links."
  ([] (exit! :normal))
  ([reason] (throw (ex-info "exit" {::exit reason}))))

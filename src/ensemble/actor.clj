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
       :name    the registered name, or nil}

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

(defn register!
  "Publish actor under name so whereis finds it."
  [name actor]
  (swap! registry assoc name actor)
  actor)

(defn whereis
  "The actor registered under name, or nil."
  [name]
  (get @registry name))

(defn unregister! [name]
  (swap! registry dissoc name)
  nil)

(defn- new-doorbell [] (atom (a/chan (a/dropping-buffer 1))))

(defn- signal! [actor] (a/>!! @(:doorbell actor) true))

(defn- actor-obj [fiber mailbox doorbell state done name]
  {:fiber fiber :mailbox mailbox :doorbell doorbell
   :state state :done done :name name})

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
                    (let [r (try {:ok (f)} (catch Throwable e {:err e}))]
                      (if (contains? r :ok)
                        (do (deliver done [:ok (:ok r)]) (:ok r))
                        (do (deliver done [:err (:err r)]) (throw (:err r))))))))
         actor (actor-obj fiber mbox bell st done (:name opts))]
     (deliver me actor)
     (when (:name opts) (register! (:name opts) actor))
     actor)))

(defn !
  "Send msg to actor.  Returns actor.  Never blocks the sender."
  [actor msg]
  (swap! (:mailbox actor) mb/enqueue msg)
  (signal! actor)
  actor)

(defn !!
  "Synchronous send: like ! but counts as delivered once queued.  Same as ! for
  an unbounded mailbox."
  [actor msg]
  (! actor msg))

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

(defn receive-match
  "Block until a mailbox message matches one of pats.  Returns [pat-idx env]
  with env the captured bindings, or [:timeout {}] when timeout-ms elapses
  first.  With timeout-ms nil it waits forever."
  [actor pats timeout-ms]
  (loop []
    (if-let [t (claim-of (:mailbox actor) pats)]
      (let [idx (nth t 0)
            msg (nth t 1)]
        [idx (match/capture (nth pats idx) msg)])
      (let [bell @(:doorbell actor)]
        (if (some? timeout-ms)
          (let [[v _] (a/alts!! [bell (a/timeout timeout-ms)])]
            (if (nil? v) [:timeout {}] (recur)))
          (do (a/<!! bell) (recur)))))))

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
         :else (let [~'env (nth ~'r 1)]
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
  "Block until the actor settles; return its value, or rethrow what it threw."
  [actor]
  (let [[tag v] @(:done actor)]
    (if (= :ok tag) v (throw v))))

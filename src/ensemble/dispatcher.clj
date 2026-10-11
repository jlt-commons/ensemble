(ns ensemble.dispatcher
  "Where actors run: named pools of carriers, as Akka's dispatchers.

  Every actor is a fiber, and a fiber stays on the carrier (OS thread) it
  was placed on for life.  Waiting in a receive, on a channel, a promise or
  a sleep parks the fiber and frees the carrier, but a blocking foreign
  call -- a C database driver, say -- holds the carrier until it returns,
  and every actor placed on that carrier waits with it.  A long computation
  is preempted, but it still takes a share of its carrier from the actors
  beside it.

  A dispatcher is a pool of carriers of its own (jolt.fibers/pool).  An
  actor spawned with {:dispatcher name} runs on that pool's carriers only,
  so work that blocks, or that must not crowd out everything else, is kept
  apart.  Messages, links, monitors and exit signals work across
  dispatchers exactly as within one.

  Two dispatchers are there from the start:

      :default   jolt's default carrier pool, one carrier per processor;
                 what every actor runs on unless told otherwise
      :blocking  16 carriers for blocking calls, as Akka's
                 default-blocking-io-dispatcher

  define! adds more.  A pool's carriers start the first time an actor is
  spawned on it, so a dispatcher defined and never used costs nothing.

  An actor's children run on :default unless they name a dispatcher too,
  as in Akka.  A hibernating actor wakes on its own dispatcher; if that has
  been shut down meanwhile, it wakes on :default."
  (:require [jolt.fibers :as fib]))

(defn- entry [name size]
  {:size size
   :pool (delay (fib/pool {:name name :size size}))})

(defonce ^:private registry
  (atom {:blocking (entry :blocking 16)}))

(defn- badarg [msg data]
  (throw (ex-info msg (assoc data :reason :badarg))))

(defn define!
  "Define dispatcher name, a keyword, with :size carriers.  A dispatcher may
  be redefined until an actor has been spawned on it, so :blocking can be
  resized at startup; after that it is a badarg.  :default is jolt's and
  cannot be.  Returns name."
  [name {:keys [size]}]
  (when-not (keyword? name) (badarg "a dispatcher is named by a keyword" {:name name}))
  (when (= :default name) (badarg "the default dispatcher is jolt's default pool" {:name name}))
  (when-not (and (int? size) (pos? size))
    (badarg "a dispatcher's :size is a positive int" {:name name :size size}))
  (swap! registry
         (fn [r]
           (when (some-> (get r name) :pool realized?)
             (badarg "dispatcher already running" {:name name}))
           (assoc r name (entry name size))))
  name)

(defn dispatchers
  "The names of the dispatchers there are, :default among them."
  []
  (into [:default] (sort (keys @registry))))

(defn pool
  "The jolt pool dispatcher name runs its actors on, started if it was not,
  or nil for :default.  An unknown name is a badarg."
  [name]
  (if (or (nil? name) (= :default name))
    nil
    (if-let [e (get @registry name)]
      @(:pool e)
      (badarg "no such dispatcher" {:dispatcher name}))))

(defn shutdown!
  "Remove dispatcher name: an actor can no longer be spawned on it, and its
  carriers stop once the actors running on them have exited.  Shutting down
  :default is a badarg; an unknown name is ignored."
  [name]
  (when (= :default name) (badarg "the default dispatcher cannot be shut down" {:name name}))
  (let [[old _] (swap-vals! registry dissoc name)]
    (when-let [p (some-> (get old name) :pool)]
      (when (realized? p) (fib/shutdown! @p))))
  nil)

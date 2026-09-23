# ensemble

Erlang-style actors and the OTP behaviours for [jolt](https://github.com/jolt-lang/jolt), built on
jolt's native fibers. No JVM, no Quasar.

This is a port of the actor layer from [pulsar](https://github.com/puniverse/pulsar) with the
Quasar machinery replaced by jolt fibers. A jolt fiber is already a lightweight
thread with its own stack, so an actor is just a fiber plus a mailbox and a
selective receive.

## What's here

- `ensemble.actor` — spawn, send, selective receive, join
- `ensemble.gen-server` — the gen_server behaviour
- `ensemble.gen-event` — the gen_event behaviour, a manager and handler records
- `ensemble.gen-fsm` — the gen_fsm behaviour, named states and events
- `ensemble.supervisor` — supervision trees with restart strategies
- `ensemble` — one namespace that re-exports the lot

## Depend on it

Not published yet, so point at the git repo:

```clojure
{:deps {io.github.jlt-commons/ensemble
        {:git/url "https://github.com/jlt-commons/ensemble"
         :git/sha "PUT-A-SHA-HERE"}}}
```

## Actors

An actor is a fiber running a body. The body calls `receive` to pull the first
message a clause matches out of its mailbox. Sending never blocks the sender.

```clojure
(require '[ensemble :as e])

(defn counter-loop [me]
  (e/receive
   [[:add k]   (do (e/set-state! me (+ (e/state me) k)) (counter-loop me))]
   [[:get from] (do (e/! from (e/state me)) (counter-loop me))]))

(def counter
  (e/spawn (fn [] (counter-loop (e/self))) {:state 0}))
```

`receive` is selective. It scans the mailbox in order and takes the first message
matching one of the clauses, leaving everything else where it is. A pattern is a
symbol to bind the whole message, `_` to match anything, a literal to match
itself, or a vector to match a message element by element.

```clojure
(e/receive
 [[:reply v] (handle v)]     ; binds v from a two-element message
 [msg        (handle-other msg)] ; binds the whole message
 [:else      (handle-any)]   ; matches the next message whatever it is
 [:after 100 (handle-timeout)]) ; runs if nothing matches within 100ms
```

`e/spawn` takes `{:name :state}` options and `e/whereis` looks up a registered
name. `e/join` blocks until the actor settles and rethrows whatever it threw;
`(e/join a timeout-ms)` throws if the actor has not settled in time.
`e/receive-timed` waits for the next message of any shape and returns it, or nil
on timeout. `!` and `!!` pack extra arguments into a vector: `(! a 1 2)` sends
`[1 2]`.

`e/watch!` makes one actor watch another. The watcher receives
`[:exit ref actor cause]` when the watched actor settles — `cause` nil for a
normal exit, the throwable otherwise — even if it was already dead when watched.
`e/unwatch!` cancels a watch by the ref `watch!` returned.

## gen_server

Implement `ensemble.gen-server/Server` on a record. A handler returns a tagged
vector: `[:reply value new-state]`, `[:noreply new-state]`, or
`[:stop reason new-state]`, each with an optional trailing timeout in
milliseconds.

```clojure
(require '[ensemble.gen-server :as gs])

(defrecord Counter []
  gs/Server
  (init [_] 0)
  (handle-call [_ _from msg st]
    (case (first msg)
      :add [:reply (+ st (nth msg 1)) (+ st (nth msg 1))]
      :get [:reply st st]))
  (handle-cast [_ _msg st] [:noreply (inc st)])
  (handle-info [_ _msg st] [:noreply st])
  (handle-timeout [_ st] [:noreply st])
  (terminate [_ _reason _st] nil))

(def c (gs/gen-server (->Counter)))
(gs/call! c [:add 3]) ;=> 3
(gs/cast! c [:tick])
```

`gen-server` takes an initial `:timeout` that arms `handle-timeout` before the
first message arrives. `call!`, `call-timed!` and `cast!` pack extra arguments
into a vector like `!`, and `call-timed!` overrides the call timeout per call. A
message that is not tagged `[:call ...]`/`[:cast ...]`/`[:info ...]` goes to
`handle-info` as-is. `reply!` from anywhere lets a handler answer a call later.
If a handler throws, `call!` rethrows to the caller and the server terminates.
`shutdown!` stops the server from outside as a normal exit.

## gen_event

A manager fans events out to registered handlers. A handler is a record
implementing `Handler` with `h-init`, `h-handle-event`, `h-handle-call` and
`h-terminate`.

```clojure
(require '[ensemble.gen-event :as ge])

(def m (ge/start-manager))
(ge/add-handler! m :log (->Logger))
(ge/notify m {:level :warn :msg "disk almost full"})
```

`notify` is async, `sync-notify!` waits for every handler to run, and
`call-handler!` talks to one handler by id. Pass `:handlers` (a seq of
`[id handler]`) to `start-manager` to register handlers before the manager runs.

## gen_fsm

A state machine with named states. `fsm-init` returns `[state-name data]`.
`fsm-handle-event` returns `[:next state data]`, `[:reply value state data]`, or
`[:stop reason state data]`.

```clojure
(require '[ensemble.gen-fsm :as fsm])

(defrecord Turnstile []
  fsm/FSM
  (fsm-init [_] [:locked :closed])
  (fsm-handle-event [_ name event data]
    (case event
      :coin [:next :unlocked data]
      :push [:next :locked data]))
  (fsm-handle-timeout [_ name data] [:next name data])
  (fsm-terminate [_ _reason _name _data] nil))

(def t (fsm/start-fsm (->Turnstile)))
(fsm/sync-send-event! t :coin) ;=> :unlocked
```

Events arrive through `send-event!`/`sync-send-event!`, or a plain `!` to the fsm
actor (the message is the event). `sync-send-event!` returns the `[:reply ...]`
value.

## Supervisors

A supervisor starts children from specs and restarts them when they exit. A spec
is a map with `:id`, a `:start` function returning the child actor, and an
optional `:restart` type.

```clojure
(require '[ensemble.supervisor :as sup])

(def sup
  (sup/start-supervisor {:strategy :one-for-one :max-restarts 3 :max-seconds 5}))

(sup/start-child! sup :db {:start (fn [] (gs/gen-server (->Db)))})
(sup/which-children! sup) ;=> [:db]
(sup/get-child sup :db)   ;=> the child actor
```

Pass `:children` (a seq of specs) to `start-supervisor` to start children up
front. `get-child` returns a child's actor, `remove-child!` untracks it without
stopping it, and `remove-and-terminate-child!` untracks it and best-effort stops
it (a gen-server child honours the shutdown message).

Strategies are `:one-for-one`, `:one-for-all` and `:rest-for-one`. A child's
restart type decides whether its exit earns a restart: `:permanent` always,
`:transient` only on an abnormal exit, `:temporary` never. If restarts exceed
`max-restarts` within `max-seconds`, the supervisor shuts down.

## Differences from pulsar

The semantics follow pulsar's actor layer, but several surfaces are shaped
differently. These are deliberate:

- The behaviour protocols carry state explicitly. `Server`/`Handler`/`FSM`
  handlers receive and return the current state instead of the actor holding it,
  so a handler is a pure function of `(state, message)`. pulsar's `handle-call`
  also takes the caller and a message id as separate arguments; here the caller
  and its reply channel arrive as one `from` map, and `reply!`/`reply-error!`
  take that `from` rather than `(to id ...)`.
- `set-timeout!` is gone. A timeout is a trailing element of a handler's tagged
  return (`[:noreply state timeout-ms]`) rather than a side-effecting call that
  mutates the current server.
- gen_event identifies handlers by an explicit `id` you supply; pulsar keys them
  by the handler object's identity.
- gen_fsm is record-and-protocol based, returning tagged vectors
  (`[:next ...]`, `[:reply ...]`, `[:stop ...]`); pulsar drives the machine with
  a function whose `:done` value terminates it.
- The behaviours start from plain functions (`start-manager`, `start-fsm`,
  `start-supervisor`, `gen-server`) that take an already-constructed record,
  rather than the variadic keyword-args macros pulsar uses.
- `whereis` does not block: it returns nil when nothing is registered, or polls
  until a timeout when you pass one.
- Quasar-era spawn options (`:scheduler`, `:stack-size`, `:lifecycle-handler`)
  and JVM-only helpers (`log`, `recur-swap`, the `actor`/`defactor` macros,
  `->Initializer`, `actor-builder`) have no counterpart.

## Not here yet

Erlang links and bidirectional exit signals (one-way monitors are covered by
`watch!`). Remote nodes and distribution. OTP applications and supervision
restart ordering with terminate callbacks. Hot code reload. Fibers do their own
scheduling, so there is no reduction budget or preemption either.

## Tests

Tests run on jolt. Specs live in `test/ensemble/*_spec.clj` and are checked with
[writ](https://github.com/jlt-commons/writ), which runs the static, law and proof
gates. Behaviour tests live in `test/ensemble/*_test.clj` and run under
`clojure.test`. The actor layer is effectful, so it is exercised by the behaviour
tests rather than by a law.

```
jolt -M:test
```

The suite prints a line per spec and per test namespace, then a summary, and
exits nonzero if anything failed.

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
name. `e/join` blocks until the actor settles and rethrows whatever it threw.

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
first message arrives.

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
`call-handler!` talks to one handler by id.

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
```

Strategies are `:one-for-one`, `:one-for-all` and `:rest-for-one`. A child's
restart type decides whether its exit earns a restart: `:permanent` always,
`:transient` only on an abnormal exit, `:temporary` never. If restarts exceed
`max-restarts` within `max-seconds`, the supervisor shuts down.

## Not here yet

Erlang links, monitors and exit signals. Remote nodes and distribution. OTP
applications and supervision restart ordering with terminate callbacks. Hot code
reload. Fibers do their own scheduling, so there is no reduction budget or
preemption either.

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

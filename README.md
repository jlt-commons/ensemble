# ensemble

Erlang processes and the OTP behaviours for [jolt](https://github.com/jolt-lang/jolt),
on jolt's native fibers. The goal is Erlang's semantics: links, exit signals,
monitors, selective receive, gen_server, gen_statem, gen_event, supervisors
and applications behave as the Erlang/OTP docs describe. Distribution (nodes,
remote pids) is not part of this first pass.

## Depend on it

Not published yet, so point at the git repo:

```clojure
{:deps {io.github.jlt-commons/ensemble
        {:git/url "https://github.com/jlt-commons/ensemble"
         :git/sha "PUT-A-SHA-HERE"}}}
```

It needs a jolt with fiber interrupts (`jolt.fibers/interrupt!`, merged in
jolt-lang/jolt#1165); until a release has them, run a checkout of jolt main.

## Processes

```clojure
(require '[ensemble.actor :as act :refer [receive]])

(def echo
  (act/spawn (fn [] (loop [] (receive [[from msg] (act/! from msg) (recur)])))))
```

`spawn` runs a fn on a fiber and returns the actor. `!` never blocks; a
message to a dead actor is dropped, and a message to an unregistered name
throws, as `Name ! Msg` does. `spawn` takes `:name`, `:link` (spawn_link),
`:trap` and `:state`; `spawn-link` and `spawn-monitor` are the OTP shapes.

### Receive

`receive` is Erlang's selective receive: the oldest message some clause takes
is removed, every other message stays where it was.

```clojure
(receive
  [[:reply ref v] v]                 ; ref already bound: matches its value
  [[:add n] :when (pos? n) (add n)]  ; a guard; one that throws is false
  [[x x] :same]                      ; a repeated name must match equal values
  [msg (log msg)]                    ; binds the whole message
  [:after 1000 :timeout])            ; ms, 0 polls, nil waits forever
```

As in Erlang, a symbol bound where the `receive` is written is not a binder:
it matches the value it holds. That is how a reply is matched to its request.

### State

An actor keeps a state. `spawn-actor` defines an actor by how each message
changes it: `(handler state msg)` returns the next state, and effects, such as
sending, happen in the handler.

```clojure
(def counter
  (act/spawn-actor (fn [n [op x]]
                     (case op
                       :add (+ n x)
                       :get (do (act/! x n) n)))
                   0))

(act/! counter [:add 3])
(act/state counter) ;=> 3, read from outside like sys:get_state
```

Inside any actor, `(act/state)`, `(act/set-state! v)` and
`(act/update-state! f)` work on its state.

### Links, exit signals and monitors

- `link!` / `unlink!`: bidirectional. When a process exits, every linked
  process gets an exit signal with its reason.
- A process that is not trapping exits dies with the reason, unless it is
  `:normal`. With `(trap-exit! true)` it receives `[:EXIT from reason]`
  instead, including `:normal`.
- `(exit! reason)` exits the current process (catchable, like `exit/1`).
  `(exit! actor reason)` sends a signal, like `exit/2`: `:kill` cannot be
  trapped and the target exits with `:killed`, which its own links see as an
  ordinary, trappable reason. `:normal` sent to yourself exits you.
- Linking to a dead process gives `:noproc`.
- `monitor!` returns a ref; the watcher gets `[:DOWN ref :process actor
  reason]`, with `:noproc` if the actor was already dead. `demonitor!` with
  `{:flush true}` also removes a delivered DOWN.
- `register!` fails if the name is taken or the actor already has one, and a
  name is released when its actor exits.

`join` and `exit-reason` wait for an actor from outside the actor world,
such as a test or the REPL.

## gen_server

```clojure
(require '[ensemble.gen-server :as gs])

(defrecord Counter []
  gs/Server
  (init [_] 0)
  (handle-call [_ req _from n]
    (case (first req)
      :add (let [n (+ n (second req))] [:reply n n])
      :get [:reply n n]))
  (handle-cast [_ _ n] [:noreply (inc n)])
  (handle-info [_ _ n] [:noreply n])
  (handle-timeout [_ n] [:noreply n])
  (terminate [_ _reason _n] nil))

(def c (gs/start (->Counter) {:name :counter}))
(gs/call! :counter [:add 3]) ;=> 3
```

- `start` and `start-link` are synchronous: they return once `init` has, and
  throw if it throws. A failed `start-link` does not take its caller down.
- The callbacks return OTP's shapes: `[:reply r st]`, `[:noreply st]`,
  `[:stop reason st]`, `[:stop reason reply st]`, each optionally with a
  timeout (ms or `:infinity`). Anything else stops the server with
  `[:bad-return-value ret]`.
- `[:stop reason st]` runs `terminate` and exits with `reason`, which is what
  links, monitors and supervisors see. A callback that throws does the same,
  with the throwable as the reason.
- `call!` monitors the server: a dead server fails at once with `:noproc`,
  one that dies mid-call fails with its reason, and after the timeout
  (default 5000 ms) it fails with `:timeout`. A late reply is dropped, as
  OTP's aliases do. The failure is an ex-info whose `:reason` says which.
- A server started with `{:trap true}` whose parent exits runs `terminate`
  with the parent's reason. That is how a supervisor stops it cleanly.
- `stop!` stops a server and waits for it. `reply!` answers a call later,
  from anywhere.

## gen_statem

`ensemble.gen-statem` is OTP's gen_statem in handle_event_function mode. One
callback, `(handle-event this type content state data)`, sees every event:
`[:call from]`, `:cast`, `:info`, `:timeout`, `:state-timeout`,
`[:generic-timeout name]`, `:internal` and `:enter`.

```clojure
(require '[ensemble.gen-statem :as sm])

(defrecord Door []
  sm/Machine
  (init [_] [:locked nil])
  (handle-event [_ type content state data]
    (case [state type]
      [:locked :cast]        [:next-state :open data [[:state-timeout 1000 :lock]]]
      [:open :state-timeout] [:next-state :locked data]
      [:open :cast]          [:keep-state-and-data [:postpone]]
      [:keep-state-and-data]))
  (terminate [_ _ _ _] nil))
```

Results are `:next-state`, `:keep-state`, `:keep-state-and-data`, `:stop`
and `:stop-and-reply`. Actions are `:postpone`, `:next-event`, `:reply`,
`:timeout`, `:state-timeout` and `:generic-timeout`. A postponed event is
retried after the next state change. Inserted events run before everything
else. A state timeout is cancelled by a state change, and the event timeout
by any event. `{:state-enter true}` turns on enter calls. Clients use
`gs/call!`, `gs/cast!` and `gs/stop!`, since a gen-statem speaks the same
protocol as a gen-server.

gen_fsm is not provided; OTP deprecated it in favour of gen_statem.

## gen_event

`ensemble.gen-event`: a manager hands each event to its handlers in order.
`h-handle-event` returns `[:ok state]` or `:remove-handler`. A handler that
throws is removed, with `h-terminate` called on `[:error reason]`, and the
manager and the other handlers carry on. `add-sup-handler!` ties a handler to
the calling actor both ways. `call!` talks to one handler (`:bad-module` if
there is none), and `delete-handler!` returns what `h-terminate` returned.

## Supervisors

```clojure
(require '[ensemble.supervisor :as sup])

(def tree
  (sup/start {:strategy :one-for-all :intensity 3 :period 5}
             [{:id :db  :start #(gs/start-link (->Db) {:trap true})}
              {:id :web :start #(gs/start-link (->Web) {:trap true}) :shutdown 2000}]))
```

- Child specs take `:id`, `:start`, `:restart` (`:permanent`, `:transient`,
  `:temporary`), `:shutdown` (ms, `:brutal-kill`, `:infinity`) and `:type`
  (`:worker`, `:supervisor`).
- Children start in order, and `start` throws if one fails, stopping those
  already started.
- On a restart, `:one-for-all` and `:rest-for-one` first stop the affected
  siblings in reverse start order, then start them again in start order. A
  `:temporary` sibling they stop is not restarted.
- A child is stopped as OTP does it: unlinked, sent `:shutdown`, and killed
  if it hasn't exited after its `:shutdown` time.
- More than `:intensity` restarts in `:period` seconds (defaults 1 and 5, as
  in OTP) and the supervisor stops its children and exits with `:shutdown`.
- The API is `start-child!`, `terminate-child!`, `restart-child!`,
  `delete-child!`, `which-children`, `count-children`, `child` and `stop!`.
  Supervisors nest through `start-link` and `:type :supervisor`.

## Applications

`ensemble.application`: `load!` a spec `{:name :start :stop :applications
:type}`, where `:start` returns the top supervisor. `start!` requires its
dependencies to be running, `ensure-all-started!` starts them first, and
`stop!` stops the tree, then runs `:stop`.

If the tree exits on its own, the type decides what happens next. A
`:temporary` application is only recorded (see `exits`). A `:permanent` one,
or a `:transient` one that exited abnormally, stops every other application,
where OTP would stop the node.

## Where this differs from Erlang

- **Exit signals land wherever the process is**, through jolt's fiber
  interrupts (`jolt.fibers/interrupt!`): a process in a long computation,
  or parked on a core.async channel of its own, dies at once, and a `kill`
  it catches still kills it at its next receive. The bookkeeping of a dying
  process -- telling its links and monitors -- runs masked, so a late
  signal cannot tear it.
- **No distribution**, no hot code loading, no `sys` suspend/resume, and no
  reductions (jolt preempts fibers on a timer instead).
- **Reasons** are any value, and a crash's reason is the throwable itself
  rather than `{Exception, Stacktrace}`.
- **Names** may be keywords, symbols or strings, all normalised to a keyword.

## Contracts

The decisions the runtime makes are pure functions with
[writ](https://github.com/jlt-commons/writ) specs in `test/ensemble/*_spec.clj`,
written from the Erlang/OTP documentation:

| namespace | decides | spec |
|---|---|---|
| `ensemble.signal` | what an exit signal does to a process | `signal_spec` |
| `ensemble.select`, `ensemble.match`, `ensemble.pattern` | which message a receive takes, how a pattern binds | `select_spec`, `match_spec`, `pattern_spec` |
| `ensemble.order` | restart types, strategy plans, restart intensity | `order_spec` |
| `ensemble.callback` | what a gen_server callback's return means | `callback_spec` |
| `ensemble.statem` | gen_statem results, actions, postpone order, timeouts | `statem_spec` |

Every spec requires proof (`{:require :proved}`): 176 of their 181 laws
are proved, 92 of them for every input -- among them that a selective
receive is the manual's, message by message and clause by clause, that a
restart plan is the supervisor docs', and that no gen_statem event is lost
or duplicated, over mailboxes, children and queues of any length. The 5
left to testing each say why: they recurse over patterns of any depth, or
over a callback's list of actions. `test/ensemble/order_proof.clj` holds
the lemmas about clojure.core the supervisor's proofs cite.

A law over `Any` covers values without NaN; one over `Any!` takes NaN in
and compares with writ's `same`, where a NaN is the same as a NaN. Erlang
has no NaN, and Clojure's `=` says NaN is not NaN; the matcher compares
as `=` does but with a NaN the same as a NaN, so a literal NaN pattern
takes a NaN message, and a name bound twice matches two NaNs. The match
spec states both.

Each spec also states, with `calls`, that the effectful runtime goes through
these fns. For example, `ensemble.actor/handle-signal!` must reach
`ensemble.signal/on-signal`, and the supervisor must decide restarts through
`ensemble.order`. The laws therefore constrain the code that actually runs, not
a model beside it. The concurrent behaviour itself (races, delivery, timing)
is covered by the tests in `test/ensemble/*_test.clj`.

```
jolt -M:test
```

Rough throughput numbers for spawning, messaging, selective receive and
gen_server calls:

```
jolt -M:bench
```

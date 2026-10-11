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

It needs a jolt with fiber kills (`jolt.fibers/kill!`), which no `catch`
can stop, and fiber pools (`jolt.fibers/pool`) for dispatchers: the
nightly build (`install --version nightly`) until a release after 0.8.20
has them. CI runs against the nightly.

## Processes

```clojure
(require '[ensemble.actor :as act :refer [receive]])

(def echo
  (act/spawn (fn [] (loop [] (receive [[from msg] (act/! from msg) (recur)])))))
```

`spawn` runs a fn on a fiber and returns the actor. `!` never blocks; a
message to a dead actor is dropped, and a message to an unregistered name
throws, as `Name ! Msg` does. `spawn` takes `:name`, `:link` (spawn_link),
`:trap`, `:state` and `:dispatcher` (below); `spawn-link` and
`spawn-monitor` are the OTP shapes.

`(act/hibernate! f & args)` is `erlang:hibernate/3`. The process gives up
its fiber and call stack, but stays alive with its pid, name, links,
monitors and mailbox. The next message or exit signal runs `(apply f
args)` on a new fiber, and a message that's already waiting wakes it at
once. `hibernating?` tells whether a process is hibernating.

`(act/passivate! f & args)` goes a step beyond Erlang and hibernates to
disk. What the process will run next is written with `jolt.image`, and
only its identity and mailbox stay in memory. The next message or signal
reads the image back, removes the file and resumes, exactly as for
`hibernate!`. A local actor in the state is written as its pid and comes
back as the live actor. A reference the process might share with others
(an atom, a channel, a promise) can't be written without splitting it
from them. In that case the process stays hibernated in memory, and
`passivation-refused` says why. gen_server's `:passivate-after` start
option passivates a server after that many ms without a message.

### Receive

`receive` is Erlang's selective receive: the oldest message some clause takes
is removed, every other message stays where it was.

```clojure
(receive
  [[:reply ref v] v]                 ; ref already bound: matches its value
  [[:add n] :when (pos? n) (add n)]  ; a guard; one that throws is false
  [[x x] :same]                      ; a repeated name must match equal values
  [[:args h & t] (run h t)]          ; a list tail, Erlang's [H|T]
  [{:op :put :at [x y]} (put x y)]   ; a map holding these keys, as #{k := V}
  [msg (log msg)]                    ; binds the whole message
  [:after 1000 :timeout])            ; ms, 0 polls, nil or :infinity waits forever
```

As in Erlang, a symbol bound where the `receive` is written is not a binder:
it matches the value it holds. That is how a reply is matched to its request.
A map pattern's keys are values or bound names, never binders. A tuple
pattern matches any sequential message, a list as well as a vector.

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
  reason]`, with `:noproc` if the actor was already dead. A monitor made by
  name names it in the DOWN as `[:At name node]`, Erlang's `{Name, Node}`,
  and is down at once with `:noproc` if no process holds the name.
  `demonitor!` with `{:flush true}` also removes a delivered DOWN.
- A ref (`make-ref`) carries the node that made it. Wherever a timeout is
  taken, `nil` and `:infinity` both wait forever.
- `register!` fails if the name is taken or the actor already has one, and a
  name is released when its actor exits. A name `[:via registry nm]` goes
  through any `ensemble.registry/Registry`, and `[:global nm]` through
  `ensemble.global`, wherever a name is taken.
- `!` also sends to an alias (`alias!`), while the alias is active.
- `process-info` gives a local actor's registered name, status
  (`:running`, `:waiting`, `:hibernating`, `:passivated`), message queue
  and its length, links, monitors, monitored-by, trap-exit flag, initial
  call, ancestors and dispatcher; `processes` lists the node's live actors. On a pid of
  another node, `process-info` and `alive?` are a badarg, as in Erlang.

`join` and `exit-reason` wait for an actor from outside the actor world,
such as a test or the REPL.

### Dispatchers

An actor runs on a fiber, and a fiber stays on its carrier (an OS thread)
for life. A receive, a channel op, a deref or a sleep parks the fiber and
frees the carrier. A blocking foreign call, such as a C database driver,
doesn't, so every actor on that carrier waits with it. A long computation
gets preempted, but it still takes its share of the carrier from the
actors next to it. As in Akka, a dispatcher keeps that kind of work on
carriers of its own:

```clojure
(require '[ensemble.dispatcher :as disp])

(disp/define! :db {:size 8})

(act/spawn query-loop {:dispatcher :db})
(gs/start (->Repo) {:dispatcher :db})
```

`:default` is jolt's carrier pool, with one carrier per processor, and it
is what every actor runs on unless told otherwise. `:blocking` has 16
carriers for blocking calls, like Akka's `default-blocking-io-dispatcher`.
A dispatcher's carriers start the first time an actor is spawned on it,
and until then it can be redefined, so `:blocking` can be resized at
startup. Children don't inherit their parent's dispatcher.
gen_server, gen_statem and gen_event take `:dispatcher` as a start option;
under a supervisor, pass it in the child's start fn. Messages, links,
monitors and exit signals work across dispatchers as they do within one.
`(disp/shutdown! name)` stops new spawns on a dispatcher, and its carriers
end once the actors already on them have exited. A hibernating actor whose
dispatcher was shut down wakes on `:default`.

## gen_server

```clojure
(require '[ensemble.gen-server :as gs])

(defrecord Counter []
  gs/Server
  (init [_] [:ok 0])
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

- `init` returns OTP's shapes: `[:ok st]`, `[:ok st action]`, `[:stop
  reason]`, `[:error reason]` or `:ignore`. `start` and `start-link` are
  synchronous: they return the server once `init` has, return `:ignore` for
  `:ignore`, and throw `{:reason r}` for a stop, an error, a throw or a bad
  return. The process then exits (`:normal` for `:error` and `:ignore`), and
  a failed `start-link` does not take its caller down. The `:timeout`
  option is how long `init` may take (default `:infinity`): a slower one
  is killed and the start throws `{:reason :timeout}`, as OTP's
  `{timeout, T}`.
- The callbacks return OTP's shapes: `[:reply r st]`, `[:noreply st]`,
  `[:stop reason st]`, `[:stop reason reply st]`, each optionally with an
  action: a timeout (ms or `:infinity`), `:hibernate`, or `[:continue c]`,
  which runs `handle-continue` with `c` before the server takes another
  message. Anything else stops the server with `[:bad-return-value ret]`.
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
- A `:hibernate` action hibernates the server until its next message, and
  the `:hibernate-after` start option does the same after that many ms
  without one. In gen_statem, `:hibernate` is a transition action,
  `:hibernate-after` is a start option there too, and a hibernating
  machine still gets its timeouts.
- `:name` may be `[:via registry nm]` or `[:global nm]`, and `call!` and
  `cast!` take the same names. `multi-call` calls a locally registered name
  on several nodes at once and answers `[replies bad-nodes]`; `abcast`
  casts to it on each.
- `enter-loop` makes the calling actor a server without `init`, as
  `gen_server:enter_loop`.
- A server implementing `FormatStatus` decides how its state shows in
  `sys/get-status` and in terminate reports, to leave a secret out.

## gen_statem

`ensemble.gen-statem` is OTP's gen_statem. In handle_event_function mode
one callback, `(handle-event this type content state data)`, sees every
event: `[:call from]`, `:cast`, `:info`, `:timeout`, `:state-timeout`,
`[:generic-timeout name]`, `:internal` and `:enter`. A machine that also
implements `StateFunctions` runs in state_functions mode: `(state-functions
this)` maps each state to a fn of `type content data`.

```clojure
(require '[ensemble.gen-statem :as sm])

(defrecord Door []
  sm/Machine
  (init [_] [:ok :locked nil])
  (handle-event [_ type content state data]
    (case [state type]
      [:locked :cast]        [:next-state :open data [[:state-timeout 1000 :lock]]]
      [:open :state-timeout] [:next-state :locked data]
      [:open :cast]          [:keep-state-and-data [:postpone]]
      [:keep-state-and-data]))
  (terminate [_ _ _ _] nil))
```

Results are `:next-state`, `:keep-state`, `:keep-state-and-data`,
`:repeat-state`, `:repeat-state-and-data` (which run the enter call again),
`:stop` and `:stop-and-reply`. Actions are `:postpone`, `:next-event`, `:reply`,
`:timeout` (or a bare time), `:state-timeout`, `:generic-timeout` and
`:hibernate`. One action may stand alone where a list goes, as
`[:keep-state-and-data [:reply from v]]`. A bad return stops the machine
with `[:bad-return-from-state-function ret]`, and an action that is none
of these with `[:bad-action-from-state-function a]`, the reasons OTP
gives. A postponed event is
retried after the next state change. Inserted events run before everything
else. A state timeout is cancelled by a state change, and the event timeout
by any event. Every timeout takes `:cancel` (`[:state-timeout :cancel]`)
and `:update` (`[:generic-timeout name :update content]`, which keeps the
running timer's deadline), and an options map `{:abs true}` makes its time
a monotonic deadline. `{:state-enter true}` turns on enter calls, and
`enter-loop` makes the calling actor a machine. Clients use
`gs/call!`, `gs/cast!` and `gs/stop!`, since a gen-statem speaks the same
protocol as a gen-server.

gen_fsm is not provided; OTP deprecated it in favour of gen_statem.

## gen_event

`ensemble.gen-event`: a manager hands each event to its handlers in order.
`h-handle-event` returns `[:ok state]`, `[:ok state :hibernate]` (the
manager hibernates), `:remove-handler`, or `[:swap-handler reason state id
f]`. A handler that throws is removed, with `h-terminate` called on
`[:error reason]`, and the manager and the other handlers carry on. A
handler that also implements `InfoHandler` gets the manager's other
messages through `h-handle-info`. `add-sup-handler!` ties a handler to the
calling actor both ways, and the link goes with the actor's last handler.
`swap-handler!` and `swap-sup-handler!` replace a handler as OTP's do: the
old one's `h-terminate` runs, and `(f what-it-returned)` is the new one.
`call!` talks to one handler (`:bad-module` if there is none), and
`delete-handler!` returns what `h-terminate` returned.

## sys

`ensemble.sys` looks into a running gen-server or gen-statem, and so into
a supervisor or a gen-event manager, through system messages it answers
between its own: `get-state`, `replace-state!`, `get-status`, `suspend!`
and `resume!` (a suspended process takes only system messages),
`statistics`, `trace!` (each message it takes is reported) and
`terminate!`. A gen-statem's state there is `[state data]`.

As proc_lib does, every actor records its initial call (`spawn`'s
`:initial-call`, which the behaviours set to `[module :init]`) and its
ancestors, and a crash report carries both.

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
- Flags and child specs are checked as `supervisor:check_childspecs`
  does, and a bad one is refused with OTP's reason: `start` throws
  `{:reason [:supervisor-data r]}` or `[:start-spec r]`, and
  `start-child!` returns `[:error r]`. `get-childspec` returns a spec
  with its defaults filled in.
- `:simple-one-for-one` takes one spec as a template, and each
  `(start-child! sup args)` starts a child from it with `args` appended to
  its start fn's arguments. These children have no ids: they are
  terminated by pid, cannot be restarted or deleted by id, and are
  restarted alone. When the supervisor stops, they are all sent
  `:shutdown` at once and waited for under one shutdown time.
- A child may be `:significant`. With `:auto-shutdown :any-significant`
  the supervisor shuts down when any significant child ends without being
  restarted. With `:all-significant` it shuts down when the last one ends.
- The API is `start-child!`, `terminate-child!`, `restart-child!`,
  `delete-child!`, `which-children`, `count-children`, `get-childspec`,
  `child` and `stop!`. Supervisors nest through `start-link` and
  `:type :supervisor`.

## Applications

`ensemble.application`: `load!` a spec `{:name :start :stop :applications
:type}`, where `:start` returns the top supervisor. `start!` requires its
dependencies to be running, `ensure-all-started!` starts them first, and
`stop!` stops the tree, then runs `:stop`.

- `:env` is the application's configuration: `get-env`, `get-all-env`,
  `set-env!` and `unset-env!`. A value set before a reload is kept.
  `get-key` reads the loaded spec.
- `:start-phases` `[[phase args] ...]` run in order after `:start`, each as
  `(start-phase phase :normal args)`; one that doesn't answer `:ok` stops
  the tree and fails the start.
- `:prep-stop` runs before the tree stops, and `:stop` gets what it
  returns.
- `:optional-applications` may be absent: one not loaded is skipped.
  `:included-applications` must be loaded, and can't be started on their
  own while their includer runs.
- `ensure-all-started!` stops what it started, the last first, when a
  later start fails, as OTP 26's does.

If the tree exits on its own, the type decides what happens next. A
`:temporary` application is only recorded (see `exits`). A `:permanent` one,
or a `:transient` one that exited abnormally, stops every other application,
the last started first, where OTP would stop the node.

## Reports

`ensemble.logger` gets what OTP's logger gets from the runtime: a crash
report when an actor's body throws something it didn't catch, a report when
a gen-server or gen-statem stops abnormally (with its last message or
event, and its state), and supervisor reports (`:child-terminated`,
`:start-error`, `:shutdown-error`, and `:shutdown` when it reaches its
restart intensity). An orderly end is never reported, nor is an actor's
own `(exit! reason)`. Reports print to `*err*` until `set-handler!`
installs another handler.

## Timers

```clojure
(require '[ensemble.timer :as timer])

(def ref (timer/send-after 1000 (act/self) :wake-up))   ; erlang:send_after
(timer/start-timer 500 :worker :tick)                   ; sends [:timeout ref :tick]
(timer/cancel-timer ref)                                ; ms left, or false
(timer/send-interval 100 :tick)                         ; timer:send_interval to self
```

- A timer to a pid is cancelled when that process exits. A timer to a
  name looks the name up when it fires, and its message is dropped if no
  process holds the name.
- `cancel-timer` and `read-timer` answer the time left, or `false` once
  the timer has fired or been cancelled. `{:async true}` answers with a
  `[:cancel-timer ref left]` message instead. `{:abs true}` takes a
  `monotonic-time` deadline.
- `apply-after`, `send-interval`, `apply-interval`, `exit-after`,
  `kill-after` and `cancel` are the `timer` module's. An interval fires at
  fixed deadlines, and ends when the process that started it exits.
- Each node has one timer server, started on first use, and every
  decision it makes is in `ensemble.timers`.

## Distribution

A process handle is anything that implements `ensemble.process/Process`:
deliver a message, send an exit signal, link, unlink, monitor, demonitor.
Local actors implement it directly. A pid on another node is an
`ensemble.node/RemotePid`, which sends each operation as a frame to its
node. `!`, `link!`, `monitor!`, `exit!`, gen_server calls and supervisors
work unchanged whether the other process is in this VM, in another OS
process or on another machine.

```clojure
(require '[ensemble.node :as node])

(node/start! :shop.a (node/loopback))
(node/start! :shop.b (node/loopback))

(node/with-node :shop.a
  (let [p (node/spawn-on :shop.b `my.ns/worker [] {:link true})]
    (act/! p [:job 1])                          ; by pid
    (act/! [:At :registry :shop.b] [:hello])    ; {Name, Node}
    (gs/call! [:At :counter :shop.b] [:get])))
```

- A pid names its node. Pids inside a message cross as data and arrive as
  pids again: a node's own pid as its local process, any other as a
  `RemotePid`.
- Nodes connect on first contact. `connect!`, `disconnect!` and
  `connected` manage connections explicitly.
- When a connection is lost, links across it break with `:noconnection`,
  monitors across it fire `[:DOWN ref :process pid :noconnection]`, and
  each `monitor-node!` gets `[:nodedown node]`. A link to a remote pid
  that is gone gives `:noproc`.
- `spawn-on` starts a fn named by a symbol on the other node, as
  `spawn(Node, M, F, A)` does, if that node allows it: `start!`'s `:spawn`
  option is a set of namespaces or a predicate on the symbol, and without
  it a node starts nothing for a peer.
- `monitor-nodes!` sends the calling actor `[:nodeup n]` and `[:nodedown
  n]` for every connection of its node, as `net_kernel:monitor_nodes`.
  `act/nodes` lists the connected nodes. `stop!` takes a node down.
- A node name is a keyword that reads back as itself, like `:shop.host`.
  Erlang's `shop@host` doesn't work here: `@` ends a token in Clojure's
  reader, and edn rejects it.

The layers, bottom up:

- **Transport** (`ensemble.transport`): a byte stream between two nodes,
  `Transport` (`-listen`, `-connect`), `Listener` and `Conn` (`-start`,
  `-write`, `-close`). That is all a transport library implements, for TCP,
  TLS or anything else; it resolves node names to addresses however it
  likes. The loopback transport joins nodes in one VM, and `{:chunk n}`
  splits its stream the way a network might.
- **Framing and codec**: a frame is its length, four bytes big-endian,
  then its payload, which a `Codec` (`ensemble.codec`, EDN by default,
  `start!`'s `:codec`) turns into a value. A frame larger than `start!`'s
  `:max-frame` (64 MB by default) drops the connection, as does one that
  doesn't decode.
- **Envelope**: each frame after the handshake is `[op hdr body]`. `hdr`
  holds the operation's own fields (the pids it names, a ref, a name) and
  `body` the data it carries, a message or an exit reason. Pids and
  throwables in the body are tagged and data that looks like a tag is
  escaped, so a message arrives exactly as it was sent. Sending something
  the wire can't carry (a record, an atom, a fn) throws at the sender; a
  throwable arrives as an `ex-info` with its message and data.
- **Handshake**: a connection is used only after the handshake, Erlang's
  shape: the connecting node sends its name and creation, the other answers
  with a status (`:nok` for a name it won't take, `:alive` when it's
  connected to that run of the node already, and of two nodes connecting
  to each other at once only one connection survives) and a challenge, and each end proves it holds
  the cookie by signing the other's challenge with HMAC-SHA256. The
  signature covers both node names as well as the challenge, unlike
  Erlang's, so a node without the cookie can't relay one node's answer to
  a third and get in under its name. `start!`'s
  `:cookie` sets it (by default nodes in one VM share a cookie drawn at
  startup), or `:auth` takes any `ensemble.node/Auth`. A connection that
  hasn't shaken hands within `:handshake-timeout` is closed, and a node
  that restarts is let in, its earlier run going down first.
- **Creation**: each start of a node draws a creation number that its pids
  and refs carry, so a pid of an earlier run names no process, though its
  id may be in use again.

### Services

```clojure
(require '[ensemble.rpc :as rpc] '[ensemble.global :as global] '[ensemble.pg :as pg])

(rpc/call :shop.b `my.ns/total [order] 5000)    ; erpc:call
(rpc/multicall [:shop.a :shop.b] `my.ns/stats [])
(global/register-name :leader (act/self))        ; :yes or :no
(gs/call! [:global :leader] :status)
(pg/join :workers (act/self))
(pg/get-members :workers)                        ; on every node
```

- `ensemble.rpc` is erpc: `call` runs a fn in a process of its own on the
  node (one its `:spawn` option allows) and answers its value, or throws
  with `:noconnection`, `:timeout` (the process is then killed),
  `[:exception e]` or `[:exit reason]`. `cast` doesn't wait, and
  `multicall` asks several nodes at once.
- `ensemble.global` is global: one name table over every connected node.
  Registering locks the name on each node, checks no node has it, and sets
  it everywhere. A name goes when its process exits or its node goes
  down. When nodes connect they merge tables, and a name both hold keeps
  the process whose pid sorts first, killing the other, as global's
  default resolve does. `[:global nm]` works wherever a name does.
- `ensemble.pg` is pg: process groups over every connected node, joined
  from the member's own node, kept eventually consistent as pg's are.
  `join`, `leave`, `get-members`, `get-local-members`, `which-groups`, and
  `monitor`, which sends `[ref :join group pids]` and `[ref :leave group
  pids]`.

## Where this differs from Erlang

- **Exit signals land wherever the process is**, through jolt's fiber
  kills (`jolt.fibers/kill!`): a process in a long computation, or parked
  on a core.async channel of its own, dies at once. As in Erlang a signal
  is not an exception, so no `catch` in the body stops it; unlike Erlang,
  the body's `finally` blocks run on the way out. `(exit! reason)`, a
  process exiting itself, is a throw it may catch, as `exit/1` is. The
  bookkeeping of a dying process -- telling its links and monitors -- runs
  masked, so a late signal cannot tear it.
- **Passivation is an extension.** Erlang's hibernation stays in memory.
- **Distribution ships only the loopback transport**; a network transport
  is a library implementing `ensemble.transport`. The cookie handshake
  signs with HMAC-SHA256 over the challenge and both names rather than
  Erlang's MD5 digest of the challenge, and the frames
  are EDN, not the external term format, so ensemble nodes do not talk to
  Erlang nodes. global and pg have the default scope only.
- There is no hot code loading, and no reductions (jolt preempts fibers on
  a timer instead).
- **Dispatchers** come from Akka; Erlang has a single scheduler pool, plus
  dirty schedulers for NIFs.
- **Reasons** are any value, and a crash's reason is the throwable itself
  rather than `{Exception, Stacktrace}`.
- **Names** may be keywords, symbols or strings, all normalised to a keyword
  that keeps the namespace: `:a/b`, `'a/b` and `"a/b"` are one name, `:a/b`
  and `:c/b` are two.

## Contracts

The decisions the runtime makes are pure functions with
[writ](https://github.com/jlt-commons/writ) specs in `test/ensemble/*_spec.clj`,
written from the Erlang/OTP documentation:

| namespace | decides | spec |
|---|---|---|
| `ensemble.signal` | what an exit signal does to a process | `signal_spec` |
| `ensemble.select`, `ensemble.match`, `ensemble.pattern` | which message a receive takes, how a pattern binds | `select_spec`, `match_spec`, `pattern_spec` |
| `ensemble.order` | restart types, strategy plans, restart intensity, auto-shutdown | `order_spec` |
| `ensemble.childspec` | which supervisor flags and child specs are valid, with their defaults; what an ignored start keeps; which child operations are allowed | `childspec_spec` |
| `ensemble.timers` | when timers fire and in what order, what a cancel answers, which timers an exit ends | `timers_spec` |
| `ensemble.request` | which message answers an asynchronous request, alone or in a collection | `request_spec` |
| `ensemble.life` | a process's life: running, hibernated, on disk, exited | `life_spec` |
| `ensemble.dist` | where a send goes, what a pid in a message is on arrival, what a lost connection does, every step of the handshake, the frame header | `dist_spec` |
| `ensemble.callback` | what a gen_server's init and callback returns mean, and their actions | `callback_spec` |
| `ensemble.statem` | gen_statem init and results, actions, postpone order, timeouts | `statem_spec` |

Every spec requires proof (`{:require :proved}`): 342 of their 356 laws
are proved, 223 of them for every input -- among them that a selective
receive is the manual's, message by message and clause by clause, that a
restart plan is the supervisor docs', that a child spec is refused for
the reason `check_childspecs` gives, that a lost connection breaks exactly
the links and monitors across it, and that no gen_statem event is lost
or duplicated, over mailboxes, children and queues of any length, and
that two nodes come up only when both hold the cookie. The 14 left to
testing each say why: they recurse over patterns of any depth, over a
callback's list of actions, walk a message of any shape for the pids in
it, or shift bits, which the prover doesn't model. `test/ensemble/order_proof.clj` and
`timers_proof.clj` hold the lemmas about clojure.core the proofs cite.

A law over `Any` covers values without NaN; one over `Any!` takes NaN in
and compares with writ's `same`, where a NaN is the same as a NaN. Erlang
has no NaN, and Clojure's `=` says NaN is not NaN; the matcher compares
as `=` does but with a NaN the same as a NaN, so a literal NaN pattern
takes a NaN message, and a name bound twice matches two NaNs. The match
spec states both.

Each spec also states, with `calls`, that the effectful runtime goes through
these fns. For example, `ensemble.actor/handle-signal!` must reach
`ensemble.signal/on-signal`, the supervisor must decide restarts through
`ensemble.order` and validate specs through `ensemble.childspec`, and every
operation `ensemble.node` sends to a remote pid must go through
`ensemble.dist/route`. The laws therefore constrain the code that actually runs, not
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

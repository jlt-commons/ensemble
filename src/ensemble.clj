(ns ensemble
  "Public API for ensemble: Erlang-style actors, the OTP behaviours and
  supervisors, all on jolt fibers.

  Everything here is a thin re-export so a caller can require one namespace:

      (require '[ensemble :as e])

      (def a (e/spawn (fn [] (e/receive [[:ping from] (e/! from :pong)]))))

  The layers are also usable directly:

      ensemble.actor       actors: spawn, send, selective receive
      ensemble.gen-server  gen_server behaviour: call/cast/info/timeout
      ensemble.gen-event   gen_event behaviour: manager + handlers
      ensemble.gen-fsm     gen_fsm behaviour: named states + events
      ensemble.supervisor  supervision trees: restart strategies

  Patterns in receive are the compiled forms of ensemble.pattern: a symbol
  binds the whole message, _ matches anything, literals match themselves, and
  a vector pattern matches a message element by element."
  (:require [ensemble.actor :as act]
            [ensemble.gen-server :as gs]
            [ensemble.gen-event :as ge]
            [ensemble.gen-fsm :as gf]
            [ensemble.supervisor :as sup]))

;; actors ---------------------------------------------------------------

(def spawn act/spawn)
(def ! act/!)
(def !! act/!!)
(def receive-match act/receive-match)
(def receive-timed act/receive-timed)
(def self act/self)
(def join act/join)
(def state act/state)
(def set-state! act/set-state!)
(def done? act/done?)
(def register! act/register!)
(def whereis act/whereis)
(def unregister! act/unregister!)
(def vref act/vref)
(def maketag act/maketag)
(def mailbox-of act/mailbox-of)
(def watch! act/watch!)
(def unwatch! act/unwatch!)
(def link! act/link!)
(def unlink! act/unlink!)
(def trap-exit! act/trap-exit!)
(def exit! act/exit!)

(defmacro receive
  "Selective receive over the current actor's mailbox.  See ensemble.actor."
  [& clauses]
  `(act/receive ~@clauses))

;; gen_server -----------------------------------------------------------

(def gen-server gs/gen-server)
(def reply! gs/reply!)
(def reply-error! gs/reply-error!)
(def call! gs/call!)
(def call-timed! gs/call-timed!)
(def cast! gs/cast!)
(def shutdown! gs/shutdown!)

;; gen_event ------------------------------------------------------------

(def start-manager ge/start-manager)
(def add-handler! ge/add-handler!)
(def remove-handler! ge/remove-handler!)
(def notify ge/notify)
(def sync-notify! ge/sync-notify!)
(def call-handler! ge/call-handler!)

;; gen_fsm --------------------------------------------------------------

(def start-fsm gf/start-fsm)
(def send-event! gf/send-event!)
(def sync-send-event! gf/sync-send-event!)

;; supervisor -----------------------------------------------------------

(def start-supervisor sup/start-supervisor)
(def start-child! sup/start-child!)
(def terminate-child! sup/terminate-child!)
(def get-child sup/get-child)
(def remove-child! sup/remove-child!)
(def remove-and-terminate-child! sup/remove-and-terminate-child!)
(def which-children! sup/which-children!)

;; behaviours (implement these on a record) -----------------------------

(def Server gs/Server)
(def Handler ge/Handler)
(def FSM gf/FSM)

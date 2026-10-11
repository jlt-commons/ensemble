(ns ensemble
  "Erlang processes and OTP behaviours on jolt fibers.

  This namespace re-exports the process primitives and the client side of
  the gen protocol (call!, cast!, reply!, stop!), which every behaviour
  speaks.  Each behaviour is used through its own namespace, as OTP's are
  modules, since they share names such as start and start-link:

      ensemble.actor        processes: spawn, !, receive, links, monitors
      ensemble.dispatcher   the pools of carriers actors run on
      ensemble.gen-server   gen_server
      ensemble.gen-statem   gen_statem
      ensemble.gen-event    gen_event
      ensemble.supervisor   supervisor
      ensemble.application  applications

  The decisions these make are pure namespaces with writ contracts:
  ensemble.signal (exit signals), ensemble.select (selective receive),
  ensemble.order (supervision), ensemble.callback (gen_server returns) and
  ensemble.statem (gen_statem transitions)."
  (:require [ensemble.actor :as act]
            [ensemble.gen-server :as gs]))

;; processes ------------------------------------------------------------

(def spawn act/spawn)
(def spawn-link act/spawn-link)
(def spawn-monitor act/spawn-monitor)
(def spawn-actor act/spawn-actor)
(def self act/self)
(def ! act/!)
(def receive-match act/receive-match)
(def make-ref act/make-ref)
(def state act/state)
(def set-state! act/set-state!)
(def update-state! act/update-state!)
(def alive? act/alive?)
(def register! act/register!)
(def unregister! act/unregister!)
(def whereis act/whereis)
(def registered act/registered)
(def link! act/link!)
(def unlink! act/unlink!)
(def trap-exit! act/trap-exit!)
(def exit! act/exit!)
(def monitor! act/monitor!)
(def demonitor! act/demonitor!)
(def join act/join)
(def exit-reason act/exit-reason)

(defmacro receive
  "Selective receive over the current actor's mailbox.  See ensemble.actor."
  [& clauses]
  `(act/receive ~@clauses))

;; the gen protocol -------------------------------------------------------

(def call! gs/call!)
(def cast! gs/cast!)
(def reply! gs/reply!)
(def stop! gs/stop!)

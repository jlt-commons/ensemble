(ns ensemble.life-spec
  "Contract for ensemble.life: a process's life, with hibernation, from
  erlang:hibernate/3.

  A running process may hibernate: it gives up its call stack and waits,
  alive -- its pid, name, links, monitors and mailbox stay -- until a
  message or an exit signal arrives, and then runs again from the fn it
  named, with the stack emptied.  A process with a message already waiting
  wakes at once.  Only a running process exits, and a process that has
  exited never runs again.

  Beyond Erlang, a hibernated process may be passivated: what it will run
  next goes to disk, and nothing of it but its identity and mailbox stays
  in memory.  A message or a signal wakes it as it wakes a hibernated one,
  from the disk."
  (:require [writ.spec :refer [spec data ann machine law calls]]
            [ensemble.life :as life]
            [ensemble.actor :as act]))

(spec ensemble.life {:require :proved})

(data Life Running Hibernated Passivated Dead)
(data Event Hibernate Passivate Mail Signal Exit)

(ann step [Life Event -> Life])

(machine life
  {:step step
   :start [:Running]
   :transitions {[:Running]    {[:Hibernate] [:Hibernated], [:Exit] [:Dead]}
                 [:Hibernated] {[:Mail] [:Running], [:Signal] [:Running], [:Passivate] [:Passivated]}
                 [:Passivated] {[:Mail] [:Running], [:Signal] [:Running]}}
   :final [[:Dead]]
   :never [[[:Dead] [:Running]] [[:Dead] [:Hibernated]] [[:Dead] [:Passivated]]]
   :before [[[:Running] [:Dead]] [[:Hibernated] [:Passivated]]]})

(law waking-is-only-from-hibernation
  (forall [l Life, e Event]
    (=> (= [:Running] (step l e))
        (or (= [:Running] l) (= [:Hibernated] l) (= [:Passivated] l)))))

(law only-a-hibernated-process-is-passivated
  (forall [l Life, e Event]
    (=> (= [:Passivated] (step l e))
        (or (= [:Hibernated] l) (= [:Passivated] l)))))

;; the actor's life moves only through step: waking on a message or a
;; signal, hibernating, and exiting
(calls act/ring! {:through [life/step]})
(calls act/run-fiber! {:through [life/step]})

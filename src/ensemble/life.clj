(ns ensemble.life
  "A process's life with hibernation, as erlang:hibernate/3 has it: a
  running process may hibernate, a hibernated one runs again when a
  message or an exit signal arrives, only a running process exits, and an
  exited one never runs again.  A hibernated process may be passivated --
  what it runs next kept on disk -- and wakes the same way.
  ensemble.actor moves every actor's life through step, and starts a
  fiber exactly when step wakes it.")

(defn step
  "The life after event, from life."
  [life event]
  (case (first life)
    :Running (case (first event)
               :Hibernate [:Hibernated]
               :Exit [:Dead]
               life)
    :Hibernated (case (first event)
                  (:Mail :Signal) [:Running]
                  :Passivate [:Passivated]
                  life)
    :Passivated (case (first event)
                  (:Mail :Signal) [:Running]
                  life)
    :Dead life))

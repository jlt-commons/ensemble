(ns ensemble.signal
  "The pure core of Erlang exit signals.  The actor runtime decides what to do
  with every exit signal here, so these rules are the ones that run.

  A signal has a kind: :link, sent when a linked process exits, or :exit, sent
  by (exit! actor reason).  A reason is any value; three are special:

      :normal  a clean exit.  A linked process that is not trapping exits
               ignores it.
      :kill    untrappable, but only as an :exit signal: the receiver dies
               with :killed whether it traps or not.  A process that dies
               with :kill as its own reason sends :kill to its links as an
               ordinary, trappable reason.
      :killed  what a killed process exits with.  To its links it is an
               ordinary abnormal reason, so a trapping link survives it.

  Any other reason is abnormal: a process that is not trapping exits dies
  with that same reason; one that traps receives [:EXIT from reason] as a
  message instead.")

(defn normal?
  "True for the one reason a non-trapping process ignores."
  [reason]
  (= :normal reason))

(defn on-signal
  "What a process does with an exit signal of kind carrying reason.
  trapping? is whether the receiver traps exits, and self? whether it sent the
  signal to itself.  Returns [:Die reason] (the process exits with reason),
  [:Deliver reason] (it receives [:EXIT from reason] as a message) or
  [:Ignore]."
  [trapping? kind reason self?]
  (cond
    (and (= :exit kind) (= :kill reason)) [:Die :killed]
    trapping?                            [:Deliver reason]
    (not (normal? reason))               [:Die reason]
    (and (= :exit kind) self?)           [:Die :normal]
    :else                                [:Ignore]))

(defn shutdown?
  "True for a reason OTP counts as an orderly stop: :normal, :shutdown, or
  [:shutdown term].  A transient child is not restarted after one of these."
  [reason]
  (or (normal? reason)
      (= :shutdown reason)
      (and (vector? reason) (= 2 (count reason)) (= :shutdown (first reason)))))

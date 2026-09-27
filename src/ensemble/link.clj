(ns ensemble.link
  "The pure core of Erlang links and exit signals.

  The effectful half -- the links table, link!/unlink!/exit! and the settle-time
  fan-out -- lives in ensemble.actor.  This namespace is pure, so the exit
  semantics can be stated as writ laws.

  A reason is a keyword.  :normal is benign: a linked process is unaffected by
  it.  :kill is untrappable.  Anything else is abnormal and kills a linked
  process that is not trapping exits.  links is an undirected edge list,
  [[a b] ...]."
  )

(defn abnormal?
  "True when reason kills a non-trapping linked process.  :normal does not."
  [reason]
  (not= :normal reason))

(defn exit-action
  "What a process does on receiving an exit signal with reason, given whether
  it traps exits.  [:Ignore], [:Deliver reason] (a trapping process receives it
  as a message and lives), or [:Die reason]."
  [trapping? reason]
  (cond
    (= :kill reason)   [:Die :killed]
    trapping?          [:Deliver reason]
    (abnormal? reason) [:Die reason]
    :else              [:Ignore]))

(defn- signal-to
  "The [dying informed] pair after the exit signal reaches `to` from a dying
  `from`, or unchanged when `to` is not newly exposed.  A neighbour that dies
  joins dying; one that traps is informed and stops the signal there."
  [traps reason dying informed from to]
  (if (and (contains? dying from)
           (not (contains? dying to))
           (not (contains? informed to)))
    (case (first (exit-action (contains? traps to) reason))
      :Die     [(conj dying to) informed]
      :Deliver [dying (conj informed to)]
      [dying informed])
    [dying informed]))

(defn death-closure
  "Origin just exited with reason.  Return [:Closure dead informed]: the pids
  that must die (each re-propagating the same reason) and those that only
  receive the exit signal.  A trapping pid is informed, never killed, except by
  :kill; a pid that dies keeps propagating, a pid that ignores does not.

  The signal spreads one pass over the edges at a time, so the number of passes
  is bounded by the edge count -- a longer chain than that cannot exist."
  [links traps origin reason]
  (loop [rounds   (+ 2 (* 2 (count links)))
         dying    #{origin}
         informed #{}]
    (if (pos? rounds)
      (let [[d i] (loop [ls links, d dying, i informed]
                    (if (seq ls)
                      (let [[a b] (first ls)
                            s1 (signal-to traps reason d i a b)
                            s2 (signal-to traps reason (nth s1 0) (nth s1 1) b a)]
                        (recur (rest ls) (nth s2 0) (nth s2 1)))
                      [d i]))]
        (recur (dec rounds) d i))
      [:Closure (disj dying origin) informed])))

(ns ensemble.process
  "What a process handle is: the operations one process makes on another,
  wherever the other runs.

  ensemble.actor implements Process for its local actors, and ensemble.node
  for a pid on another node, by sending each operation over the node's
  connection.  So !, link!, monitor!, exit! and the behaviours built on
  them work the same on a process in this VM, in another OS process or on
  another machine, as Erlang's do on any pid.

  The operations are the ones Erlang's distribution protocol carries: a
  message, an exit signal, a link and an unlink, a monitor and a
  demonitor.")

(defprotocol Process
  (-pid [p] "The process's id, unique on its node.")
  (-node [p] "The name of the node the process runs on.")
  (-alive? [p] "True while the process runs, as far as this node knows.")
  (-deliver [p msg] "Put msg in p's mailbox.  A message to a dead process is dropped.")
  (-signal [p from kind reason checked]
    "Send p an exit signal from process from: kind :exit (exit/2) or :link
    (a linked process exited).  checked says a link signal is dropped
    unless the link still stands when it arrives.")
  (-link [p from]
    "Record that p is linked to from.  A local p that is gone answers false
    at once; a remote one answers true and sends from the noproc signal
    when its node finds p gone, as Erlang does.")
  (-unlink [p from] "Remove p's link to from.")
  (-add-monitor [p ref notify]
    "Call (notify reason) once when p exits, under ref; at once, with
    :noproc, if p is gone.")
  (-drop-monitor [p ref] "Cancel the monitor ref of p."))

(defn process?
  "True when x is a process handle."
  [x]
  (satisfies? Process x))

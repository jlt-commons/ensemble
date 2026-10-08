(ns ensemble.registry
  "A process registry other than the node's own, for names written
  [:via registry name], as OTP's {via, Module, Name}.  Any value
  implementing Registry may be one: a global registry, a process group,
  a table of its own.

  ensemble registers a process's via name when it is spawned with
  {:name [:via r nm]}, resolves the name with whereis-name wherever a
  process is taken (!, gen-server's call!, link!, monitor!), and
  unregisters it when the process exits, so a registry need not watch its
  processes itself.")

(defprotocol Registry
  (register-name [r nm pid]
    "Register pid under nm.  True if it is now registered, false if nm is
    taken.")
  (unregister-name [r nm] "Release nm.")
  (whereis-name [r nm] "The pid registered under nm, or nil."))

(defn via?
  "Is x a name written [:via registry name]?"
  [x]
  (and (vector? x) (= 3 (count x)) (= :via (first x))))

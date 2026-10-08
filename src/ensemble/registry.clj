(ns ensemble.registry
  "A process registry other than the node's own, for names written
  [:via registry name], as OTP's {via, Module, Name}.  Any value
  implementing Registry may be one: a global registry, a process group,
  a table of its own.

  ensemble registers a process's via name when it is spawned with
  {:name [:via r nm]}, resolves the name with whereis-name wherever a
  process is taken (!, gen-server's call!, link!, monitor!), and
  unregisters it when the process exits, so a registry need not watch its
  processes itself -- unless it implements Watches, saying it does.")

(defprotocol Registry
  (register-name [r nm pid]
    "Register pid under nm.  True if it is now registered, false if nm is
    taken.")
  (unregister-name [r nm] "Release nm.")
  (whereis-name [r nm] "The pid registered under nm, or nil."))

(defprotocol Watches
  (watches-its-processes? [r]
    "True: r releases a process's names itself when the process exits, so
    ensemble need not."))

(defn via?
  "Is x a name written [:via registry name]?"
  [x]
  (and (vector? x) (= 3 (count x)) (= :via (first x))))

(defonce ^{:doc "The registry a name written [:global name] stands for:
  ensemble.global's, once it is loaded."}
  global
  (atom nil))

(defn global?
  "Is x a name written [:global name], OTP's {global, Name}?"
  [x]
  (and (vector? x) (= 2 (count x)) (= :global (first x))))

(defn named
  "x, with a name [:global nm] as the [:via registry nm] it stands for."
  [x]
  (if (global? x)
    (if-let [g @global]
      [:via g (second x)]
      (throw (ex-info "no global registry: require ensemble.global" {:reason :badarg :name x})))
    x))

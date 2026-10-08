(ns ensemble.global
  "A name registry over every connected node, as OTP's global.

  Each node runs a registrar, which holds a copy of the table of names.
  Registering a name locks it on every node -- in node order, and backing
  off when another registration holds a lock, so two never wait on each
  other -- checks that no node has it, then sets it on all.  A name is
  released everywhere when its process exits, and a node's names when the
  connection to it is lost.  When two nodes connect they send each other
  their tables; a name both hold for different processes keeps the one
  whose pid sorts first, on both nodes alike, and the other process is
  killed, as global's default resolve does.

  [:global nm] is a name wherever one is taken -- !, gen-server's start
  and call!, monitor! -- as OTP's {global, Name}."
  (:require [clojure.core.async :as a]
            [ensemble.actor :as act :refer [receive]]
            [ensemble.gen-server :as gs]
            [ensemble.node :as node]
            [ensemble.process :as proc]
            [ensemble.registry :as reg]))

(defonce ^:private tables
  ;; node -> {name pid}: each node's copy, written only by its registrar
  (atom {}))

(defn- table [] (get @tables (act/node) {}))

(defn- pid-order
  "Where a pid sorts, to settle a clash the same way on every node."
  [p]
  [(str (proc/-node p)) (proc/-pid p)])

(defn- bind
  "st with nm bound to pid on this node, monitoring pid."
  [st nm pid]
  (let [mref (act/monitor! pid)]
    (swap! tables assoc-in [(act/node) nm] pid)
    (assoc-in st [:mons mref] [nm pid])))

(defn- unbind
  "st with nm released on this node, if it is still bound to pid (any pid
  when pid is nil)."
  [st nm pid]
  (let [cur (get (table) nm)]
    (if (and cur (or (nil? pid) (= pid cur)))
      (let [mrefs (keep (fn [[r [n p]]] (when (and (= n nm) (= p cur)) r)) (:mons st))]
        (doseq [r mrefs] (act/demonitor! r {:flush true}))
        (swap! tables update (act/node) dissoc nm)
        (update st :mons #(apply dissoc % mrefs)))
      st)))

(defn- merge-entry
  "st once the binding nm -> pid, sent by a peer, is merged in."
  [st nm pid]
  (let [cur (get (table) nm)]
    (cond
      (nil? cur) (bind st nm pid)
      (= cur pid) st
      :else (let [[keep lose] (sort-by pid-order [cur pid])]
              (try (act/exit! lose :kill) (catch Throwable _ nil))
              (if (= keep cur) st (bind (unbind st nm cur) nm keep))))))

(defn- unlock [st nm owner]
  (if (= owner (first (get-in st [:locks nm])))
    (do (act/demonitor! (second (get-in st [:locks nm])) {:flush true})
        (update st :locks dissoc nm))
    st))

(defrecord Registrar []
  gs/Server
  (init [_]
    (node/monitor-nodes! true)
    [:ok {:locks {} :mons {}}])
  (handle-call [_ req _ st]
    (case (first req)
      :lock (let [[_ nm owner] req
                  holder (first (get-in st [:locks nm]))]
              (cond
                (= holder owner) [:reply [:ok (get (table) nm)] st]
                holder [:reply :busy st]
                :else [:reply [:ok (get (table) nm)]
                       (assoc-in st [:locks nm] [owner (act/monitor! owner)])]))
      :unlock (let [[_ nm owner] req] [:reply :ok (unlock st nm owner)])
      :register (let [[_ nm pid] req] [:reply :ok (bind (unbind st nm nil) nm pid)])
      :unregister (let [[_ nm] req] [:reply :ok (unbind st nm nil)])))
  (handle-cast [_ req st]
    (case (first req)
      :sync (let [[_ entries] req] [:noreply (reduce (fn [s [nm pid]] (merge-entry s nm pid)) st entries)])))
  (handle-info [_ msg st]
    (cond
      (and (vector? msg) (= :DOWN (first msg)))
      (let [[_ mref] msg]
        (if-let [[nm pid] (get-in st [:mons mref])]
          [:noreply (-> (unbind st nm pid) (update :mons dissoc mref))]
          ;; a registration that held a lock died
          (let [nm (some (fn [[n [_ r]]] (when (= r mref) n)) (:locks st))]
            [:noreply (if nm (update st :locks dissoc nm) st)])))
      (and (vector? msg) (= :nodeup (first msg)))
      (do (gs/cast! [:At ::registrar (second msg)] [:sync (vec (table))])
          [:noreply st])
      (and (vector? msg) (= :nodedown (first msg)))
      (let [peer (second msg)]
        [:noreply (reduce (fn [s [nm pid]] (if (= peer (proc/-node pid)) (unbind s nm pid) s))
                          st (table))])
      :else [:noreply st]))
  (handle-timeout [_ st] [:noreply st])
  (handle-continue [_ _ st] [:noreply st])
  (terminate [_ _ _] nil))

(defn- ensure-registrar!
  "This node's registrar, started if it is not running."
  []
  (or (act/whereis ::registrar)
      (try (gs/start (->Registrar) {:name ::registrar})
           (catch Throwable _ (act/whereis ::registrar)))))

(node/on-start! ::registrar (fn [_] (ensure-registrar!)))

(defn- all-nodes [] (sort-by str (distinct (cons (act/node) (act/nodes)))))

(defn- ask
  "Call the registrar of node n; nil when it cannot be reached."
  [n req]
  (try (gs/call! [:At ::registrar n] req 5000) (catch Throwable _ nil)))

(defn- in-actor
  "(f) in a fresh actor of this node, which is the owner of its locks."
  [f]
  (let [p (promise)]
    (act/spawn (fn [] (deliver p (try [:ok (f)] (catch Throwable e [:err e])))))
    (let [[k v] @p] (if (= :ok k) v (throw v)))))

(defn register-name
  "Register pid under nm on every connected node.  :yes, or :no when a
  live process anywhere has the name already."
  [nm pid]
  (ensure-registrar!)
  (in-actor
   (fn []
     (let [me (act/self)]
       (loop [attempt 0]
         (let [ns (all-nodes)
               got (reduce (fn [acc n]
                             (let [r (ask n [:lock nm me])]
                               (if (= :busy r) (reduced (conj acc [n :busy])) (conj acc [n r]))))
                           [] ns)
               release! #(doseq [[n r] got :when (vector? r)] (ask n [:unlock nm me]))]
           (if (some #(= :busy (second %)) got)
             (do (release!)
                 (when (> attempt 200) (throw (ex-info "global lock not taken" {:reason :timeout :name nm})))
                 (a/<!! (a/timeout (+ 1 (rand-int (min 50 (* 2 (inc attempt)))))))
                 (recur (inc attempt)))
             (let [taken (some (fn [[_ r]] (when (and (vector? r) (some? (second r)) (not= pid (second r))) r)) got)
                   result (if (or taken (and (act/actor? pid) (not (act/alive? pid))))
                            :no
                            (do (doseq [[n r] got :when (vector? r)] (ask n [:register nm pid])) :yes))]
               (release!)
               result))))))))

(defn unregister-name
  "Release nm on every connected node.  Returns :ok."
  [nm]
  (ensure-registrar!)
  (doseq [n (all-nodes)] (ask n [:unregister nm]))
  :ok)

(defn whereis-name
  "The process registered under nm, or nil."
  [nm]
  (ensure-registrar!)
  (get (table) nm))

(defn registered-names
  "The names registered, as this node knows them."
  []
  (ensure-registrar!)
  (vec (keys (table))))

(defn send
  "Send msg to the process registered under nm; throws {:reason :badarg}
  when there is none.  Returns msg."
  [nm msg]
  (if-let [p (whereis-name nm)]
    (act/! p msg)
    (throw (ex-info "no process registered globally under name" {:reason :badarg :name nm}))))

(defrecord GlobalRegistry []
  reg/Registry
  (register-name [_ nm pid] (= :yes (register-name nm pid)))
  (unregister-name [_ nm] (unregister-name nm))
  (whereis-name [_ nm] (whereis-name nm))
  ;; each registrar monitors the processes it holds names for
  reg/Watches
  (watches-its-processes? [_] true))

(reset! reg/global (->GlobalRegistry))

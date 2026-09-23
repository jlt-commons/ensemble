(ns ensemble.gen-event
  "The OTP gen_event behaviour: a manager process holds a set of event handlers
  and fans an event out to all of them.

  A handler is a record implementing Handler and runs inside the manager, so it
  must not block.  notify is asynchronous; sync-notify! waits for every handler
  to see the event before returning."
  (:require [ensemble.gen-server :as gs]))

(defprotocol Handler
  (h-init [this] "Return the handler's initial state.")
  (h-handle-event [this event hstate] "Handle an event; return new handler state.")
  (h-handle-call [this msg hstate] "Return [:reply r new-hstate].")
  (h-terminate [this reason hstate] "Run when the handler is removed or the manager stops."))

(defn- hfind [hs id]
  (some (fn [h] (when (= (:id h) id) h)) hs))

(defn- hput [hs id handler]
  (if (hfind hs id)
    (mapv (fn [h] (if (= (:id h) id) (assoc h :handler handler) h)) hs)
    (conj hs {:id id :handler handler :st (h-init handler)})))

(defn- hdel [hs id]
  (filterv (fn [h] (not= (:id h) id)) hs))

(defn- notify-all [hs ev]
  (mapv (fn [h] (assoc h :st (h-handle-event (:handler h) ev (:st h)))) hs))

(defn- terminate-all [hs reason]
  (doseq [h hs] (h-terminate (:handler h) reason (:st h)))
  hs)

(defrecord Manager [initial]
  gs/Server
  (init [_] {:handlers (mapv (fn [[id h]] {:id id :handler h :st (h-init h)}) initial)})
  (handle-call [_ _from msg st]
    (let [hs (:handlers st)]
      (case (first msg)
        :add (let [[_ id h] msg] [:reply :ok {:handlers (hput hs id h)}])
        :remove (let [[_ id] msg
                      h (hfind hs id)]
                  (if h (h-terminate (:handler h) :removed (:st h)) nil)
                  [:reply :ok {:handlers (hdel hs id)}])
        :sync-notify (let [[_ ev] msg] [:reply :ok {:handlers (notify-all hs ev)}])
        :call-handler
        (let [[_ id hmsg] msg
              h (hfind hs id)
              r (h-handle-call (:handler h) hmsg (:st h))]
          [:reply (nth r 1)
           {:handlers (mapv (fn [x]
                             (if (= (:id x) id) (assoc x :st (nth r 2)) x))
                           hs)}]))))
  (handle-cast [_ msg st]
    (case (first msg)
      :notify (let [[_ ev] msg] [:noreply {:handlers (notify-all (:handlers st) ev)}])))
  (handle-info [_ _msg st] [:noreply st])
  (handle-timeout [_ st] [:noreply st])
  (terminate [_ reason st] (terminate-all (:handlers st) reason)))

(defn start-manager
  "Create and start an event manager.  Options are gen-server options plus
  :handlers, a seq of [id handler] pairs registered before the manager starts
  handling events."
  ([] (start-manager {}))
  ([opts] (gs/gen-server (->Manager (:handlers opts)) (dissoc opts :handlers))))

(defn add-handler!
  "Add (or replace) the handler registered under id."
  [mgr id handler]
  (gs/call! mgr [:add id handler]))

(defn remove-handler! [mgr id] (gs/call! mgr [:remove id]))

(defn notify
  "Send event to every handler, asynchronously."
  [mgr event]
  (gs/cast! mgr [:notify event]))

(defn sync-notify!
  "Send event to every handler and wait for all of them."
  [mgr event]
  (gs/call! mgr [:sync-notify event]))

(defn call-handler!
  "Run handler id's handle-call with msg and return its reply."
  [mgr id msg]
  (gs/call! mgr [:call-handler id msg]))

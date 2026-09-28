(ns ensemble.gen-event
  "The OTP gen_event behaviour: a manager actor holds event handlers and hands
  each event to all of them, in the order they were added.

  A handler is a record implementing Handler.  It runs inside the manager, so
  it must not block.  Its callbacks return:

      h-init          the handler's initial state (throw to refuse)
      h-handle-event  [:ok state] or :remove-handler
      h-handle-call   [:ok reply state] or [:remove-handler reply]
      h-terminate     anything; add-handler!/delete-handler! return it

  As in OTP, a handler that throws or returns something else is removed, with
  h-terminate called on [:error reason]; the manager and the other handlers
  carry on.  A handler added with add-sup-handler! is tied to the actor that
  added it: if that actor exits, the handler is removed with [:stop reason];
  if the handler is removed for any reason but delete-handler!, that actor
  receives [:gen-event-EXIT id reason]."
  (:require [ensemble.actor :as act]
            [ensemble.gen-server :as gs]))

(defprotocol Handler
  (h-init [this] "Return the handler's initial state.")
  (h-handle-event [this event state] "Return [:ok state] or :remove-handler.")
  (h-handle-call [this request state] "Return [:ok reply state] or [:remove-handler reply].")
  (h-terminate [this reason state] "Run once as the handler is removed."))

(defn- hfind [hs id] (first (filter #(= id (:id %)) hs)))

(defn- hdel [hs id] (filterv #(not= id (:id %)) hs))

(defn- remove-handler
  "Remove h for reason: run its terminate (a throw there is ignored, as in
  OTP) and tell a supervising actor, unless it asked for the removal itself.
  Returns [handlers terminate-result]."
  [hs h reason tell?]
  (let [r (try (h-terminate (:handler h) reason (:st h)) (catch Throwable e [:error e]))]
    (when (and tell? (:owner h))
      (act/! (:owner h) [:gen-event-EXIT (:id h) reason]))
    [(hdel hs (:id h)) r]))

(defn- event-to
  "Hand ev to one handler; return the handlers after it has run."
  [hs h ev]
  (let [r (try (h-handle-event (:handler h) ev (:st h)) (catch Throwable e [::crash e]))]
    (cond
      (= :remove-handler r) (first (remove-handler hs h :remove-handler true))
      (and (vector? r) (= :ok (first r)) (= 2 (count r)))
      (mapv #(if (= (:id h) (:id %)) (assoc % :st (second r)) %) hs)
      (and (vector? r) (= ::crash (first r)))
      (first (remove-handler hs h [:error (act/reason-of (second r))] true))
      :else (first (remove-handler hs h [:error [:bad-return-value r]] true)))))

(defn- notify-all [hs ev]
  (reduce (fn [acc h] (if (hfind acc (:id h)) (event-to acc h ev) acc)) hs hs))

(defn- add [hs id handler owner]
  (if (hfind hs id)
    [[:error :already-added] hs]
    (let [r (try [:ok (h-init handler)] (catch Throwable e [:error (act/reason-of e)]))]
      (if (= :ok (first r))
        (do (when owner (act/link! owner))
            [[:ok nil] (conj hs {:id id :handler handler :st (second r) :owner owner})])
        [r hs]))))

(defrecord Manager [initial]
  gs/Server
  (init [_]
    (reduce (fn [hs [id h]] (second (add hs id h nil))) [] initial))
  (handle-call [_ req _ hs]
    (case (first req)
      :add (let [[_ id h owner] req
                 [r hs*] (add hs id h owner)]
             [:reply r hs*])
      :delete (let [[_ id reason] req]
                (if-let [h (hfind hs id)]
                  (let [[hs* r] (remove-handler hs h reason false)] [:reply [:ok r] hs*])
                  [:reply [:error :not-found] hs]))
      :sync-notify [:reply [:ok nil] (notify-all hs (second req))]
      :call (let [[_ id hreq] req
                  h (hfind hs id)]
              (if (nil? h)
                [:reply [:error :bad-module] hs]
                (let [r (try (h-handle-call (:handler h) hreq (:st h)) (catch Throwable e [::crash e]))]
                  (cond
                    (and (vector? r) (= :ok (first r)) (= 3 (count r)))
                    [:reply [:ok (nth r 1)] (mapv #(if (= id (:id %)) (assoc % :st (nth r 2)) %) hs)]
                    (and (vector? r) (= :remove-handler (first r)))
                    [:reply [:ok (second r)] (first (remove-handler hs h :remove-handler true))]
                    :else
                    (let [reason (if (= ::crash (first r))
                                   (act/reason-of (second r))
                                   [:bad-return-value r])]
                      [:reply [:error reason] (first (remove-handler hs h [:error reason] true))])))))
      :which [:reply [:ok (mapv :id hs)] hs]))
  (handle-cast [_ req hs]
    [:noreply (notify-all hs (second req))])
  (handle-info [_ msg hs]
    (if (and (vector? msg) (= :EXIT (first msg)))
      (let [[_ from reason] msg]
        [:noreply (reduce (fn [acc h]
                            (if (= from (:owner h))
                              (first (remove-handler acc h [:stop reason] false))
                              acc))
                          hs hs)])
      [:noreply hs]))
  (handle-timeout [_ hs] [:noreply hs])
  (terminate [_ reason hs]
    (doseq [h hs] (remove-handler hs h (if (= :normal reason) :stop [:stop reason]) true))))

(defn start
  "Start an event manager.  Options are gen-server start's, plus :handlers, a
  seq of [id handler] added before the manager runs."
  ([] (start {}))
  ([opts] (gs/start (->Manager (:handlers opts)) (assoc (dissoc opts :handlers) :trap true))))

(defn start-link
  "start, linked to the calling actor."
  ([] (start-link {}))
  ([opts] (gs/start-link (->Manager (:handlers opts)) (assoc (dissoc opts :handlers) :trap true))))

(defn- result [r]
  (if (= :ok (first r))
    (second r)
    (throw (ex-info (str "gen-event: " (pr-str (second r))) {:reason (second r)}))))

(defn add-handler!
  "Add handler under id.  Throws if id is taken or h-init throws."
  [mgr id handler]
  (result (gs/call! mgr [:add id handler nil]))
  :ok)

(defn add-sup-handler!
  "add-handler!, tied to the calling actor (see the namespace doc)."
  [mgr id handler]
  (result (gs/call! mgr [:add id handler (act/self)]))
  :ok)

(defn delete-handler!
  "Remove handler id, calling its terminate with reason (default :delete).
  Returns what terminate returned; throws if there is no such handler."
  ([mgr id] (delete-handler! mgr id :delete))
  ([mgr id reason] (result (gs/call! mgr [:delete id reason]))))

(defn notify
  "Hand event to every handler, without waiting.  Returns :ok."
  [mgr event]
  (gs/cast! mgr [:notify event]))

(defn sync-notify!
  "Hand event to every handler and wait until all have run.  Returns :ok."
  [mgr event]
  (result (gs/call! mgr [:sync-notify event]))
  :ok)

(defn call!
  "Run handler id's h-handle-call with request and return its reply.  Throws
  with :bad-module when there is no such handler."
  ([mgr id request] (result (gs/call! mgr [:call id request])))
  ([mgr id request timeout-ms] (result (gs/call! mgr [:call id request] timeout-ms))))

(defn which-handlers
  "The handler ids, in the order they were added."
  [mgr]
  (result (gs/call! mgr [:which])))

(defn stop!
  "Stop the manager, terminating every handler.  Waits until it has."
  [mgr]
  (gs/stop! mgr))

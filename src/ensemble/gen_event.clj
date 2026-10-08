(ns ensemble.gen-event
  "The OTP gen_event behaviour: a manager actor holds event handlers and hands
  each event to all of them, in the order they were added.

  A handler is a record implementing Handler.  It runs inside the manager, so
  it must not block.  Its callbacks return:

      h-init          the handler's initial state (throw to refuse)
      h-handle-event  [:ok state] or :remove-handler
      h-handle-call   [:ok reply state] or [:remove-handler reply]
      h-terminate     anything; add-handler!/delete-handler! return it

  h-handle-event may also return [:ok state :hibernate], which hibernates the
  manager, or [:swap-handler reason state id f], which swaps the handler as
  swap-handler! does.  A handler that also implements InfoHandler receives
  the manager's other messages through h-handle-info.  As in OTP, a handler
  that throws or returns something else is removed, with h-terminate called
  on [:error reason]; the manager and the other handlers carry on.  A handler added with add-sup-handler! is tied to the actor that
  added it: if that actor exits, the handler is removed with [:stop reason];
  if the handler is removed for any reason but delete-handler!, that actor
  receives [:gen-event-EXIT id reason]."
  (:require [ensemble.actor :as act]
            [ensemble.gen-server :as gs]
            [ensemble.logger :as logger]))

(defprotocol Handler
  (h-init [this] "Return the handler's initial state.")
  (h-handle-event [this event state] "Return [:ok state], [:ok state :hibernate], :remove-handler or [:swap-handler reason state id f].")
  (h-handle-call [this request state] "Return [:ok reply state] or [:remove-handler reply].")
  (h-terminate [this reason state] "Run once as the handler is removed."))

(defprotocol InfoHandler
  (h-handle-info [this msg state]
    "Optional, as in OTP: take a message the manager received that is not an
    event, a call or a supervising process's exit.  Returns as
    h-handle-event."))

(defn- hfind [hs id] (first (filter #(= id (:id %)) hs)))

(defn- hdel [hs id] (filterv #(not= id (:id %)) hs))

(defn- release-owner
  "Unlink owner once hs holds no handler of its, unless the two were linked
  before its first handler came (a start-link parent, say)."
  [hs owner linked-before?]
  (when (and owner (not linked-before?) (not (some #(= owner (:owner %)) hs)))
    (act/unlink! owner)))

(defn- remove-handler
  "Remove h for reason: run its terminate (a throw there is ignored, as in
  OTP) and tell a supervising actor, unless it asked for the removal itself
  -- told, when given, is what it hears instead of reason.  An owner left
  with no handler of its own is unlinked, unless release? is false.
  Returns [handlers terminate-result]."
  ([hs h reason tell?] (remove-handler hs h reason tell? reason true))
  ([hs h reason tell? told release?]
   (let [r (try (h-terminate (:handler h) reason (:st h)) (catch Throwable e [:error e]))
         hs* (hdel hs (:id h))]
     (when-let [owner (:owner h)]
       (when tell? (act/! owner [:gen-event-EXIT (:id h) told]))
       (when release? (release-owner hs* owner (:linked-before h))))
     [hs* r])))

(defn- add
  "Add handler under id, at position at (the end when nil), supervised by
  owner when there is one: [[:ok nil] hs] or [[:error reason] hs]."
  [hs id handler owner at]
  (if (hfind hs id)
    [[:error :already-added] hs]
    (let [r (try [:ok (h-init handler)] (catch Throwable e [:error (act/reason-of e)]))]
      (if (= :ok (first r))
        (let [linked-before (when owner
                              (if-let [same (first (filter #(= owner (:owner %)) hs))]
                                (:linked-before same)
                                (contains? (act/process-info (act/self) :links) owner)))
              entry {:id id :handler handler :st (second r) :owner owner :linked-before linked-before}
              at (or at (count hs))]
          (when owner (act/link! owner))
          [[:ok nil] (vec (concat (take at hs) [entry] (drop at hs)))])
        [r hs]))))

(defn- swap
  "Replace handler h with one made by (f terminate-result) under new-id, as
  OTP's swap_handler: h's terminate runs with reason, and what it returns
  is the new handler's start.  The new one is supervised by owner.  A
  supervising actor of h's hears [:gen-event-EXIT id [:swapped new-id
  owner]], as OTP's does."
  [hs h reason new-id f owner]
  (let [at (count (take-while #(not= (:id h) (:id %)) hs))
        old-owner (:owner h)
        [hs* res] (remove-handler hs h reason (some? old-owner) [:swapped new-id owner] false)
        handler (try (f res) (catch Throwable e e))
        [r hs**] (if (instance? Throwable handler)
                   [[:error (act/reason-of handler)] hs*]
                   (add hs* new-id handler owner at))
        ;; the same owner keeps what it had: the link is h's, not from before
        hs** (if (and owner (= owner old-owner))
               (mapv #(if (= owner (:owner %)) (assoc % :linked-before (:linked-before h)) %) hs**)
               hs**)]
    ;; unlinked only now, so swapping to the same owner keeps the link
    (release-owner hs** old-owner (:linked-before h))
    [r hs**]))

(defn- run-handler
  "Run (f) for handler h, a callback that answers as h-handle-event does,
  and return [handlers hibernate?] after it.  A handler that throws or
  answers anything else is removed with [:error reason]."
  [hs h f]
  (let [r (try (f) (catch Throwable e [::crash e]))]
    (cond
      (= :remove-handler r) [(first (remove-handler hs h :remove-handler true)) false]
      (and (vector? r) (= :ok (first r)) (<= 2 (count r) 3)
           (or (= 2 (count r)) (= :hibernate (nth r 2))))
      [(mapv #(if (= (:id h) (:id %)) (assoc % :st (second r)) %) hs) (= 3 (count r))]
      (and (vector? r) (= :swap-handler (first r)) (= 5 (count r)))
      (let [[_ reason st new-id f] r
            [res hs*] (swap hs (assoc h :st st) reason new-id f (:owner h))]
        (when-not (= :ok (first res))
          (logger/report! {:level :error :kind :gen-event-swap-failed :manager (act/self)
                           :handler (:id h) :new-handler new-id :reason (second res)}))
        [hs* false])
      (and (vector? r) (= ::crash (first r)))
      [(first (remove-handler hs h [:error (act/reason-of (second r))] true)) false]
      :else [(first (remove-handler hs h [:error [:bad-return-value r]] true)) false])))

(defn- each-handler
  "Run (f h) for each handler still present that takes? accepts, in order:
  [handlers hibernate?], hibernating when any asked to."
  [hs takes? f]
  (reduce (fn [[acc hib] h]
            (if (and (hfind acc (:id h)) (takes? h))
              (let [[acc h?] (run-handler acc h #(f h))] [acc (or hib h?)])
              [acc hib]))
          [hs false] hs))

(defn- notify-all [hs ev]
  (each-handler hs any? (fn [h] (h-handle-event (:handler h) ev (:st h)))))

(defn- info-all [hs msg]
  (each-handler hs #(satisfies? InfoHandler (:handler %))
                (fn [h] (h-handle-info (:handler h) msg (:st h)))))

(defn- noreply [[hs hibernate?]]
  (if hibernate? [:noreply hs :hibernate] [:noreply hs]))

(defrecord Manager [initial]
  gs/Server
  (init [_]
    [:ok (reduce (fn [hs [id h]] (second (add hs id h nil nil))) [] initial)])
  (handle-call [_ req _ hs]
    (case (first req)
      :add (let [[_ id h owner] req
                 [r hs*] (add hs id h owner nil)]
             [:reply r hs*])
      :delete (let [[_ id reason] req]
                (if-let [h (hfind hs id)]
                  (let [[hs* r] (remove-handler hs h reason false)] [:reply [:ok r] hs*])
                  [:reply [:error :not-found] hs]))
      :swap (let [[_ old-id reason new-id f owner] req]
              (if-let [h (hfind hs old-id)]
                (let [[r hs*] (swap hs h reason new-id f owner)] [:reply r hs*])
                [:reply [:error :not-found] hs]))
      :sync-notify (let [[hs* hib] (notify-all hs (second req))]
                     (if hib [:reply [:ok nil] hs* :hibernate] [:reply [:ok nil] hs*]))
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
    (noreply (notify-all hs (second req))))
  (handle-info [_ msg hs]
    (let [owner-exit (when (and (vector? msg) (= :EXIT (first msg)))
                       (let [[_ from] msg] (when (some #(= from (:owner %)) hs) from)))]
      (if owner-exit
        [:noreply (reduce (fn [acc h]
                            (if (= owner-exit (:owner h))
                              (first (remove-handler acc h [:stop (nth msg 2)] false))
                              acc))
                          hs hs)]
        (noreply (info-all hs msg)))))
  (handle-timeout [_ hs] [:noreply hs])
  (handle-continue [_ _ hs] [:noreply hs])
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

(defn swap-handler!
  "Replace handler old-id, as OTP's swap_handler: its terminate runs with
  reason, and (f what-terminate-returned) makes the handler added under
  new-id in its place.  Returns :ok; throws if old-id is not a handler or
  the new one's h-init throws."
  [mgr old-id reason new-id f]
  (result (gs/call! mgr [:swap old-id reason new-id f nil]))
  :ok)

(defn swap-sup-handler!
  "swap-handler!, the new handler tied to the calling actor as
  add-sup-handler! ties one."
  [mgr old-id reason new-id f]
  (result (gs/call! mgr [:swap old-id reason new-id f (act/self)]))
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

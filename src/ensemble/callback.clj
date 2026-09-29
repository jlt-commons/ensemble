(ns ensemble.callback
  "What a gen-server callback's return means.  The server loop reads every
  handler result through interpret, so these are the rules that run.

  handle-call may return

      [:reply reply state]          [:reply reply state timeout]
      [:noreply state]              [:noreply state timeout]
      [:stop reason reply state]    [:stop reason state]

  and handle-cast, handle-info and handle-timeout the same without the reply
  forms.  A timeout is milliseconds, nil or :infinity (no timeout).  Anything
  else is a bad return value, and the server stops with
  [:bad-return-value ret], as OTP's does.")

(defn timeout?
  "True for a valid timeout: nil, :infinity or a non-negative integer."
  [t]
  (or (nil? t) (= :infinity t) (and (integer? t) (not (neg? t)))))

(defn- action-of
  "The Next an Action asks for: [:Wait ms-or-nil], [:Hibernate] or
  [:Cont c]; nil when a is not an Action."
  [a]
  (cond
    (= :infinity a) [:Wait nil]
    (timeout? a) [:Wait a]
    (= :hibernate a) [:Hibernate]
    (and (vector? a) (= 2 (count a)) (= :continue (first a))) [:Cont (second a)]
    :else nil))

(defn interpret-init
  "What init's ret means:
      [:Start state next]      run, then do next (see interpret)
      [:Ignore]                start answers :ignore; the process exits :normal
      [:Fail reason exit]      start fails with reason; the process exits with exit
  init returns [:ok state], [:ok state action], [:stop reason], [:error
  reason] or :ignore; anything else fails with [:bad-return-value ret]."
  [ret]
  (let [n (if (vector? ret) (count ret) 0)
        tag (when (pos? n) (first ret))
        bad [:bad-return-value ret]]
    (cond
      (= :ignore ret) [:Ignore]
      (and (= :ok tag) (= 2 n)) [:Start (get ret 1) [:Wait nil]]
      (and (= :ok tag) (= 3 n) (some? (action-of (get ret 2)))) [:Start (get ret 1) (action-of (get ret 2))]
      (and (= :stop tag) (= 2 n)) [:Fail (get ret 1) (get ret 1)]
      (and (= :error tag) (= 2 n)) [:Fail (get ret 1) :normal]
      :else [:Fail bad bad])))

(defn interpret
  "The step a callback's ret asks for.  kind is :call for handle-call and
  anything else for the other callbacks.
      [:Reply reply state next]      answer the caller, then do next
      [:NoReply state next]          do next (a call answered later by reply!)
      [:Stop reason reply? reply state]  terminate; answer first when reply?
      [:Bad ret]                     not a valid return
  next is [:Wait timeout-ms] (nil: none), [:Hibernate], or [:Cont c]: run
  handle-continue with c before taking another message."
  [kind ret]
  (let [n (if (vector? ret) (count ret) 0)
        tag (when (pos? n) (first ret))
        call? (= :call kind)]
    (cond
      (and call? (= :reply tag) (= 3 n)) [:Reply (get ret 1) (get ret 2) [:Wait nil]]
      (and call? (= :reply tag) (= 4 n) (some? (action-of (get ret 3))))
      [:Reply (get ret 1) (get ret 2) (action-of (get ret 3))]
      (and (= :noreply tag) (= 2 n)) [:NoReply (get ret 1) [:Wait nil]]
      (and (= :noreply tag) (= 3 n) (some? (action-of (get ret 2))))
      [:NoReply (get ret 1) (action-of (get ret 2))]
      (and call? (= :stop tag) (= 4 n))
      [:Stop (get ret 1) true (get ret 2) (get ret 3)]
      (and (= :stop tag) (= 3 n))
      [:Stop (get ret 1) false nil (get ret 2)]
      :else [:Bad ret])))

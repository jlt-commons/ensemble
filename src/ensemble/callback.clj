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

(defn- timeout-of [t] (if (= :infinity t) nil t))

(defn interpret
  "The step a callback's ret asks for.  kind is :call for handle-call and
  anything else for the other callbacks.

      [:Reply reply state timeout]   answer the caller, carry on
      [:Continue state timeout]      carry on (a call answered later by reply!)
      [:Stop reason reply? reply state]  terminate; answer first when reply?
      [:Bad ret]                     not a valid return"
  [kind ret]
  (let [n (if (vector? ret) (count ret) 0)
        tag (when (pos? n) (first ret))
        call? (= :call kind)]
    (cond
      (and call? (= :reply tag) (<= 3 n 4) (timeout? (get ret 3)))
      [:Reply (get ret 1) (get ret 2) (timeout-of (get ret 3))]

      (and (= :noreply tag) (<= 2 n 3) (timeout? (get ret 2)))
      [:Continue (get ret 1) (timeout-of (get ret 2))]

      (and call? (= :stop tag) (= 4 n))
      [:Stop (get ret 1) true (get ret 2) (get ret 3)]

      (and (= :stop tag) (= 3 n))
      [:Stop (get ret 1) false nil (get ret 2)]

      :else [:Bad ret])))

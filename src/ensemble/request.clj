(ns ensemble.request
  "Which message answers an asynchronous gen-server request, as OTP's
  check_response and the request id collections decide it.  Pure;
  ensemble.gen-server reads every message it waits for through these.

  A request id is [:Req alias mref server]: the alias the reply comes to,
  the ref of the monitor the request set on the server, and the server as
  it was named.")

(defn answer
  "What msg is to request req: [:Reply reply] when it is the reply to it,
  [:Failed reason server] when it is the DOWN of its monitor, [:NoReply]
  otherwise."
  [msg req]
  (case (first req)
    :Req (let [[_ al mr srv] req]
           (cond
             (and (vector? msg) (= 2 (count msg)) (= al (first msg))) [:Reply (second msg)]
             (and (vector? msg) (= 5 (count msg)) (= :DOWN (first msg)) (= mr (second msg))
                  (= :process (nth msg 2)))
             [:Failed (nth msg 4) srv]
             :else [:NoReply]))))

(defn check
  "What msg is to the collection coll, [[req label] ...]: [:Response answer
  label req coll'] when it answers request req, coll' without req when
  delete?; [:NotOurs] when it answers none; [:NoRequest] when coll is
  empty."
  [msg coll delete?]
  (if (empty? coll)
    [:NoRequest]
    (let [hit (some (fn [[req lbl]]
                      (let [a (answer msg req)]
                        (case (first a) :NoReply nil [req lbl a])))
                    coll)]
      (if hit
        (let [[req lbl a] hit]
          [:Response a lbl req (if delete? (filterv (fn [[r _]] (not= r req)) coll) coll)])
        [:NotOurs]))))

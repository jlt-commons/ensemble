(ns ensemble.request-spec
  "Contract for ensemble.request: which message answers an asynchronous
  gen_server request, from the gen_server docs (send_request,
  check_response, receive_response, wait_response and the request id
  collections of OTP 25).

  - A request id stands for one request.  The reply to it, or the DOWN of
    the monitor the request set on the server, answers it: {reply, Reply}
    or {error, {Reason, ServerRef}}.  Any other message is no_reply to it.
  - A collection holds request ids with their labels, as [[req label] ...].  A message answering one of
    them is that request's response with its label; the request leaves the
    collection when Delete is true, and stays when it is false.  An empty
    collection answers no_request, whatever the message.
  - A message answers at most one request of a collection, and answering
    it changes nothing else in the collection.

  A request id is [:Req alias mref server]: the alias the reply comes to,
  the ref of the monitor on the server, and the server as it was named.  A
  reply is [alias reply]; a DOWN is [:DOWN mref :process pid reason]."
  (:require [writ.spec :refer [spec data ann refine graph law calls]]
            [ensemble.request :as rq]
            [ensemble.gen-server :as gs]))

(spec ensemble.request {:require :proved})

(data ReqId (Req Any Any Any))
(data Answer (Reply Any) (Failed Any Any) (NoReply))
(data Checked (Response Answer Any ReqId (Vec Any)) (NoRequest) (NotOurs))

(ann answer [Any ReqId -> Answer])
(ann check [Any (Vec Any) Bool -> Checked])

(refine Answered [a Answer] (contains? #{:Reply :Failed} (first a)))
(refine Unanswered [a Answer] (= :NoReply (first a)))

;; a reply answers only with the alias its request made, which no two
;; values generated apart share: the step is shown by one
(graph response
  {:states {:msg Any, :answered Answered, :unanswered Unanswered}
   :edges  {:msg {[answer _ ReqId] #{:answered :unanswered}}}
   :witnesses {[:msg :answered] [[:a 1] [:Req :a :m :srv]]}})

;; --- one request --------------------------------------------------------

(law the-reply-answers-it
  (forall [al Any, mr Any, srv Any, r Any]
    (= (answer [al r] [:Req al mr srv]) [:Reply r])))

(law the-down-of-its-monitor-is-an-error
  (forall [al Any, mr Any, srv Any, p Any, why Any]
    (= (answer [:DOWN mr :process p why] [:Req al mr srv]) [:Failed why srv])))

(law a-reply-to-another-alias-is-no-reply
  (forall [al Any, other Any, mr Any, srv Any, r Any]
    (=> (not= al other) (= (answer [other r] [:Req al mr srv]) [:NoReply]))))

(law a-down-of-another-monitor-is-no-reply
  (forall [al Any, mr Any, other Any, srv Any, p Any, why Any]
    (=> (not= mr other)
        (= (answer [:DOWN other :process p why] [:Req al mr srv]) [:NoReply]))))

(law any-other-message-is-no-reply
  (forall [n Int, mr Any, srv Any, al Keyword]
    (= (answer n [:Req al mr srv]) [:NoReply])))

;; --- collections --------------------------------------------------------

(law an-empty-collection-has-no-request
  (forall [m Any, d Bool] (= (check m [] d) [:NoRequest])))

(law a-response-comes-with-its-label-and-is-deleted
  (forall [al Keyword, mr Keyword, srv Any, r Any, lbl Any]
    (= (check [al r] [[[:Req al mr srv] lbl]] true)
       [:Response [:Reply r] lbl [:Req al mr srv] []])))

(law a-response-kept-when-delete-is-false
  (forall [al Keyword, mr Keyword, srv Any, r Any, lbl Any]
    (= (check [al r] [[[:Req al mr srv] lbl]] false)
       [:Response [:Reply r] lbl [:Req al mr srv] [[[:Req al mr srv] lbl]]])))

(law answering-one-request-leaves-the-others
  (forall [a Keyword, b Keyword, ma Keyword, mb Keyword, srv Any, r Any, la Any, lb Any]
    (=> (and (not= a b) (not= ma mb) (not= a mb) (not= b ma))
        (and (= (check [a r] [[[:Req a ma srv] la] [[:Req b mb srv] lb]] true)
                [:Response [:Reply r] la [:Req a ma srv] [[[:Req b mb srv] lb]]])
             (= (check [a r] [[[:Req b mb srv] lb] [[:Req a ma srv] la]] true)
                [:Response [:Reply r] la [:Req a ma srv] [[[:Req b mb srv] lb]]])))))

(law a-message-for-none-of-them-is-not-ours
  (forall [a Keyword, ma Keyword, srv Any, lbl Any, n Int, d Bool]
    (= (check n [[[:Req a ma srv] lbl]] d) [:NotOurs])))

;; --- the client reads every message through these -----------------------

(calls gs/check-response {:through [rq/answer]})
(calls gs/receive-response {:through [rq/check]})

(ns ensemble.gen-fsm-test
  (:require [clojure.test :refer [deftest is]]
            [ensemble.actor :as act]
            [ensemble.gen-fsm :as fsm]))

(defrecord Turnstile []
  fsm/FSM
  (fsm-init [_] [:locked :closed])
  (fsm-handle-event [_ name event data]
    (case event
      :coin (case name
              :locked [:next :unlocked data]
              :unlocked [:next :unlocked data])
      :push (case name
              :unlocked [:next :locked :open]
              :locked [:next :locked :closed])
      :query [:reply name name data]))
  (fsm-handle-timeout [_ name data] [:next name data])
  (fsm-terminate [_ _reason _name _data] nil))

(deftest transitions
  (let [f (fsm/start-fsm (->Turnstile))]
    (is (= :unlocked (fsm/sync-send-event! f :coin)))
    (is (= :locked (fsm/sync-send-event! f :push)))
    (is (= :locked (fsm/sync-send-event! f :query)))))

(defrecord Door []
  fsm/FSM
  (fsm-init [_] [:closed :idle])
  (fsm-handle-event [_ name event data]
    (case event
      :open [:next :open data]
      :close [:next :closed data]
      :quit [:stop :requested name data]))
  (fsm-handle-timeout [_ name data] (if (= name :open) [:next :closed data] [:next name data]))
  (fsm-terminate [_ _reason _name _data] nil))

(deftest stop-settles-with-reason
  (let [f (fsm/start-fsm (->Door))]
    (fsm/send-event! f :open)
    (is (= :open (fsm/sync-send-event! f :open)))
    (fsm/send-event! f :quit)
    (is (= :requested (act/join f)))))

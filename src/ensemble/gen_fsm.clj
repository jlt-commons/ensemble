(ns ensemble.gen-fsm
  "An OTP gen_fsm-style state machine.

  An fsm is a record implementing FSM.  fsm-init returns [state-name data] and
  fsm-handle-event receives the current state name and data and returns a tagged
  vector:

      [:next name data]            [:next name data timeout-ms]
      [:reply reply name data]     [:reply reply name data timeout-ms]
      [:stop reason name data]

  send-event! is asynchronous; sync-send-event! replies with the new state name.
  A timeout-ms arms fsm-handle-timeout, run when no event arrives in time."
  (:require [ensemble.gen-server :as gs]))

(defprotocol FSM
  (fsm-init [this] "Return [state-name data].")
  (fsm-handle-event [this state-name event data] "Return a tagged vector.")
  (fsm-handle-timeout [this state-name data] "Return a tagged vector or same [name data].")
  (fsm-terminate [this reason state-name data] "Run once on :stop."))

(defrecord FsmServer [fsm]
  gs/Server
  (init [_]
    (let [[nm d] (fsm-init fsm)]
      {:name nm :data d}))
  (handle-call [_ _from msg st]
    (let [r (fsm-handle-event fsm (:name st) (nth msg 1) (:data st))]
      (case (first r)
        :next [:reply (nth r 1) {:name (nth r 1) :data (nth r 2)} (nth r 3 nil)]
        :reply [:reply (nth r 1) {:name (nth r 2) :data (nth r 3)} (nth r 4 nil)]
        :stop [:stop (nth r 1) {:name (nth r 2) :data (nth r 3)}])))
  (handle-cast [_ msg st]
    (let [r (fsm-handle-event fsm (:name st) (nth msg 1) (:data st))]
      (case (first r)
        :next [:noreply {:name (nth r 1) :data (nth r 2)} (nth r 3 nil)]
        :reply [:noreply {:name (nth r 2) :data (nth r 3)} (nth r 4 nil)]
        :stop [:stop (nth r 1) {:name (nth r 2) :data (nth r 3)}])))
  (handle-info [_ _msg st] [:noreply st])
  (handle-timeout [_ st]
    (let [r (fsm-handle-timeout fsm (:name st) (:data st))]
      (case (first r)
        :next [:noreply {:name (nth r 1) :data (nth r 2)} (nth r 3 nil)]
        :reply [:noreply {:name (nth r 2) :data (nth r 3)} (nth r 4 nil)]
        :stop [:stop (nth r 1) {:name (nth r 2) :data (nth r 3)}])))
  (terminate [_ reason st] (fsm-terminate fsm reason (:name st) (:data st))))

(defn start-fsm
  ([] (start-fsm nil {}))
  ([fsm] (start-fsm fsm {}))
  ([fsm opts] (gs/gen-server (->FsmServer fsm) opts)))

(defn send-event! [fsm event] (gs/cast! fsm [:event event]))

(defn sync-send-event!
  "Send event and return the resulting state name."
  [fsm event]
  (gs/call! fsm [:sync-event event]))

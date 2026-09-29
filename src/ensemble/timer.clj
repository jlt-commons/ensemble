(ns ensemble.timer
  "Timers, as erlang's send_after, start_timer, cancel_timer and
  read_timer, and the timer module's apply_after, send_interval,
  apply_interval, exit_after and cancel.

  Each node has one timer server, started on first use, that keeps the
  table of ensemble.timers and decides everything through it: when a timer
  fires, what a cancel answers, and which timers an exit ends.

  - A timer to a pid is cancelled when that process exits.  A timer to a
    name (or [:At name node]) looks the name up when it fires, and its
    message is dropped if no process has it.
  - start-timer's message is [:timeout ref msg]; send-after's is msg.
  - cancel-timer answers the ms left, or false for a timer that has fired
    or was cancelled.  With {:async true} it returns :ok at once, and with
    {:info true} as well, the answer comes as [:cancel-timer ref answer].
  - An interval (send-interval, apply-interval) fires every ms at fixed
    deadlines until cancelled, and ends when the process that started it
    exits."
  (:require [ensemble.actor :as act :refer [receive]]
            [ensemble.timers :as tm]))

(defn monotonic-time
  "Milliseconds of a monotonic clock, as erlang:monotonic_time(millisecond)."
  []
  (quot (System/nanoTime) 1000000))

(defn- fire!
  "Deliver what a timer that fired carries."
  [[dest msg]]
  (try
    (case (and (vector? dest) (first dest))
      ::apply (let [[f args] msg] (act/spawn (fn [] (apply f args))))
      ::exit (act/exit! (second dest) msg)
      (act/! dest msg))
    ;; a name nobody holds when the timer fires: the message is dropped
    (catch Throwable _ nil)))

(defn- answer!
  "Answer a cancel or read: a promise, or [pid tag ref] sent [tag ref v]."
  [to v]
  (if (vector? to)
    (let [[pid tag ref] to] (act/! pid [tag ref v]))
    (deliver to v)))

(defn serve
  "The timer server's loop: the table, and the processes it watches so an
  exit can end their timers."
  []
  (loop [t [] watched #{}]
    (let [now (monotonic-time)
          next (tm/next-deadline t)]
      (if (and (some? next) (<= next now))
        (let [[_ fired t] (tm/due t now)]
          (run! fire! fired)
          (recur t watched))
        (let [m (receive
                 [[::arm ref ms dest msg every owner] [:arm ref ms dest msg every owner]]
                 [[::cancel ref to] [:cancel ref to]]
                 [[::read ref to] [:read ref to]]
                 [[:DOWN _ :process p _] [:down p]]
                 [:after (when next (- next now)) [:tick]])]
          (case (first m)
            :arm (let [[_ ref ms dest msg every owner] m
                       ;; watch the pids an exit of which ends this timer
                       new (remove (fn [p] (or (nil? p) (contains? watched p)))
                                   (distinct [(when (act/pid? dest) dest) owner]))]
                   (run! act/monitor! new)
                   (recur (tm/arm t ref (monotonic-time) ms dest msg every owner) (into watched new)))
            :cancel (let [[_ ref to] m
                          [_ t answer] (tm/cancel t ref (monotonic-time))]
                      (when to (answer! to answer))
                      (recur t watched))
            :read (let [[_ ref to] m]
                    (answer! to (tm/left t ref (monotonic-time)))
                    (recur t watched))
            :down (let [[_ p] m] (recur (tm/gone t p) (disj watched p)))
            :tick (recur t watched)))))))

(defonce ^:private servers (atom {}))

(defn- server
  "This node's timer server, started on first use."
  []
  (let [n (act/node)
        s (get @servers n)]
    (if (and s (act/alive? s))
      s
      (let [s (act/spawn serve)]
        (get (swap! servers (fn [m] (if (and (get m n) (act/alive? (get m n))) m (assoc m n s)))) n)))))

(defn- ms-of [ms {:keys [abs]}]
  (let [ms (if abs (- ms (monotonic-time)) ms)]
    (when-not (integer? ms)
      (throw (ex-info "a timer's time is an integer of ms" {:reason :badarg :time ms})))
    (when (and (neg? ms) (not abs))
      (throw (ex-info "a timer's time is never negative" {:reason :badarg :time ms})))
    (max 0 ms)))

(defn- start!
  "Arm a timer; msg-of makes its message from its ref."
  [ms dest msg-of every owner opts]
  (let [ref (act/make-ref)]
    (act/! (server) [::arm ref (ms-of ms opts) dest (msg-of ref) every owner])
    ref))

(defn send-after
  "Send msg to dest after ms, as erlang:send_after.  With {:abs true}, ms
  is a monotonic-time deadline.  Returns the timer's ref."
  ([ms dest msg] (send-after ms dest msg {}))
  ([ms dest msg opts] (start! ms dest (constantly msg) nil nil opts)))

(defn start-timer
  "Send [:timeout ref msg] to dest after ms, as erlang:start_timer.
  Returns ref."
  ([ms dest msg] (start-timer ms dest msg {}))
  ([ms dest msg opts] (start! ms dest (fn [ref] [:timeout ref msg]) nil nil opts)))

(defn- ask
  "Ask the server to cancel or read timer ref, as erlang's /2 forms."
  [op ref {:keys [async info] :or {async false info true}}]
  (cond
    (not async) (let [p (promise)]
                  (act/! (server) [op ref p])
                  (if info @p :ok))
    info (do (act/! (server) [op ref [(act/self) (if (= op ::cancel) :cancel-timer :read-timer) ref]])
             :ok)
    :else (do (act/! (server) [op ref nil]) :ok)))

(defn cancel-timer
  "Cancel timer ref: the ms it had left, or false.  Options :async
  (default false) and :info (default true), as erlang:cancel_timer/2: an
  async cancel returns :ok, and with :info sends [:cancel-timer ref
  answer] to the caller.  A message a timer sent before the cancel may
  already be in the mailbox."
  ([ref] (cancel-timer ref {}))
  ([ref opts] (ask ::cancel ref opts)))

(defn read-timer
  "The ms timer ref has left, or false.  {:async true} sends [:read-timer
  ref answer] instead and returns :ok."
  ([ref] (read-timer ref {}))
  ([ref opts] (ask ::read ref opts)))

;; --- the timer module ------------------------------------------------------

(defn apply-after
  "Spawn (apply f args) after ms, as timer:apply_after.  Returns a ref."
  [ms f & args]
  (start! ms [::apply] (constantly [f args]) nil nil {}))

(defn send-interval
  "Send msg to dest every ms, as timer:send_interval, until cancelled or
  the calling process exits.  Returns a ref."
  ([ms msg] (send-interval ms (act/self) msg))
  ([ms dest msg] (start! ms dest (constantly msg) ms (act/self) {})))

(defn apply-interval
  "Spawn (apply f args) every ms, as timer:apply_interval, until cancelled
  or the calling process exits.  Returns a ref."
  [ms f & args]
  (start! ms [::apply] (constantly [f args]) ms (act/self) {}))

(defn exit-after
  "Send target the exit signal reason after ms, as timer:exit_after."
  [ms target reason]
  (start! ms [::exit target] (constantly reason) nil nil {}))

(defn kill-after
  "exit-after with :kill."
  [ms target]
  (exit-after ms target :kill))

(defn cancel
  "Cancel a timer the timer module started, as timer:cancel.  :ok."
  [ref]
  (cancel-timer ref {:async true :info false}))

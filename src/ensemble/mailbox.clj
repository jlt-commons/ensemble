(ns ensemble.mailbox
  "The mailbox as pure data, so the selective-receive scan can be checked as a
  law rather than sampled through fibers.

  A mailbox is a FIFO queue: [:Empty] or [:Msg value rest].  find-first walks it
  oldest first and returns the first message a compiled pattern matches,
  together with the mailbox left behind -- the matched message removed, every
  earlier (skipped) and later message still in place.  That is Erlang's
  selective receive: a non-matching message is stepped over, never discarded."
  (:require [ensemble.match :as match]))

(defn enqueue
  "Append v to the back of the mailbox."
  [mb v]
  (case (first mb)
    :Empty [:Msg v [:Empty]]
    :Msg (let [[_ hd tl] mb] [:Msg hd (enqueue tl v)])))

(defn dequeue
  "Drop the front message."
  [mb]
  (case (first mb)
    :Empty [:Empty]
    :Msg (let [[_ _ tl] mb] tl)))

(defn size
  "How many messages the mailbox holds."
  [mb]
  (case (first mb)
    :Empty 0
    :Msg (let [[_ _ tl] mb] (+ 1 (size tl)))))

(defn msgs
  "The mailbox contents, oldest first, as a list."
  [mb]
  (case (first mb)
    :Empty ()
    :Msg (let [[_ hd tl] mb] (cons hd (msgs tl)))))

(defn scan
  "Walk mb looking for the first message p matches.  On a match, [:Take v rest]
  with v the message and rest the mailbox minus v.  On no match, [:None]."
  [mb p]
  (case (first mb)
    :Empty [:None]
    :Msg (let [[_ hd tl] mb
               r (scan tl p)]
           (if (nil? (match/capture p hd))
             (case (first r)
               :None [:None]
               :Take (let [[_ v rest] r] [:Take v [:Msg hd rest]]))
             [:Take hd tl]))))

(defn find-first
  "The first message in mb matching p, or [:None].  Alias over scan."
  [mb p]
  (scan mb p))

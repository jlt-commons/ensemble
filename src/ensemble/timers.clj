(ns ensemble.timers
  "The timer table behind ensemble.timer: which timers there are, when each
  fires, and what an exit or a cancel does to them.  Pure; the timer server
  keeps one table and decides everything here.

  A timer is [:Timer ref deadline dest msg every owner]: every is nil for a
  one-shot timer and the interval in ms otherwise, owner the process an
  interval is linked to.  The table is a vector in start order.")

(defn arm
  "t with a timer ref, started at now for ms: it fires at now + ms."
  [t ref now ms dest msg every owner]
  (conj t [:Timer ref (+ now ms) dest msg every owner]))

(defn- deadline-of
  "The deadline of timer ref in t, or nil."
  [t ref]
  (some (fn [x] (case (first x) :Timer (let [[_ r at] x] (when (= r ref) at)))) t))

(defn left
  "The ms timer ref has left at now, or false: it has fired, was cancelled,
  or never was."
  [t ref now]
  (let [at (deadline-of t ref)]
    (if (and (some? at) (> at now)) (- at now) false)))

(defn cancel
  "[:Cancelled t' answer]: t without timer ref, and the time it had left
  (false if none)."
  [t ref now]
  [:Cancelled (filterv (fn [x] (case (first x) :Timer (let [[_ r] x] (not= r ref)))) t)
   (left t ref now)])

(defn- expired? [now x] (case (first x) :Timer (let [[_ _ at] x] (<= at now))))

(defn- deadline [x] (case (first x) :Timer (let [[_ _ at] x] at)))

(defn fire
  "The table after a look at now: the one-shot timers whose deadline has
  passed gone, each such interval back at its deadline plus its
  interval."
  [t now]
  (into (filterv (fn [x] (not (expired? now x))) t)
        (keep (fn [x] (case (first x)
                        :Timer (let [[_ r at d m every o] x]
                                 (when (some? every) [:Timer r (+ at every) d m every o]))))
              (sort-by deadline (filter (fn [x] (expired? now x)) t)))))

(defn due
  "[:Due fired t']: the [dest msg] of every timer whose deadline has passed
  at now, in deadline order (start order for equal deadlines), and the
  table after, as fire leaves it."
  [t now]
  [:Due (mapv (fn [x] (case (first x) :Timer (let [[_ _ _ d m] x] [d m])))
              (sort-by deadline (filter (fn [x] (expired? now x)) t)))
   (fire t now)])

(defn gone
  "t once process p has exited: without the timers to p, and without the
  intervals p started."
  [t p]
  (filterv (fn [x] (case (first x) :Timer (let [[_ _ _ d _ _ o] x] (and (not= d p) (not= o p))))) t))

(defn next-deadline
  "The earliest deadline in t, or nil when it is empty."
  [t]
  (when (seq t) (reduce min (map deadline t))))

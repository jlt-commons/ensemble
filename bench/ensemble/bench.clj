(ns ensemble.bench
  "Rough throughput numbers for the actor layer: jolt -M:bench."
  (:require [ensemble.actor :as act :refer [receive]]
            [ensemble.gen-server :as gs]))

(defn- timed [label n f]
  (let [t0 (System/nanoTime)
        _ (f)
        ms (/ (- (System/nanoTime) t0) 1e6)]
    (println (format "%-34s %8d in %8.1f ms  %10.0f /s" label n ms (/ n (/ ms 1000.0))))))

(defn spawn-join [n]
  (timed "spawn + join" n
         #(doseq [a (doall (for [_ (range n)] (act/spawn (fn [] :ok))))]
            (act/join a))))

(defn ping-pong [n]
  (timed "ping-pong round trips" n
         #(let [pong (act/spawn (fn []
                                  (loop []
                                    (receive
                                     [[:ping from] (act/! from :pong) (recur)]
                                     [:stop :done]))))
                ping (act/spawn (fn []
                                  (dotimes [_ n]
                                    (act/! pong [:ping (act/self)])
                                    (receive [:pong nil]))
                                  (act/! pong :stop)))]
            (act/join ping))))

(defn one-way [n]
  (timed "one-way sends, then drained" n
         #(let [sink (act/spawn (fn [] (dotimes [_ n] (receive [_ nil]))))]
            (dotimes [i n] (act/! sink i))
            (act/join sink))))

(defn selective [backlog n]
  (timed (str "selective past " backlog " queued") n
         #(let [a (act/spawn (fn []
                               (receive [:go nil])
                               (dotimes [_ n] (receive [[:want _] nil]))))]
            (dotimes [i backlog] (act/! a [:other i]))
            (act/! a :go)
            (dotimes [i n] (act/! a [:want i]))
            (act/join a))))

(defrecord Counter []
  gs/Server
  (init [_] 0)
  (handle-call [_ req _ st] (case req :inc [:reply (inc st) (inc st)]))
  (handle-cast [_ _ st] [:noreply st])
  (handle-info [_ _ st] [:noreply st])
  (handle-timeout [_ st] [:noreply st])
  (terminate [_ _ _] nil))

(defn calls [n]
  (let [srv (gs/start (->Counter))]
    (timed "gen_server call!, from outside" n #(dotimes [_ n] (gs/call! srv :inc)))
    (timed "gen_server call!, from an actor" n
           #(act/join (act/spawn (fn [] (dotimes [_ n] (gs/call! srv :inc))))))
    (gs/stop! srv)))

(defn -main [& _]
  (spawn-join 10000)
  (ping-pong 20000)
  (one-way 50000)
  (selective 2000 2000)
  (calls 20000))

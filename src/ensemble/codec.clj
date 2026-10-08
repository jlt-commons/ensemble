(ns ensemble.codec
  "How a frame's value becomes bytes and back.  ensemble.node hands a codec
  plain data -- each process and throwable in it already written as tagged
  data -- so a codec need only carry keywords, symbols, strings, numbers,
  characters, booleans, nil, uuids, instants, vectors, lists, maps and
  sets.  EDN is the default; a library may supply a faster one."
  (:require [clojure.edn :as edn]))

(defprotocol Codec
  (-encode [c v] "v as a byte array.")
  (-decode [c bs] "The value the byte array bs holds."))

(defrecord Edn []
  Codec
  (-encode [_ v] (.getBytes ^String (pr-str v) "UTF-8"))
  (-decode [_ bs] (edn/read-string (String. ^bytes bs "UTF-8"))))

(defn edn "The EDN codec." [] (->Edn))

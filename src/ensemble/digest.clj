(ns ensemble.digest
  "SHA-256 (FIPS 180-4) and HMAC-SHA256 (RFC 2104), for the distribution
  handshake's cookie challenge.  The runtime has no message digest of its
  own, so these are written out here.

  A message is a sequence of bytes, each an integer; a signed byte (from a
  byte array) is taken as the unsigned value it stands for.  A digest is a
  vector of 32 integers from 0 to 255.")

(def ^:private mask 0xFFFFFFFF)

(def ^:private k
  [0x428a2f98 0x71374491 0xb5c0fbcf 0xe9b5dba5 0x3956c25b 0x59f111f1 0x923f82a4 0xab1c5ed5
   0xd807aa98 0x12835b01 0x243185be 0x550c7dc3 0x72be5d74 0x80deb1fe 0x9bdc06a7 0xc19bf174
   0xe49b69c1 0xefbe4786 0x0fc19dc6 0x240ca1cc 0x2de92c6f 0x4a7484aa 0x5cb0a9dc 0x76f988da
   0x983e5152 0xa831c66d 0xb00327c8 0xbf597fc7 0xc6e00bf3 0xd5a79147 0x06ca6351 0x14292967
   0x27b70a85 0x2e1b2138 0x4d2c6dfc 0x53380d13 0x650a7354 0x766a0abb 0x81c2c92e 0x92722c85
   0xa2bfe8a1 0xa81a664b 0xc24b8b70 0xc76c51a3 0xd192e819 0xd6990624 0xf40e3585 0x106aa070
   0x19a4c116 0x1e376c08 0x2748774c 0x34b0bcb5 0x391c0cb3 0x4ed8aa4a 0x5b9cca4f 0x682e6ff3
   0x748f82ee 0x78a5636f 0x84c87814 0x8cc70208 0x90befffa 0xa4506ceb 0xbef9a3f7 0xc67178f2])

(def ^:private h0
  [0x6a09e667 0xbb67ae85 0x3c6ef372 0xa54ff53a 0x510e527f 0x9b05688c 0x1f83d9ab 0x5be0cd19])

(defn- rotr
  "x, a 32-bit word, rotated right by n."
  [x n]
  (bit-or (unsigned-bit-shift-right x n)
          ;; the low n bits, moved to the top: masked first, so the shift
          ;; stays within 32 bits
          (bit-shift-left (bit-and x (dec (bit-shift-left 1 n))) (- 32 n))))

(defn- add [& xs] (bit-and (reduce + xs) mask))

(defn- padded
  "The message with SHA-256's padding: a 1 bit, zeros, and its length in
  bits as 8 bytes, to a multiple of 64 bytes."
  [bs]
  (let [n (count bs)
        zeros (mod (- 55 n) 64)
        bits (* 8 n)]
    (-> (transient (vec bs))
        (conj! 0x80)
        (as-> v (reduce conj! v (repeat zeros 0)))
        (as-> v (reduce conj! v (for [i (range 7 -1 -1)] (bit-and (unsigned-bit-shift-right bits (* 8 i)) 0xff))))
        persistent!)))

(defn- schedule
  "The 64 words of the message schedule for one 64-byte block."
  [block]
  (let [w (mapv (fn [i] (bit-or (bit-shift-left (nth block (* 4 i)) 24)
                                (bit-shift-left (nth block (+ 1 (* 4 i))) 16)
                                (bit-shift-left (nth block (+ 2 (* 4 i))) 8)
                                (nth block (+ 3 (* 4 i)))))
                (range 16))]
    (loop [w (transient w), t 16]
      (if (= t 64)
        (persistent! w)
        (let [x (nth w (- t 15))
              y (nth w (- t 2))
              s0 (bit-xor (rotr x 7) (rotr x 18) (unsigned-bit-shift-right x 3))
              s1 (bit-xor (rotr y 17) (rotr y 19) (unsigned-bit-shift-right y 10))]
          (recur (conj! w (add (nth w (- t 16)) s0 (nth w (- t 7)) s1)) (inc t)))))))

(defn- compress
  "The hash state after one block."
  [hs block]
  (let [w (schedule block)]
    (loop [i 0, [a b c d e f g h] hs]
      (if (= i 64)
        (mapv add hs [a b c d e f g h])
        (let [s1 (bit-xor (rotr e 6) (rotr e 11) (rotr e 25))
              ch (bit-xor (bit-and e f) (bit-and (bit-xor e mask) g))
              t1 (add h s1 ch (nth k i) (nth w i))
              s0 (bit-xor (rotr a 2) (rotr a 13) (rotr a 22))
              maj (bit-xor (bit-and a b) (bit-and a c) (bit-and b c))
              t2 (add s0 maj)]
          (recur (inc i) [(add t1 t2) a b c (add d t1) e f g]))))))

(defn- unsigned [bs] (map #(bit-and % 0xff) bs))

(defn sha-256
  "The SHA-256 digest of the bytes bs."
  [bs]
  (let [m (padded (unsigned bs))
        hs (reduce compress h0 (map #(subvec m % (+ % 64)) (range 0 (count m) 64)))]
    (vec (for [x hs, i [24 16 8 0]] (bit-and (unsigned-bit-shift-right x i) 0xff)))))

(defn hmac-sha-256
  "The HMAC-SHA256 of the bytes msg under the bytes key."
  [key msg]
  (let [key (vec (unsigned key))
        key (if (> (count key) 64) (sha-256 key) key)
        key (into key (repeat (- 64 (count key)) 0))
        pad (fn [b] (mapv #(bit-xor % b) key))]
    (sha-256 (concat (pad 0x5c) (sha-256 (concat (pad 0x36) (unsigned msg)))))))

(defn hex
  "A digest as lowercase hex."
  [bs]
  (apply str (map #(let [s (Integer/toHexString (bit-and % 0xff))] (if (= 1 (count s)) (str "0" s) s)) bs)))

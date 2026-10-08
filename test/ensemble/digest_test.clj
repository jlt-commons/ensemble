(ns ensemble.digest-test
  "SHA-256 and HMAC-SHA256 against the published test vectors (FIPS 180-2,
  RFC 4231)."
  (:require [clojure.test :refer [deftest is]]
            [ensemble.digest :as d]))

(defn- utf8 [s] (vec (.getBytes ^String s "UTF-8")))

(deftest sha-256-vectors
  (is (= "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" (d/hex (d/sha-256 (utf8 "")))))
  (is (= "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad" (d/hex (d/sha-256 (utf8 "abc")))))
  (is (= "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"
         (d/hex (d/sha-256 (utf8 "abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq")))))
  (is (= "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0"
         (d/hex (d/sha-256 (vec (repeat 1000000 (int \a))))))))

(deftest a-message-at-each-padding-boundary
  ;; 55, 56 and 64 bytes: the length either fits in the last block or does not
  (is (= "9f4390f8d30c2dd92ec9f095b65e2b9ae9b0a925a5258e241c9f1e910f734318" (d/hex (d/sha-256 (vec (repeat 55 97))))))
  (is (= "b35439a4ac6f0948b6d6f9e3c6af0f5f590ce20f1bde7090ef7970686ec6738a" (d/hex (d/sha-256 (vec (repeat 56 97))))))
  (is (= "ffe054fe7ae0cb6dc65c3af9b61d5209f439851db43d0ba5997337df154668eb" (d/hex (d/sha-256 (vec (repeat 64 97)))))))

(deftest hmac-sha-256-vectors
  (is (= "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
         (d/hex (d/hmac-sha-256 (vec (repeat 20 0x0b)) (utf8 "Hi There")))))
  (is (= "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
         (d/hex (d/hmac-sha-256 (utf8 "Jefe") (utf8 "what do ya want for nothing?")))))
  ;; a key longer than a block is hashed first
  (is (= "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54"
         (d/hex (d/hmac-sha-256 (vec (repeat 131 0xaa))
                                (utf8 "Test Using Larger Than Block-Size Key - Hash Key First"))))))

(deftest bytes-of-a-signed-array-are-taken-unsigned
  (is (= (d/sha-256 [200 1]) (d/sha-256 (vec (byte-array [(unchecked-byte 200) 1]))))))

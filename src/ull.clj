(ns ull
  "UltraLogLog sketching with serialization.
   UltraLogLog is a data structure for estimating the number of distinct elements in a stream."
  (:import
   [com.dynatrace.hash4j.hashing Hashing]
   [com.dynatrace.hash4j.distinctcount UltraLogLog]))

;## Utility functions

(defn bytes->ull
  "Convert a byte array to an UltraLogLog instance."
  ^UltraLogLog [bytes]
  (UltraLogLog/wrap bytes))

(defn ull-state
  "Get the state of an UltraLogLog instance."
  ^bytes [^UltraLogLog ull]
  (.getState ull))

(defprotocol EstimatedCounter
  "A protocol for estimating the number of distinct elements."
  (estimate-count
    [this]
    "Estimate the number of distinct elements.")
  (add-string
    [this s]
    "add a string to the counter."))

(let [hashers {:komihash5-0 (Hashing/komihash5_0)}]
  (defn kw->hasher
    "convert a keyword `kw` to a hasher instance."
    ^Hashing [kw]
    (or (hashers kw) (Hashing/komihash5_0))))

;## Implementation
(defrecord UltraLogLogWrapper [bytes hasher-name]
  EstimatedCounter
  (estimate-count
   [_]
   (-> bytes bytes->ull .getDistinctCountEstimate Math/round))
  (add-string
   [_ x]
   (let [ull (bytes->ull bytes)
         ^Hashing hasher (kw->hasher hasher-name)]
     (.add ull (.hashCharsToLong hasher x))
     (UltraLogLogWrapper. (ull-state ull) hasher-name))))

(defn create-ull 
  "Create an UltraLogLog EstimatedCounter instance with the given `precision` and optional `hasher-name`.
    - `precision` is the number of bits used to estimate the number of distinct elements.
    - `hasher-name` is the name of the hasher to use, default to `:komihash5-0`"
  ([precision]
   (create-ull precision :komihash5-0))
  ([precision hasher-name]
   (-> precision
       UltraLogLog/create
       ull-state
       (UltraLogLogWrapper. hasher-name))))

^:rct/test
(comment 
  ;test ULL and its serialization
  (with-open [bos (java.io.ByteArrayOutputStream.)
              oos (java.io.ObjectOutputStream. bos)]
    (.writeObject oos (-> (create-ull 16) (add-string "foo") (add-string "bar") (add-string "foo")))
    (let [bis (java.io.ByteArrayInputStream. (.toByteArray bos))
          ois (java.io.ObjectInputStream. bis)]
      (-> ois .readObject estimate-count))) ;=>
  2
  )

(ns robertluo.ull
  "UltraLogLog sketching with serialization.
   UltraLogLog is a data structure for estimating the number of distinct elements in a stream."
  (:require
   [taoensso.nippy :as nippy])
  (:import
   [com.dynatrace.hash4j.hashing Hashing Hasher64]
   [com.dynatrace.hash4j.distinctcount UltraLogLog]))

(defprotocol EstimatedCounter
  "A protocol for estimating the number of distinct elements."
  (estimate-count
    [this]
    "Estimate the number of distinct elements.")
  (add-string
    [this s]
    "add a string to the counter.")
  (union
    [this other]
    "union `this` with `other`, returns a new counter."))

(def ^:private supported-hashers
  "All supported hasher for UltraLogLog, has to be hash64 compatible."
  {:komihash5-0 (Hashing/komihash5_0)
   :komihash4-3 (Hashing/komihash4_3)
   :murmur3-128 (Hashing/murmur3_128)})

(defn ^:const kw->hasher
  "convert a keyword `kw` to a hasher instance."
  ^Hasher64 [kw]
  (get supported-hashers kw
       (ex-info (str "Unknown hasher: " kw) {:kw kw})))

;## Implementation
(deftype UltraLogLogWrapper [^UltraLogLog ull hasher-name]
  EstimatedCounter
  (estimate-count
    [_]
    (-> ull .getDistinctCountEstimate Math/round))
  (add-string
    [_ x]
    (let [^Hasher64 hasher (kw->hasher hasher-name)
          ull (.add ull (.hashCharsToLong hasher x))]
      (UltraLogLogWrapper. ull hasher-name)))
  (union
    [_ other] 
    (-> ull
        (UltraLogLog/merge (.ull ^UltraLogLogWrapper other))
        (UltraLogLogWrapper. hasher-name))))

(defn create-ull 
  "Create an UltraLogLog EstimatedCounter instance with the given `precision` and optional `hasher-name`.
    - `precision` is the number of bits used to estimate the number of distinct elements.
    - `hasher-name` is the name of the hasher to use, default to `:komihash5-0`, avaiable hashers are
      `:komihash5-0`(default) `:komihash4-3` `:murmur3-128`"
  ([]
   (create-ull 16))
  ([precision]
   (create-ull precision :komihash5-0))
  ([precision hasher-name]
   (-> precision
       UltraLogLog/create
       (UltraLogLogWrapper. hasher-name))))

(defn words-counter
  "returns a counter with the given `words` added for `ull`."
  [counter & words]
  (reduce add-string counter words))

^:rct/test
(comment 
  (for [hasher (keys supported-hashers)]
    (-> (words-counter (create-ull 16 hasher) "foo" "bar" "baz" "foo") 
        (estimate-count))) ;=>>
  #(every? (fn [x] (= 3 x)) %)
  
  (-> (-> (create-ull) (words-counter "foo" "bar" "baz" "foo"))
      (union (-> (create-ull) (words-counter "secret" "baz")))
      (estimate-count)) ;=> 4 
  )

;;## Nippy de/serialization

#_{:clj-kondo/ignore [:unresolved-symbol]}
(nippy/extend-freeze 
 UltraLogLogWrapper
 ::ultraloglog-wrapper
 [^UltraLogLogWrapper x ^java.io.DataOutput out]
 (let [state (-> x .ull .getState)]
   (.writeLong out (alength state))
   (.write out state)
   (nippy/freeze-to-out! out (.hasher-name x))))

#_{:clj-kondo/ignore [:unresolved-symbol]}
(nippy/extend-thaw
 ::ultraloglog-wrapper
 [^java.io.DataInput in]
 (let [len (.readLong in)]
   (if (pos? len)
     (let [bytes (byte-array len)]
       (.readFully in bytes) 
       (UltraLogLogWrapper. (UltraLogLog/wrap bytes) (nippy/thaw-from-in! in)))
     (throw (ex-info "Invalid length of frozen UltraLogLogWrapper" {:len len})))))

^:rct/test
(comment
  (-> (create-ull) (nippy/freeze) alength) ;=> 294
  (for [hasher (keys supported-hashers)]
    (-> (create-ull 16 hasher)
        (nippy/freeze)
        (nippy/thaw)
        (estimate-count))) ;=>>
  #(every? (fn [x] (zero? x)) %)
  )

;;## Pretty print
(defmethod print-method UltraLogLogWrapper
  [^UltraLogLogWrapper x ^java.io.Writer wtr]
  (.write wtr (format "#ull{:hasher %s :estimate-count %d}" (.hasher-name x) (estimate-count x))))
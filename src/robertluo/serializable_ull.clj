(ns robertluo.serializable-ull
  "UltraLogLog sketching with serialization"
  (:import
   [com.dynatrace.hash4j.hashing Hashing]
   [com.dynatrace.hash4j.distinctcount UltraLogLog]))

(defn bytes->ull
  ^UltraLogLog [bytes]
  (UltraLogLog/wrap bytes))

(defn get-state
  ^bytes [^UltraLogLog ull]
  (.getState ull))

(defprotocol EstimatedCounter
  (-estimate-count [this])
  (-add [this x]))

(let [hashers {:komihash5-0 (Hashing/komihash5_0)}]
  (defn kw->hasher
    [kw]
    (or (hashers kw) (Hashing/komihash5_0))))

(defn kw->hasher [kw]
  (or ({:komihash5-0 (Hashing/komihash5_0)} kw) (Hashing/komihash5_0)))

(defrecord UltraLogLogWrapper [bytes hasher-name]
  EstimatedCounter
  (-estimate-count
   [_]
   (-> bytes bytes->ull .getDistinctCountEstimate Math/round))
  (-add
   [_ x]
   (let [ull (bytes->ull bytes)
         hasher (kw->hasher hasher-name)]
     (.add ull (.hashCharsToLong hasher x))
     (UltraLogLogWrapper. (get-state ull) hasher-name))))

(defn create-ull 
  ([precision]
   (create-ull precision :komihash5-0))
  ([precision hasher-name]
   (-> precision
       UltraLogLog/create
       get-state
       (UltraLogLogWrapper. hasher-name))))

(comment
  (-> 16
      create-ull
      (-add "foo")
      (-add "bar")
      (-add "foo")
      -estimate-count)
  (with-open [bos (java.io.ByteArrayOutputStream.)
              oos (java.io.ObjectOutputStream. bos)]
    (.writeObject oos (-> (create-ull 16) (-add "foo") (-add "bar") (-add "foo")))
    (let [bis (java.io.ByteArrayInputStream. (.toByteArray bos))
          ois (java.io.ObjectInputStream. bis)]
      (-> ois .readObject -estimate-count))))
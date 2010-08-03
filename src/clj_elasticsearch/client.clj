(ns clj-elasticsearch.client
  (:import (org.elasticsearch.node NodeBuilder)
           (org.elasticsearch.common.xcontent XContentFactory)))

(def *client*)

(defn make-node
  [args]
  (let [nodebuilder (NodeBuilder.)]
    (. nodebuilder client true)
    (if (:local args) (. nodebuilder local true))
    (. nodebuilder node)))

(defn get-client
  [node]
  (. node client))

(defmacro with-server
  [server-spec & body]
  `(with-open [node# (make-node ~server-spec)]
    (binding [*client* (get-client node#)]
      (do
        ~@body))))  

(defn build-document
  [builder doc]
  (.startObject builder)
  (doseq [[field value] doc]
    (.field builder (name field) value))
  (.endObject builder))

(defn make-xson
  [doc]
  (build-document (XContentFactory/xsonBuilder) doc))

(defn make-json
  [doc]
  (build-document (XContentFactory/jsonBuilder) doc))



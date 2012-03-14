(ns clj-elasticsearch.client
  (:require [cheshire.core :as json])
  (:import [org.elasticsearch.node NodeBuilder]
           [org.elasticsearch.common.xcontent XContentFactory ToXContent$Params]
           [org.elasticsearch.common.settings ImmutableSettings]
           [org.elasticsearch.action.admin.indices.status IndicesStatusRequest]
           [org.elasticsearch.common.io FastByteArrayOutputStream]
           [org.elasticsearch.client.transport TransportClient]
           [org.elasticsearch.client.support AbstractClient]
           [org.elasticsearch.common.transport InetSocketTransportAddress]
           [org.elasticsearch.action  ActionListener]))

(def ^{:dynamic true} *client*)

(defn update-settings-builder
  ([builder settings]
     (doseq [[k v] settings]
       (if (or (vector? v) (list? v))
         (.putArray builder (name k) (into-array String (map str v)))
         (.put builder (name k) (str v))))
     builder)
  ([settings]
     (update-settings-builder (ImmutableSettings/settingsBuilder) settings)))

(defn make-node
  [{:keys [local-mode client-mode load-config cluster-name settings hosts]
    :or {client-mode true
         load-config false
         local-mode false
         settings {}}
    :as args}]
  (let [nodebuilder (NodeBuilder.)
        host-conf (if hosts {"discovery.zen.ping.unicast.hosts" hosts
                             "discovery.zen.ping.multicast.enabled" false}
                      {})]
    (doto nodebuilder
      (.client client-mode)
      (.local local-mode)
      (.loadConfigSettings load-config))
    (if cluster-name
      (.clusterName nodebuilder cluster-name))
    (update-settings-builder (.settings nodebuilder) (merge settings host-conf))
    (.node nodebuilder)))

(defn make-inet-address
  [spec]
  (let [m (re-matcher #"([^\[\:]+)[\[\:]?(\d*)" spec)
        _ (.find m)
        [_ host p] (re-groups m)
        port (if p (Integer/parseInt (str p)) 9300)]
    (InetSocketTransportAddress. host port)))

(defn make-transport-client
  [{:keys [load-config cluster-name settings hosts sniff]
    :or {client-mode true
         load-config false
         local-mode false
         settings {}
         sniff true}
    :as args}]
  (let [settings (if cluster-name (assoc settings "cluster.name" cluster-name) settings)
        conf (update-settings-builder (merge settings {"client.transport.sniff" sniff}))
        client (TransportClient. conf load-config)]
    (doseq [host hosts]
      (.addTransportAddress client (make-inet-address host)))
    client))

(defn make-content-builder
  [& [type]]
  (case type
    :json (XContentFactory/jsonBuilder)
    :smile (XContentFactory/smileBuilder)
    (XContentFactory/smileBuilder)))

(defn convert-response
  [response & [type]]
  (let [os (FastByteArrayOutputStream.)
        builder (if (= type :json)
                  (XContentFactory/jsonBuilder os)
                  (XContentFactory/smileBuilder os))
        funny-request (proxy [ToXContent$Params] [])]
    (.startObject builder)
    (.toXContent response builder funny-request)
    (.endObject builder)
    (.flush builder)
    (if (= type :json)
      (.toString os "UTF-8")
      (json/decode-smile (.underlyingBytes os) true))))

(defn make-client
  [type spec]
  (case type
    :node (.client (make-node spec))
    :transport (make-transport-client spec)
    (make-transport-client spec)))

(defmacro with-node-client
  [server-spec & body]
  `(with-open [node# (make-node ~server-spec)]
    (binding [clj-elasticsearch.client/*client* (.client node#)]
      (do
        ~@body))))

(defmacro with-transport-client
  [server-spec & body]
  `(with-open [client# (make-client :transport ~server-spec)]
    (binding [clj-elasticsearch.client/*client* client#]
      (do
        ~@body))))

(defn build-document
  [doc]
  (let [builder (XContentFactory/smileBuilder)]
    (.startObject builder)
    (doseq [[field value] doc]
      (.field builder (name field) value))
    (.endObject builder)
    builder))

(defn get-index-admin-client
  [client]
  (-> client (.admin) (.indices)))

(defn get-cluster-admin-client
  [client]
  (-> client (.admin) (.cluster)))

(defn index-status
  ([client {:keys [indices snapshot? threaded? recovery? cb]
            :or {snapshot? false
                 threaded? true
                 recovery? false}}]
     (let [i (get-index-admin-client client)
           req (if indices (IndicesStatusRequest. (into-array (map name indices)))
                   (IndicesStatusRequest.))]
       (doto req
         (.snapshot snapshot?)
         (.recovery recovery?)
         (.listenerThreaded threaded?))
       (if cb
         (.status i req cb)
         (convert-response (.actionGet (.status i req))))))
  ([args]
     (index-status *client* args)))

(defn search
  ([client {:keys [source from size indices types format threaded?]
            :or {from 0 size 10 threaded? true}
            :as req} cb]
     (let [builder (.prepareSearch client (into-array (map name indices)))]
       (doto builder
           (.setExtraSource source)
           (.setFrom from)
           (.setSize size)
           (.setListenerThreaded threaded?))
       (if types (.setTypes builder (into-array (map name types))))
       (if cb
         (.execute builder cb)
         (convert-response (.actionGet (.execute builder)) format))))
  ([arg1 arg2]
     (if (instance? AbstractClient arg1)
       (search arg1 arg2 nil)
       (search *client* arg1 arg2)))
  ([req]
     (search *client* req nil)))

(defn make-listener
  [{:keys [on-failure on-response]}]
  (proxy [ActionListener] []
    (onFailure [e] (on-failure e))
    (onResponse [r] (on-response r))))
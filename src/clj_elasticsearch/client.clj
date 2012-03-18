(ns clj-elasticsearch.client
  (:require [cheshire.core :as json]
            [clojure.string :as str])
  (:import [org.elasticsearch.node NodeBuilder]
           [org.elasticsearch.common.xcontent XContentFactory ToXContent$Params]
           [org.elasticsearch.common.settings ImmutableSettings]
           [org.elasticsearch.action.admin.indices.status IndicesStatusRequest]
           [org.elasticsearch.common.io FastByteArrayOutputStream]
           [org.elasticsearch.client.transport TransportClient]
           [org.elasticsearch.client.support AbstractClient]
           [org.elasticsearch.common.transport InetSocketTransportAddress]
           [org.elasticsearch.action  ActionListener]
           [org.elasticsearch.common.xcontent ToXContent]))

(def ^{:dynamic true} *client*)

(defprotocol Clojurable
  "Protocol for conversion of Response Classes to Clojure"
  (convert [response format] "convert response to format"))

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

(defn convert-xcontent
  [response & [format]]
  (let [os (FastByteArrayOutputStream.)
        builder (if (= type :json)
                  (XContentFactory/jsonBuilder os)
                  (XContentFactory/smileBuilder os))
        params org.elasticsearch.action.search.SearchResponse/EMPTY_PARAMS]
    (.startObject builder)
    (.toXContent response builder params)
    (.endObject builder)
    (.flush builder)
    (if (= type :json)
      (.toString os "UTF-8")
      (json/decode-smile (.underlyingBytes os) true))))

(defn method->arg
  [method]
  (let [name (.getName method)
        parameter (first (seq (.getParameterTypes method)))
        conv (str/replace name #"^set|get" "")
        conv (str/lower-case (str/replace conv #"(\p{Lower})(\p{Upper})" "$1-$2"))
        added (if (and parameter (= parameter java.lang.Boolean/TYPE)) (str conv "?") conv)]
    added))

(defmacro def-converter
  [fn-name class-name]
  (let [klass (Class/forName class-name)
        methods (.getMethods klass)
        getters-m  (filter #(let [n (.getName %)]
                              (and (.startsWith n "get")
                                   (not (#{"getClass" "getShardFailures"} n)))) methods)
        sig (reduce (fn [acc m]
                      (let [m-name (.getName m)]
                        (assoc acc
                          (keyword (method->arg m))
                          (symbol (str "." m-name)))))
                    {} getters-m)
        response (gensym "response")]
    `(defn ~fn-name
       [~(with-meta response {:tag klass}) & [format#]]
       (let [res# (hash-map
                  ~@(apply concat
                           (for [[kw getter] sig]
                             `(~kw (~getter ~response)))))]
         (if (= format# :json)
           (json/generate-string res#)
           res#)))))

(defmacro def-converters
  [& conv-defs]
  `(do ~@(for [conv-def conv-defs]
           `(do (def-converter ~@conv-def)
                (extend ~(symbol (second conv-def))
                  Clojurable
                  {:convert (fn [response# format#] (~(symbol (str "clj-elasticsearch.client/" (first conv-def))) response# format#))})))))

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

(defn is-settable-method?
  [klass method]
  (let [return (.getReturnType method)
        super (.getSuperclass klass)
        allowed #{klass super}
        parameters (.getParameterTypes method)
        nb-params (alength parameters)]
    (and (allowed return) (= nb-params 1))))

(defn is-execute-method?
  [klass method]
  (let [return (.getReturnType method)
        parameters (into #{} (seq (.getParameterTypes method)))
        nb-params (count parameters)]
    (and (contains? parameters klass) (= nb-params 1))))

(defn get-settable-methods
  [class-name]
  (let [klass (Class/forName class-name)
        methods (.getMethods klass)
        settable (filter #(is-settable-method? klass %) (seq methods))]
    settable))

(defn get-execute-method
  [request-class-name client-class-name]
  (let [c-klass (Class/forName client-class-name)
        r-klass (Class/forName request-class-name)
        methods (.getMethods c-klass)
        executable (first (filter #(is-execute-method? r-klass %) (seq methods)))]
    executable))

(defn request-signature
  [class-name]
  (let [methods (get-settable-methods class-name)
        args (map method->arg methods)]
    (zipmap (map keyword args)
            methods)))

(defn acoerce
  [val]
  (if (or (vector? val) (list? val))
    (into-array val)
    val))

(defmacro defn-request
  [fn-name request-class-name cst-args client-class-name]
  (let [r-klass (Class/forName request-class-name)
        sig (request-signature request-class-name)
        c-klass (Class/forName client-class-name)
        method (get-execute-method request-class-name client-class-name)
        response-klass (.getReturnType method)
        response-type (cond
                       (some #(=  %)
                             (seq (.getInterfaces response-klass))) :xcontent
                       :else :bean)
        m-name (symbol (str "." (.getName method)))
        args (remove (into #{} cst-args) (keys sig))
        arglists [['options] ['client `{:keys [~@(map #(-> % name symbol) (conj args "listener" "format"))] :as ~'options}]]
        cst-gensym (take (count cst-args) (repeatedly gensym))
        signature (reduce (fn [acc [k v]] (assoc acc k (symbol (str "." (.getName v))))) {} sig)
        request (gensym "request")
        options (gensym "options")
        client (gensym "client")]
    `(defn
       ~fn-name
       {:doc (format "Required args: %s. Generated from class %s" ~(pr-str cst-args) ~request-class-name)
        :arglists '(~@arglists)}
       ([~client options#]
          (let [client# ~@(case client-class-name
                            "org.elasticsearch.client.internal.InternalClient" `(~client)
                            "org.elasticsearch.client.IndicesAdminClient"
                            `((get-index-admin-client ~client))
                            "org.elasticsearch.client.ClusterAdminClient"
                            `((get-cluster-admin-client ~client)))
                [~@cst-gensym] (map acoerce (vals (select-keys options# [~@cst-args])))
                ~request (new ~r-klass ~@cst-gensym)
                ~options (dissoc options# ~@cst-args)]
            ~@(for [[k met] signature] `(when (contains?  ~options ~k)
                                          (~met ~request (acoerce (get ~options ~k)))))
            (if (get ~options :listener)
              (~m-name client# ~request (:listener ~options))
              (convert (.actionGet (~m-name client# ~request)) (:format ~options)))))
       ([options#]
          (~fn-name *client* options#)))))

(defmacro def-requests
  [client-class-name & request-defs]
  `(do ~@(map (fn [req-def]
                `(defn-request ~@(concat req-def [client-class-name])))
              request-defs)))

(def-converters
  (convert-count "org.elasticsearch.action.count.CountResponse")
  (convert-delete "org.elasticsearch.action.delete.DeleteResponse")
  (convert-delete-by-query "org.elasticsearch.action.deletebyquery.DeleteByQueryResponse")
  (convert-index "org.elasticsearch.action.index.IndexResponse")
  (convert-optimize "org.elasticsearch.action.admin.indices.optimize.OptimizeResponse")
  (convert-analyze "org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse")
  (convert-clear-cache "org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheResponse")
  (convert-create-index "org.elasticsearch.action.admin.indices.create.CreateIndexResponse")
  (convert-delete-index "org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse")
  (convert-delete-mapping "org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingResponse")
  (convert-exists-index "org.elasticsearch.action.admin.indices.exists.IndicesExistsResponse")
  (convert-flush-request "org.elasticsearch.action.admin.indices.flush.FlushResponse")
  (convert-gateway-snapshot "org.elasticsearch.action.admin.indices.gateway.snapshot.GatewaySnapshotResponse")
  (convert-put-mapping "org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse")
  (convert-put-template "org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse")
  (convert-refresh-index "org.elasticsearch.action.admin.indices.refresh.RefreshResponse")
  (convert-update-index-settings "org.elasticsearch.action.admin.indices.settings.UpdateSettingsResponse"))

(extend-type ToXContent
  Clojurable
  (convert [response format] (convert-xcontent response format)))

(def-requests "org.elasticsearch.client.internal.InternalClient"
  (index-doc "org.elasticsearch.action.index.IndexRequest" [])
  (search "org.elasticsearch.action.search.SearchRequest" [])
  (get-doc "org.elasticsearch.action.get.GetRequest" [:index :type :id])
  (count-docs "org.elasticsearch.action.count.CountRequest" [:indices])
  (delete-doc "org.elasticsearch.action.delete.DeleteRequest" [:index :type :id])
  (delete-by-query "org.elasticsearch.action.deletebyquery.DeleteByQueryRequest" [])
  (more-like-this "org.elasticsearch.action.mlt.MoreLikeThisRequest" [:index])
  (percolate "org.elasticsearch.action.percolate.PercolateRequest" []))

(def-requests "org.elasticsearch.client.IndicesAdminClient"
  (optimize-index "org.elasticsearch.action.admin.indices.optimize.OptimizeRequest" [])
  (analyze-request "org.elasticsearch.action.admin.indices.analyze.AnalyzeRequest" [:index :text])
  (clear-index-cache "org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheRequest" [:indices])
  (close-index "org.elasticsearch.action.admin.indices.close.CloseIndexRequest" [:index])
  (create-index "org.elasticsearch.action.admin.indices.create.CreateIndexRequest" [:index])
  (delete-index "org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest" [:indices])
  (delete-mapping "org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingRequest" [:indices])
  (delete-template "org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateRequest" [:name])
  (exists-index "org.elasticsearch.action.admin.indices.exists.IndicesExistsRequest" [:indices])
  (flush-index "org.elasticsearch.action.admin.indices.flush.FlushRequest" [:indices])
  (gateway-snapshot "org.elasticsearch.action.admin.indices.gateway.snapshot.GatewaySnapshotRequest" [:indices])
  (put-mapping "org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest" [:indices])
  (put-template "org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest" [:name])
  (refresh-index "org.elasticsearch.action.admin.indices.refresh.RefreshRequest" [:indices])
  (index-segments "org.elasticsearch.action.admin.indices.segments.IndicesSegmentsRequest" [])
  (index-stats "org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest" [])
  (index-status "org.elasticsearch.action.admin.indices.status.IndicesStatusRequest" [])
  (update-index-settings "org.elasticsearch.action.admin.indices.settings.UpdateSettingsRequest" [:indices]))

(defn make-listener
  [{:keys [on-failure on-response]}]
  (proxy [ActionListener] []
    (onFailure [e] (on-failure e))
    (onResponse [r] (on-response r))))
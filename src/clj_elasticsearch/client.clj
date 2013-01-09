(ns clj-elasticsearch.client
  (:require [cheshire.core :as json]
            [clojure.string :as str]
            [gavagai.core :as gav]
            [clojure.stacktrace :as cst])
  (:import [org.elasticsearch.node NodeBuilder]
           [org.elasticsearch.common.xcontent XContentFactory ToXContent$Params]
           [org.elasticsearch.common.settings ImmutableSettings ImmutableSettings$Builder]
           [org.elasticsearch.action.admin.indices.status IndicesStatusRequest]
           [org.elasticsearch.action ActionFuture]
           [org.elasticsearch.common.io FastByteArrayOutputStream]
           [org.elasticsearch.client.transport TransportClient]
           [org.elasticsearch.client.support AbstractClient]
           [org.elasticsearch.node Node]
           [org.elasticsearch.client Client ClusterAdminClient IndicesAdminClient]
           [org.elasticsearch.common.transport InetSocketTransportAddress]
           [org.elasticsearch.action  ActionListener]
           [org.elasticsearch.common.xcontent ToXContent]
           [org.elasticsearch Version]
           [org.elasticsearch.action.search SearchType]
           [java.lang.reflect Method Field Constructor]))

(def ^{:dynamic true} *client*)

(defprotocol FromSource
  (convert-source [this] "convert source to bytes"))

(extend-protocol FromSource
  String
  (convert-source [this] (json/encode-smile (json/decode this)))
  java.util.Map
  (convert-source [this] (json/encode-smile this))
  Object
  (convert-source [this] this))

(defn update-settings-builder
  "creates or updates a settingsBuilder with the given hash-map"
  (^ImmutableSettings$Builder
   [^ImmutableSettings$Builder builder settings]
   (doseq [[k v] settings]
     (cond
      (or (number? v) (true? v) (false? v)) (.put builder (name k) v)
      (or (vector? v) (list? v)) (.putArray builder (name k) (into-array String (map str v)))
      :else (.put builder (name k) (str v))))
   builder)
  (^ImmutableSettings$Builder
   [settings]
   (update-settings-builder (ImmutableSettings/settingsBuilder) settings)))

(defn find-terminals
  [spec]
  (letfn [(deconstruct [m] (map (fn [[k v]] [(list (name k)) v]) (seq m)))]
    (loop [todo (deconstruct spec)
           paths []]
      (if-let [[path v] (first todo)]
        (if (map? v)
          (let [new (map (fn [[p v]]
                           [(concat path p) v])
                         (deconstruct v))]
            (recur (concat (rest todo) new) paths))
          (recur (rest todo) (conj paths [path v])))
        paths))))

(defn collapse-tree
  [m]
  (let [leaves (find-terminals m)]
    (reduce
     (fn [sts [p v]]
       (assoc sts (str/join "." p) v))
     {} leaves)))

(defn make-node
  "makes a new native node client"
  ^org.elasticsearch.node.Node
  [{:keys [local-mode client-mode load-config cluster-name settings hosts]
    :or {client-mode true
         load-config false
         local-mode false
         settings {}}
    :as args}]
  (let [nodebuilder (NodeBuilder.)
        host-conf (collapse-tree
                   (if hosts
                     {:discovery {:zen {:ping
                                        {:unicast {:hosts hosts}
                                         :multicast {:enabled false}}}}}
                     {}))
        flat-settings (if (and
                           (every? string? (keys settings))
                           (not-any? map? (vals settings)))
                        settings
                        (collapse-tree settings))]
    (doto nodebuilder
      (.client client-mode)
      (.local local-mode)
      (.loadConfigSettings load-config))
    (if cluster-name
      (.clusterName nodebuilder cluster-name))
    (let [sbuilder (update-settings-builder (.settings nodebuilder) (merge flat-settings host-conf))]
      (.settings nodebuilder (.build sbuilder)))
    (.node nodebuilder)))

(defn- make-inet-address
  [spec]
  (let [m (re-matcher #"([^\[\:]+)[\[\:]?(\d*)" spec)
        _ (.find m)
        [_ ^String host p] (re-groups m)
        ^Integer port (if (and p (not (empty? p))) (Integer/parseInt (str p)) 9300)]
    (InetSocketTransportAddress. host port)))

(defn make-transport-client
  "creates a transport client"
  [{:keys [^Boolean load-config cluster-name settings hosts sniff]
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

(defn- make-content-builder
  [& [type]]
  (case type
    :json (XContentFactory/jsonBuilder)
    :smile (XContentFactory/smileBuilder)
    (XContentFactory/smileBuilder)))

(defn after-0-19?
  []
  (try
    (>= (.minor Version/CURRENT) 19)
    (catch Exception _
      false)))

(defn- make-compatible-decode-smile
  "produces a fn for backward compatibility with es prior 0.19.0"
  []
  (if (after-0-19?)
    (fn [^FastByteArrayOutputStream os] (json/decode-smile (.. os bytes toBytes) true))
    (fn [^FastByteArrayOutputStream os] (json/decode-smile (.underlyingBytes os) true))))

(def compatible-decode-smile (make-compatible-decode-smile))

(defn- convert-source-result
  [src]
  (cond
   (instance? java.util.HashMap src) (into {}
                                           (map (fn [^java.util.Map$Entry e]
                                                  [(.getKey e)
                                                   (convert-source-result (.getValue e))]) src))
   (instance? java.util.ArrayList src) (into [] (map convert-source-result src))
   :else src))

(defn- convert-fields
  [^java.util.HashMap hm]
  (into {} (map (fn [^org.elasticsearch.index.get.GetField f]
                  [(.getName f) (convert-source-result (.getValue f))]) (.values hm))))

(defn- convert-get
  [_ ^org.elasticsearch.action.get.GetResponse response _]
  (let [data (if (.exists response)
               {:_index (.getIndex response)
                :_type (.getType response)
                :_id (.getId response)
                :_version (.getVersion response)})
        data (if-not (.isSourceEmpty response)
               (assoc data :_source (convert-source-result (.sourceAsMap response)))
               data)
        data (let [fields (.getFields response)]
               (if-not (empty? fields)
                 (assoc data :fields (convert-fields fields))
                 data))]
    data))

(defn get-empty-params
  [class-name]
  (let [klass (Class/forName class-name)
        ^Field empty-params-field (first (filter (fn [^Field m]
                                            (= (.getName m) "EMPTY_PARAMS"))
                                          (.getFields klass)))
        empty-params (.get empty-params-field klass)]
    empty-params))

(defn- convert-xcontent
  [^org.elasticsearch.common.xcontent.ToXContent response empty-params]
  (let [os (FastByteArrayOutputStream.)
        builder (if (= format :json)
                  (XContentFactory/jsonBuilder os)
                  (XContentFactory/smileBuilder os))]
    (.startObject builder)
    (.toXContent response builder empty-params)
    (.endObject builder)
    (.flush builder)
    (compatible-decode-smile os)))

(defn- make-xconverter
  [class-name]
  (if-let [klass (gav/class-for-name class-name {:throw? false})]
    (let [empty (get-empty-params class-name)]
      (fn convert
        [_ response _] (convert-xcontent response empty)))))

(def translator
  (let [translator
        (-> (gav/make-translator)
            (gav/register-converters
             {:lazy? false :exclude [:class]}
             [["org.elasticsearch.cluster.ClusterName"
               :add {:value (fn [^org.elasticsearch.cluster.ClusterName cluster-name]
                              (.value cluster-name))}]
              ["org.elasticsearch.cluster.ClusterState"]
              ["org.elasticsearch.cluster.metadata.MetaData" :translate-seqs? true]
              ["org.elasticsearch.cluster.metadata.AliasMetaData"]
              ["org.elasticsearch.cluster.metadata.IndexMetaData"]
              ["org.elasticsearch.cluster.metadata.MappingMetaData"]
              ["org.elasticsearch.cluster.node.DiscoveryNode"]
              ["org.elasticsearch.common.compress.CompressedString"
               :add {:string (fn [^org.elasticsearch.common.compress.CompressedString s]
                               (.string s))}]
              ["org.elasticsearch.cluster.node.DiscoveryNodes"]
              ["org.elasticsearch.common.settings.ImmutableSettings"]])
            (gav/register-converters
             {:lazy? false :exclude [:class] :translate-seqs? true}
             [["org.elasticsearch.action.count.CountResponse"]
              ["org.elasticsearch.action.delete.DeleteResponse"]
              ["org.elasticsearch.action.deletebyquery.DeleteByQueryResponse"]
              ["org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateResponse"]
              ["org.elasticsearch.action.index.IndexResponse"]
              ["org.elasticsearch.action.percolate.PercolateResponse"]
              ["org.elasticsearch.action.admin.indices.optimize.OptimizeResponse"]
              ["org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheResponse"]
              ["org.elasticsearch.action.admin.indices.create.CreateIndexResponse"]
              ["org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse"]
              ["org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingResponse"]
              ["org.elasticsearch.action.admin.indices.flush.FlushResponse"]
              ["org.elasticsearch.action.admin.indices.gateway.snapshot.GatewaySnapshotResponse"]
              ["org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse"]
              ["org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse"]
              ["org.elasticsearch.action.admin.indices.refresh.RefreshResponse"]
              ["org.elasticsearch.action.admin.indices.settings.UpdateSettingsResponse"]
              ["org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse"]
              ["org.elasticsearch.action.admin.cluster.state.ClusterStateResponse"]
              ["org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse"]
              ["org.elasticsearch.action.admin.cluster.node.restart.NodesRestartResponse"]
              ["org.elasticsearch.action.admin.cluster.node.shutdown.NodesShutdownResponse"]
              ["org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse"]
              ["org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse"]
              ["org.elasticsearch.action.update.UpdateResponse" :throw? false]
              ;; for es < 0.20
              ["org.elasticsearch.action.admin.indices.exists.IndicesExistsResponse"
               :throw? false]
              ;; for es > 0.20
              ["org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse"
               :throw? false]])
            (gav/add-converter org.elasticsearch.action.get.GetResponse
                               convert-get {:throw? false}))]
    (reduce
     (fn [tr class-name]
       (if-let [xconverter (make-xconverter class-name)]
         (gav/add-converter tr class-name xconverter {:throw? false})
         tr))
     translator ["org.elasticsearch.action.search.SearchResponse"
                 "org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse"
                 "org.elasticsearch.action.admin.indices.status.IndicesStatusResponse"])))

(def convert* (partial gav/translate translator))

(defn convert
  ([response format]
     (if (= format :java)
       response
       (convert* response {})))
  ([response]
     (convert* response {})))

(defn- method->arg
  [^Method method]
  (let [name (.getName method)
        parameter (first (seq (.getParameterTypes method)))
        conv (str/replace name #"^set|^get|^is" "")
        conv (str/lower-case (str/replace conv #"(\p{Lower})(\p{Upper})" "$1-$2"))
        added (if (and parameter (= parameter java.lang.Boolean/TYPE)) (str conv "?") conv)]
    added))

(defn class-for-name
  [class-name]
  (try
    (Class/forName class-name)
    (catch ClassNotFoundException _
      nil)))

(defn make-client
  "creates a client of given type (:node or :transport) and spec"
  [type spec]
  (case type
    :node (.client (make-node spec))
    :transport (make-transport-client spec)
    (make-transport-client spec)))

(defmacro with-node-client
  "opens a node client with given spec and executes the body before closing it"
  [server-spec & body]
  `(with-open [node# (make-node ~server-spec)]
    (binding [clj-elasticsearch.client/*client* (.client node#)]
      (do
        ~@body))))

(defmacro with-transport-client
  "opens a transport client with given spec and executes the body before closing it"
  [server-spec & body]
  `(with-open [client# (make-client :transport ~server-spec)]
    (binding [clj-elasticsearch.client/*client* client#]
      (do
        ~@body))))

(defn build-document
  "returns a string representation of a document suitable for indexing"
  [doc]
  (json/encode-smile doc))

(defn- get-index-admin-client
  ^IndicesAdminClient
  [^Client client]
  (-> client (.admin) (.indices)))

(defn- get-cluster-admin-client
  ^ClusterAdminClient
  [^Client client]
  (-> client (.admin) (.cluster)))

(defn- is-settable-method?
  [^Class klass ^Method method]
  (let [return (.getReturnType method)
        super (.getSuperclass klass)
        allowed #{klass super}
        parameters (.getParameterTypes method)
        nb-params (alength parameters)]
    (and (allowed return) (= nb-params 1))))

(defn- get-executable-methods
  [^Class klass methods]
  (reduce
   (fn [acc ^Method m]
     (let [return (.getReturnType m)
           parameters (into #{} (seq (.getParameterTypes m)))
           nb-params (count parameters)]
       (if (contains? parameters klass)
         (cond
          (= nb-params 1) (assoc acc :sync m)
          (= nb-params 2) (assoc acc :async m)
          :else acc)
         acc)))
   {} methods))

(defn- get-settable-methods
  [^Class klass]
  (let [methods (.getMethods klass)
        settable (filter #(is-settable-method? klass %) (seq methods))
        by-name (group-by (fn [^Method m] (.getName m)) settable)]
    (for [[n ms] by-name]
      (if (= 1 (count ms))
        (first ms)
        (or
         (first
          (filter
           (fn [^Method m]
             (= (first (.getParameterTypes m)) (type (byte-array 0))))
           ms))
         (first ms))))))

(defn- make-executor
  [^Class request-class ^Class client-class]
  (let [methods (.getMethods client-class)
        {:keys [^Method sync ^Method async]} (get-executable-methods request-class methods)]
    (fn
      ([client request]
         (.invoke sync client (into-array Object (list request))))
      ([client request listener]
         (.invoke async client (into-array Object (list request listener)))))))

(defn make-mappers
  [types]
  (for [^Class t types]
    (if (.isArray t)
      (let [k (.getComponentType t)]
        (fn [args] (into-array k args)))
      identity)))

(defn apply-mappers
  [fns vs]
  (map (fn [f v] (f v)) fns vs))

(defn- extract-source-val
  [coll k default]
  (let [val (get coll k default)]
    (if (= val default)
      val
      (convert-source val))))

(def search-type-map
  {:count SearchType/COUNT
   :dfs-query-and-fetch SearchType/DFS_QUERY_AND_FETCH
   :dfs-query-then-fetch SearchType/DFS_QUERY_THEN_FETCH
   :query-and-fetch SearchType/QUERY_AND_FETCH
   :query-then-fetch SearchType/QUERY_THEN_FETCH
   :scan SearchType/SCAN})

(defn extract-search-type
  [coll k default]
  (if (keyword? k)
    (get search-type-map (get coll k) default)
    (get coll k default)))

(defn- request-signature
  [^Class klass]
  (let [methods (get-settable-methods klass)
        fns (for [^Method method methods]
              (let [m-name (-> method (method->arg) (keyword))
                    types (.getParameterTypes method)
                    mapper (first (make-mappers types))
                    extract-arg (cond
                                 (#{:extra-source :source} m-name)
                                 extract-source-val
                                 (= :search-type m-name)
                                 extract-search-type
                                 :else get)]
                [m-name
                 (fn [obj args]
                   (let [arg (extract-arg args m-name ::not-found)]
                     (when-not (= arg ::not-found)
                       (.invoke method obj (into-array Object
                                                       (list (mapper arg)))))))]))]
    (into {} fns)))

(defn select-vals
  [h ks]
  (for [k ks]
    (get h k)))

(defn make-constructor
  [^Class klass names]
  (let [nb-args (count names)
        constructors (.getConstructors klass)
        [^Constructor const types]
        (loop [todo constructors]
          (if-let [^Class c (first todo)]
            (let [ptypes (.getParameterTypes c)]
              (if (= nb-args (count ptypes))
                [c ptypes]
                (recur (rest todo))))
            (throw
             (ex-info
              (format "invalid constructor signature for %s => %s" klass names)
              {:names names :class klass}))))
        mappers (make-mappers types)]
    (fn [args]
      (.newInstance const
                    (into-array Object
                                (apply-mappers mappers (map #(get args %) names)))))))

(def client-types
  {:client org.elasticsearch.client.internal.InternalClient
   :indices org.elasticsearch.client.IndicesAdminClient
   :index org.elasticsearch.client.IndicesAdminClient
   :cluster org.elasticsearch.client.ClusterAdminClient})

(defn- get-response-class-fields
  [class-name]
  (let [response-name (str/replace class-name #"Request" "Response")]
    (if-let [response-class (gav/class-for-name response-name false)]
      (gav/get-class-fields translator response-class))))

(defn make-requester
  [request-class-name cst-args client-type]
  (if-let [r-klass (class-for-name request-class-name)]
    (if-let [c-klass (get client-types client-type)]
      (let [sig (request-signature r-klass)
            req-args (keys sig)
            setters (vals sig)
            get-client-fn (case client-type
                            :client  identity
                            :indices get-index-admin-client
                            :index   get-index-admin-client
                            :cluster get-cluster-admin-client
                            identity)
            exec (make-executor r-klass c-klass)
            args (remove (into #{} cst-args) req-args)
            arglists [['options]
                      ['client {:keys (into [] (map #(-> % name symbol)
                                                    (conj req-args "listener" "format")))
                                :as 'options}]]
            response-fields (get-response-class-fields request-class-name)
            fn-doc (format
                    "Required constructor args: %s. Generated from class %s"
                    (pr-str cst-args) request-class-name)
            fn-doc (if-not (empty? response-fields)
                     (str fn-doc ". Response fields expected " (pr-str response-fields))
                     fn-doc)
            cst (make-constructor r-klass cst-args)]
        (vary-meta
         (fn make-request
           ([client {:keys [debug listener] :as options}]
              (let [c (get-client-fn client)
                    request (cst options)]
                (doseq [m setters]
                  (m request options))
                (cond
                 debug request
                 listener (exec c request listener)
                 :else (let [^ActionFuture ft (exec c request)]
                         (convert (.actionGet ft) (:format options))))))
           ([options] (make-request *client* options)))
         assoc :arglists arglists :doc fn-doc)))))

(defmacro def-requests
  ^{:private true}
  [client-type & request-defs]
  `(do ~@(map (fn [req-def]
                `(if-let [req-fn# (make-requester
                                   ~@(concat (rest req-def) [client-type]))]
                   (let [symb-name# (vary-meta '~(first req-def) merge (meta req-fn#))]
                     (intern 'clj-elasticsearch.client
                             symb-name#
                             req-fn#))))
              request-defs)))

(def-requests :client
  (index-doc "org.elasticsearch.action.index.IndexRequest" [])
  (search "org.elasticsearch.action.search.SearchRequest" [])
  (get-doc "org.elasticsearch.action.get.GetRequest" [:index])
  (count-docs "org.elasticsearch.action.count.CountRequest" [:indices])
  (delete-doc "org.elasticsearch.action.delete.DeleteRequest" [])
  (delete-by-query "org.elasticsearch.action.deletebyquery.DeleteByQueryRequest" [])
  (more-like-this "org.elasticsearch.action.mlt.MoreLikeThisRequest" [:index])
  (percolate "org.elasticsearch.action.percolate.PercolateRequest" [])
  (scroll "org.elasticsearch.action.search.SearchScrollRequest" [:scroll-id]))

(def-requests :indices
  (optimize-index "org.elasticsearch.action.admin.indices.optimize.OptimizeRequest" [])
  (analyze-request "org.elasticsearch.action.admin.indices.analyze.AnalyzeRequest" [:index :text])
  (clear-index-cache "org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheRequest" [:indices])
  (close-index "org.elasticsearch.action.admin.indices.close.CloseIndexRequest" [:index])
  (create-index "org.elasticsearch.action.admin.indices.create.CreateIndexRequest" [:index])
  (delete-index "org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest" [:indices])
  (delete-mapping "org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingRequest" [:indices])
  (delete-template "org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateRequest" [:name])
  ;; for es < 0.20
  (exists-index "org.elasticsearch.action.admin.indices.exists.IndicesExistsRequest" [:indices])
  ;; for es > 0.20
  (exists-index "org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest" [:indices])
  (flush-index "org.elasticsearch.action.admin.indices.flush.FlushRequest" [:indices])
  (gateway-snapshot "org.elasticsearch.action.admin.indices.gateway.snapshot.GatewaySnapshotRequest" [:indices])
  (put-mapping "org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest" [:indices])
  (put-template "org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest" [:name])
  (refresh-index "org.elasticsearch.action.admin.indices.refresh.RefreshRequest" [:indices])
  (index-segments "org.elasticsearch.action.admin.indices.segments.IndicesSegmentsRequest" [])
  (index-stats "org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest" [])
  (index-status "org.elasticsearch.action.admin.indices.status.IndicesStatusRequest" [])
  (update-index-settings "org.elasticsearch.action.admin.indices.settings.UpdateSettingsRequest" [:indices]))

(def-requests :cluster
  (cluster-health "org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest" [:indices])
  (cluster-state "org.elasticsearch.action.admin.cluster.state.ClusterStateRequest" [])
  (node-info "org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest" [])
  (node-restart "org.elasticsearch.action.admin.cluster.node.restart.NodesRestartRequest" [:nodes-ids])
  (node-shutdown "org.elasticsearch.action.admin.cluster.node.shutdown.NodesShutdownRequest" [:nodes-ids])
  (nodes-stats "org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest" [:nodes-ids])
  (update-cluster-settings "org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest" [])
  ;; for es > 0.20
  (update-doc "org.elasticsearch.action.update.UpdateRequest" [:index :type :id]))

(defn make-listener
  "makes a listener suitable as a callback for requests"
  [{:keys [on-failure on-response format]
    :or {format :clj
         on-failure (fn [e]
                      (binding [*out* *err*]
                        (println "Error in listener")
                        (cst/print-cause-trace e)))}}]
  (proxy [ActionListener] []
    (onFailure [e] (on-failure e))
    (onResponse [r] (on-response (convert r format)))))

(defn listener
  "easy to use listener with reasonable defaults"
  [on-response]
  (make-listener {:on-response on-response :format :clj}))

(defn atomic-update-from-source
  "atomically updates a document with an optimistic concurrency control policy.
   The provided function will receive the _source field of the doc as param,
   or nil if the doc id does not exist. If the function returns a map, it will
   become the new value, else nothing will happen. Any additional values you
   want to pass to the requests have to be set into the opts param."
  ([f client opts]
     (let [full-opts (assoc opts :preference "_primary")
           {:keys [_source _version _id]} (get-doc client full-opts)]
       (if _id
         (let [new-source (f _source)
               payload (assoc opts
                         :source new-source :version _version
                         :id _id)
               result (try
                        (index-doc client payload)
                        (catch org.elasticsearch.index.engine.VersionConflictEngineException _
                          nil))]
           (if result
             result
             (recur f client opts)))
         (let [source (f nil)]
           (when (map? source)
             (index-doc client (assoc opts :source source)))))))
  ([f opts] (atomic-update-from-source f *client* opts)))

(ns clj-elasticsearch-native.core
  (:require [cheshire.core :as json]
            [clojure.string :as str]
            [gavagai.core :as gav]
            [clojure.stacktrace :as cst]
            [clj-elasticsearch.specs :as specs])
  (:import [org.elasticsearch.node NodeBuilder]
           [org.elasticsearch.common.xcontent XContentFactory ToXContent$Params]
           [org.elasticsearch.common.settings ImmutableSettings ImmutableSettings$Builder]
           [org.elasticsearch.action.admin.indices.status IndicesStatusRequest]
           [org.elasticsearch.action ActionFuture]
           [org.elasticsearch.common.io FastByteArrayOutputStream]
           [org.elasticsearch.client.transport TransportClient]
           [org.elasticsearch.client.support AbstractClient]
           [org.elasticsearch.node Node]
           [org.elasticsearch.common.unit TimeValue]
           [org.elasticsearch.client Client ClusterAdminClient IndicesAdminClient]
           [org.elasticsearch.common.transport InetSocketTransportAddress]
           [org.elasticsearch.action  ActionListener]
           [org.elasticsearch.common.xcontent ToXContent]
           [org.elasticsearch.search Scroll]
           [org.elasticsearch Version]
           [org.elasticsearch.action.search SearchType]
           [java.lang.reflect Method Field Constructor])
  (:use [clojure.pprint :only [cl-format]]))

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
  "makes a new native elasticsearch node"
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

(defn- make-transport-client
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

(defn- make-compatible-decode-smile
  "produces a fn for backward compatibility with old es version"
  []
  (let [new-met (try
                  (.getMethod FastByteArrayOutputStream "bytes" (make-array Class 0))
                  (catch NoSuchMethodException _ nil))]
    (if new-met
      (fn [^FastByteArrayOutputStream os] (json/decode-smile (.. os bytes toBytes) specs/parse-json-key))
      (fn [^FastByteArrayOutputStream os] (json/decode-smile (.underlyingBytes os) specs/parse-json-key)))))

(def compatible-decode-smile (make-compatible-decode-smile))

(defn- convert-source-result
  [src]
  (cond
   (instance? java.util.HashMap src) (into {}
                                           (map (fn [^java.util.Map$Entry e]
                                                  [(keyword (.getKey e))
                                                   (convert-source-result (.getValue e))]) src))
   (instance? java.util.ArrayList src) (into [] (map convert-source-result src))
   :else src))

(defn- convert-fields
  [^java.util.HashMap hm]
  (into {} (map (fn [^org.elasticsearch.index.get.GetField f]
                  [(keyword (.getName f)) (convert-source-result (.getValue f))]) (.values hm))))

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

(defn kw->enum
  [kw]
  (-> (name)
      (str/upper-case)
      (str/replace #"-" "_")))

(defn enum->kw
  [enum]
  (-> enum
   (str)
   (str/lower-case)))

(defn make-enum-tables*
  [^Class t]
  (let [^Method values-m (first (filter
                                 (fn [^Method m] (= "values" (.getName m)))
                                 (.getMethods t)))
        values (.invoke values-m nil (object-array 0))]
    [(into {} (map (fn [enum] [(enum->kw enum) enum]) values))
     (into {} (map (fn [enum] [enum (enum->kw enum)]) values))]))

(def make-enum-tables
  (memoize make-enum-tables*))

(defn str-conv
  [_ o _]
  (str o))

(def translator
  (let [translator
        (-> (gav/make-translator true)
            (gav/register-converters
             {:lazy? false :exclude [:class]}
             [["org.elasticsearch.cluster.ClusterState" :exclude [:allocation-explanation :blocks]]
              ["org.elasticsearch.cluster.metadata.MetaData" :translate-seqs? true]
              ["org.elasticsearch.cluster.metadata.AliasMetaData"]
              ["org.elasticsearch.cluster.metadata.IndexMetaData"]
              ["org.elasticsearch.cluster.node.DiscoveryNode"]
              ["org.elasticsearch.cluster.node.DiscoveryNodes"]
              ["org.elasticsearch.action.admin.cluster.health.ClusterIndexHealth"]
              ["org.elasticsearch.action.admin.cluster.health.ClusterShardHealth"]
              ["org.elasticsearch.cluster.routing.ImmutableShardRouting"
               :force ["primary" "initializing" "shortSummary" "relocating" "relocatingNodeId"
                       "started" "state" "unassigned" "version" "started"]]
              ["org.elasticsearch.cluster.routing.MutableShardRouting"
               :force ["primary" "initializing" "shortSummary" "relocating" "relocatingNodeId"
                       "started" "state" "unassigned" "version" "started"]]
              ["org.elasticsearch.cluster.routing.IndexShardRoutingTable"]
              ["org.elasticsearch.index.shard.ShardId"]
              ["org.elasticsearch.action.admin.cluster.node.info.NodeInfo"]])
            (gav/register-converters
             {:lazy? false :exclude [:class]
              :translate-seqs? true}
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
              ["org.elasticsearch.cluster.routing.RoutingTable"]
              ["org.elasticsearch.cluster.routing.RoutingNodes" :exclude [:blocks]]
              ["org.elasticsearch.cluster.routing.IndexRoutingTable"]
              ["org.elasticsearch.action.admin.cluster.node.stats.NodeStats"]
              ;; for es < 0.20
              ["org.elasticsearch.action.admin.indices.exists.IndicesExistsResponse"
               :throw? false]
              ;; for es > 0.20
              ["org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse"
               :throw? false]
              ;; custom converters
              ["org.elasticsearch.cluster.routing.RoutingNode"
               :force ["nodeId" "numberOfOwningShards" "shards"]]
              ["org.elasticsearch.action.get.GetResponse" :custom-converter convert-get :throw? false]
              ["org.elasticsearch.common.transport.LocalTransportAddress"
               :custom-converter str-conv]
              ["org.elasticsearch.common.transport.InetSocketTransportAddress"
               :custom-converter str-conv]
              ["org.elasticsearch.Version" :custom-converter str-conv]
              ["org.elasticsearch.cluster.ClusterName"
               :custom-converter (fn [_ ^org.elasticsearch.cluster.ClusterName cluster-name _]
                                   (.value cluster-name))]
              ["org.elasticsearch.common.compress.CompressedString"
               :custom-converter (fn [_ ^org.elasticsearch.common.compress.CompressedString s _]
                                   (.string s))]
              ["org.elasticsearch.common.settings.ImmutableSettings"
               :custom-converter (fn [_ ^org.elasticsearch.common.settings.ImmutableSettings o _]
                                   (into {} (.getAsMap o)))]
              ["org.elasticsearch.cluster.metadata.MappingMetaData"
               :custom-converter (fn [tr ^org.elasticsearch.cluster.metadata.MappingMetaData o opts]
                                   (convert-source-result (.getSourceAsMap o)))]]))
        ;; handle enums
        translator (reduce
                    (fn [t klass]
                      (let [[_ table]
                            (make-enum-tables klass)]
                        (gav/add-converter
                         t
                         klass
                         (fn [_ enum _] (get table enum enum)))))
                    translator
                    [org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus
                     org.elasticsearch.cluster.metadata.IndexMetaData$State
                     org.elasticsearch.cluster.routing.ShardRoutingState])]
    ;; handle xcontent
    (reduce
     (fn [tr class-name]
       (if-let [xconverter (make-xconverter class-name)]
         (gav/add-converter tr class-name xconverter {:throw? false})
         tr))
     translator ["org.elasticsearch.action.search.SearchResponse"
                 "org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse"
                 "org.elasticsearch.action.admin.indices.status.IndicesStatusResponse"
                 "org.elasticsearch.action.admin.indices.segments.IndicesSegmentResponse"
                 "org.elasticsearch.action.admin.indices.status.IndicesStatusResponse"
                 "org.elasticsearch.action.admin.indices.stats.CommonStats"
                 "org.elasticsearch.action.admin.indices.stats.IndicesStats"
                 "org.elasticsearch.indices.NodeIndicesStats"
                 "org.elasticsearch.common.xcontent.ToXContent"])))

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
        conv (if (and (not (#{"get" "set" "is"} name))
                      (re-find #"^(get|set|is)[A-Z]" name))
               (str/replace name #"^set|^get|^is" "")
               name)
        conv (str/lower-case (str/replace conv #"(\p{Lower})(\p{Upper})" "$1-$2"))
        added (if (and parameter
                       (or (= java.lang.Boolean parameter)
                           (= parameter java.lang.Boolean/TYPE)))
                (str conv "?") conv)]
    added))

(defprotocol ComposableListener
  (compose [this listener] "composes with the given listener, returning a new listener")
  (getFailureHandler [this])
  (getResponseHandler [this])
  (getFormat [this]))

(defmethod specs/make-listener :native
  [type
   {:keys [on-failure on-response format]
    :or {format :clj
         on-failure (fn [e]
                      (binding [*out* *err*]
                        (println "Error in listener")
                        (cst/print-cause-trace e)))}}]
  (reify
    ActionListener
    (onFailure [this e] (on-failure e))
    (onResponse [this r] (on-response (convert r format)))
    ComposableListener
    (getFailureHandler [this] on-failure)
    (getResponseHandler [this] on-response)
    (getFormat [this] format)
    (compose [this listener]
      (specs/make-listener
       type
       {:format :clj
        :on-failure (getFailureHandler this)
        :on-response (comp
                      (getResponseHandler this)
                      (getResponseHandler listener))}))))

(defn make-listener [arg] (specs/make-listener :node arg))

(defn make-es-future
  [^ActionFuture action-future {:keys [format async-callback]
                                :or {async-callback identity
                                     format :clj}}]
  (reify
    clojure.lang.IDeref
    (deref [this]
      (async-callback (convert (.actionGet action-future) format)))
    clojure.lang.IBlockingDeref
    (deref [this timeout-ms timeout-val]
      (try
        (async-callback (convert (.actionGet action-future timeout-ms) format))
        (catch org.elasticsearch.ElasticSearchInterruptedException _
          timeout-val)))
    clojure.lang.IPending
    (isRealized [this] (.isDone action-future))
    java.util.concurrent.Future
    (cancel [this int-if-running?] (.cancel action-future int-if-running?))
    (get [this] (async-callback (convert (.get action-future) format)))
    (get [this tv tu] (async-callback (convert (.get action-future tv tu) format)))
    (isCancelled [this] (.isCancelled action-future))
    (isDone [this] (.isDone action-future))
    ActionFuture
    (actionGet [this] (.actionGet action-future))
    (actionGet [this ^long ms] (.actionGet action-future ms))
    (actionGet [this ^long ms ^java.util.concurrent.TimeUnit tu]
      (.actionGet action-future ms) tu)
    (actionGet [this ^String to] (.actionGet action-future to))
    (actionGet [this ^org.elasticsearch.common.unit.TimeValue tv]
      (.actionGet action-future tv))
    (getRootFailure [this] (.getRootFailure action-future))))

(defn class-for-name
  [class-name]
  (try
    (Class/forName class-name)
    (catch ClassNotFoundException _
      nil)))

(defprotocol PClosable
  (close [this] "closes this Elasticsearch client and any underlying infrastructure"))

(defprotocol PGetClient
  (getClient ^Client [this] "returns a Elasticsearch Client instance"))

(extend-protocol PGetClient
  org.elasticsearch.node.internal.InternalNode
  (getClient [this] (.client this))
  TransportClient
  (getClient [this] this)
  org.elasticsearch.client.node.NodeClient
  (getClient [this] this))

(defrecord ESNodeClient [^org.elasticsearch.node.Node node
                         ^org.elasticsearch.client.Client client]
  PClosable
  (close [this] (do (.close client)
                    (.close node)))
  PGetClient
  (getClient [this] client))

(defmacro with-client
  "uses an existing client in the body, does not close it afterward"
  [client & body]
  `(binding [clj-elasticsearch-native.core/*client* ~client]
     (do ~@body)))

(defn build-document
  "returns a string representation of a document suitable for indexing"
  [doc]
  (json/encode-smile doc))

(defn- get-index-admin-client
  ^IndicesAdminClient
  [^Client client]
  (-> client (getClient) (.admin) (.indices)))

(defn- get-cluster-admin-client
  ^ClusterAdminClient
  [^Client client]
  (-> client (getClient) (.admin) (.cluster)))

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

(defn- clean-class-name
  [^Class klass]
  (let [n (.getName klass)
        last-part (last (str/split n #"\."))]
    last-part))

(defn- make-enum-doc-string
  [table]
  (let [enums (keys table)]
    (cl-format false "one of ~{~S~^, ~}~^ or ~S" (butlast enums) (last enums))))

(defn- make-settable
  [method]
  (for [^Class klass (.getParameterTypes method)]
    (cond
     (= String klass)
     {:method method :mapper name :doc "String" :rest-type :string :rest-doc "String"}
     (.isArray klass)
     (let [k (.getComponentType klass)
           mapper (if (= k String)
                    (fn [args] (into-array k (map name args)))
                    (fn [args] (into-array k args)))
           doc (str "seq of " (clean-class-name k) "s")]
       {:method method :mapper mapper :doc doc
        :rest-type :string-or-list-of-strings :rest-doc "String or seq of Strings"})
     (.isEnum klass)
     (let [[table _] (make-enum-tables klass)
           mapper (fn [arg]
                    (get table arg arg))
           doc (make-enum-doc-string table)]
       {:method method :mapper mapper :doc doc
        :rest-type (mapv keyword (keys table)) :rest-doc doc})
     (= org.elasticsearch.search.Scroll klass)
     (let [mapper (fn [arg]
                    (Scroll. (TimeValue/parseTimeValue arg nil)))
           doc "String time spec (ex: \"5s\")"]
       {:method method :mapper mapper :doc doc
        :rest-type :time-string :rest-doc doc})
     (= "boolean" (str klass))
     {:method method :mapper identity :doc (clean-class-name klass)
      :rest-type :boolean :rest-doc "boolean"}
     :else
     {:method method :mapper identity :doc (clean-class-name klass)
      :rest-type :string :rest-doc "String"})))

(defn- get-settable-methods
  [^Class klass]
  (let [methods (.getMethods klass)
        settable (filter #(is-settable-method? klass %) (seq methods))
        by-name (group-by (fn [^Method m] (.getName m)) settable)]
    (for [[n ms] by-name]
      (if (= 1 (count ms))
        (let [method (first ms)]
          (first (make-settable method)))
        (let [sigs (into {} (map (fn [^Method m]
                                   [(first (.getParameterTypes m)) m]) ms))
              types (into #{} (keys sigs))
              bytes-array-class (type (byte-array 0))]
          (cond
           ;; only one left
           (= 1 (count types))
           (first (make-settable (first (vals sigs))))
           ;; CreateIndexRequest
           (contains? types org.elasticsearch.common.settings.Settings$Builder)
           (let [met (get sigs org.elasticsearch.common.settings.Settings$Builder)
                 mapper (fn [arg]
                          (update-settings-builder arg))
                 doc "HashMap of Settings"
                 rest-type :hash-map-or-json
                 rest-doc "Hash-map or JSON string"]
             {:method met :mapper mapper :doc doc :rest-type rest-type :rest-doc rest-doc})
           ;; mapping fields
           (and
            (contains? types org.elasticsearch.common.xcontent.XContentBuilder)
            (not (contains? types bytes-array-class)))
           (let [met (get sigs String)
                 doc "HashMap or JSON String"
                 mapper (fn [arg]
                          (if (string? arg)
                            arg
                            (json/encode arg)))
                 rest-type :hash-map-or-json
                 rest-doc "Hash-map or JSON string"]
             {:method met :mapper mapper :doc doc :rest-type rest-type :rest-doc rest-doc})
           ;; source fields
           (contains? types org.elasticsearch.common.xcontent.XContentBuilder)
           (let [met (get sigs bytes-array-class)
                 doc "HashMap or JSON String or SMILE bytes-array"
                 mapper (fn [arg]
                          (convert-source arg))
                 rest-type :hash-map-or-json
                 rest-doc "Hash-map or JSON string"]
             {:method met :mapper mapper :doc doc :rest-type rest-type :rest-doc rest-doc})
           ;; timeout field
           (contains? types org.elasticsearch.common.unit.TimeValue)
           (let [met (get sigs String)
                 mapper identity
                 doc "String time spec (ex: \"5s\")"
                 rest-type :time-string
                 rest-doc doc]
             {:method met :mapper mapper :doc doc :rest-type rest-type :rest-doc rest-doc})
           ;; enums
           (some (fn [^Class t] (.isEnum t)) (keys sigs))
           (let [t (first (filter (fn [^Class t] (.isEnum t)) (keys sigs)))
                 [table _] (make-enum-tables t)
                 met (get sigs t)
                 mapper (fn [arg]
                          (get table arg arg))
                 doc (make-enum-doc-string table)
                 rest-type (mapv keyword (keys table))
                 rest-doc doc]
             {:method met :mapper mapper :doc doc :rest-type rest-type :rest-doc rest-doc})
           ;; routing field
           (and (contains? sigs String) (contains? sigs (type (make-array String 0))))
           (let [met (get sigs (type (make-array String 0)))
                 mapper (fn [arg]
                          (if (string? arg)
                            [arg]
                            (into-array String arg)))
                 doc "String or seq of Strings"
                 rest-type :string-or-list-of-strings
                 rest-doc doc]
             {:method met :mapper mapper :doc doc :rest-type rest-type :rest-doc rest-doc})
           :else
           (throw
            (ex-info
             (format "Method signature problem with method %s from class %s" n klass)
             {:class klass :method n :types types :count (count ms) :all-methods ms}))))))))

(defn- make-executor
  [^Class request-class ^Class client-class]
  (let [methods (.getMethods client-class)
        {:keys [^Method sync ^Method async]} (get-executable-methods request-class methods)]
    (fn
      ([client request]
         (.invoke sync client (into-array Object (list request))))
      ([client request listener]
         (.invoke async client (into-array Object (list request listener)))))))

(defn- request-signature
  [^Class klass]
  (let [methods (get-settable-methods klass)
        fns (for [{:keys [^Method method mapper doc]} methods]
              (let [m-name (-> method (method->arg) (keyword))]
                [m-name
                 (vary-meta
                  (fn [obj args]
                    (let [arg (get args m-name ::not-found)]
                      (when-not (= arg ::not-found)
                        (.invoke method obj (into-array Object
                                                        (list (mapper arg)))))))
                  merge {::type-doc doc})]))]
    (into {} fns)))


(defn request-rest-signature
  [^Class klass]
  (let [methods (get-settable-methods klass)
        fns (for [{:keys [^Method method rest-type rest-doc]} methods]
              (let [m-name (-> method (method->arg) (keyword))]
                [m-name [rest-type rest-doc]]))]
    (into {} fns)))

(defn select-vals
  [h ks]
  (for [k ks]
    (get h k)))

(defn apply-mappers
  [fns vs]
  (map (fn [f v] (f v)) fns vs))

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
        mappers (map :mapper (make-settable const))]
    (fn [args]
      (.newInstance const
                    (into-array Object
                                (apply-mappers mappers
                                               (map #(get args %) names)))))))

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

(defn- format-params-doc
  [methods]
  (let [ordered-mets (concat (sort (:required methods))
                             (sort (:optional methods)))
        max-length (inc (apply max
                               (mapcat (fn [mets]
                                         (map #(-> % (first) (str) (count)) mets))
                                       (vals methods))))]
    (cl-format false "~2TParams keys:~%~:{~4T~vA=> ~A~%~}"
               (map #(concat [max-length] %) ordered-mets))))

(defn- format-params-doc-from-sig
  [sig cst-args]
  (let [required (into #{} cst-args)
        mets (reduce (fn [acc [m s]]
                       (let [type-doc (::type-doc (meta s))]
                         (if (required m)
                           (update-in acc [:required] conj [m (str "(required) " type-doc)])
                           (update-in acc [:optional] conj [m type-doc]))))
                     {:required [] :optional []} sig)
        mets (update-in mets [:optional] conj
                             [:listener "takes a listener object and makes the call async"]
                             [:format "one of :clj, :json or :java"]
                             [:async? "if set to true, the call returns a Future instead of blocking"])]
    (format-params-doc mets)))

(defn make-requester
  [request-class-name {client-type :impl cst-args :constructor req-args :required}]
  (if-let [r-klass (class-for-name request-class-name)]
    (if-let [c-klass (get client-types client-type)]
      (let [sig (request-signature r-klass)
            all-args (keys sig)
            setters (vals sig)
            get-client-fn (case client-type
                            :client  getClient
                            :indices get-index-admin-client
                            :index   get-index-admin-client
                            :cluster get-cluster-admin-client
                            identity)
            exec (make-executor r-klass c-klass)
            arglists [[{:keys (into []
                                    (sort
                                     (map #(-> % name symbol)
                                          (conj all-args "listener" "format" "async?"))))
                        :as 'params}]
                      ['client 'params]]
            response-fields (get-response-class-fields request-class-name)
            params-doc (format-params-doc-from-sig sig (concat cst-args req-args))
            fn-doc (format
                    "Generated from Class %s\n%s"
                    request-class-name params-doc)
            fn-doc (if-not (empty? response-fields)
                     (str fn-doc "Response fields expected " (pr-str response-fields))
                     fn-doc)
            cst (make-constructor r-klass cst-args)]
        (vary-meta
         (fn make-request
           ([client {:keys [debug listener async?] :as options}]
              (let [c (get-client-fn client)
                    request (cst options)]
                (doseq [m setters]
                  (m request options))
                (cond
                 debug request
                 listener (exec c request listener)
                 async? (let [ft (exec c request)]
                          (make-es-future ft options))
                 :else (let [^ActionFuture ft (exec c request)]
                         (convert (.actionGet ft) (:format options))))))
           ([options] (make-request *client* options)))
         assoc :arglists arglists :doc fn-doc)))))

(defn make-requester-wrapper
  [requester & {:keys [on-params on-response argslist fn-doc]
                :or {on-params identity on-response identity
                     argslist [['client 'params]
                               ['params]]
                     fn-doc "Generated based on another requester."}}]
  (vary-meta
   (fn make-request
     ([client options]
        (let [{:keys [async? listener] :as opts} (on-params options)
              [comp-opts wrapper]
              (cond
               listener (let [base-listener
                              (specs/make-listener :native
                                                   [:on-response (fn [r] (on-response opts r))])]
                          [(assoc opts :listener (compose listener base-listener))
                           (fn [_ obj] obj)])
               async? [(update-in opts [:async-callback]
                                  (fn [old cb] (if old (comp cb old) cb))
                                  (partial on-response opts))
                       (fn [_ obj] obj)]
               :else [opts on-response])]
          (wrapper opts (requester client comp-opts))))
     ([options] (make-request *client* options)))
   assoc :argslist argslist :doc fn-doc))

(defn make-implementation!
  [specs]
  (reduce (fn [acc [class-name {:keys [symb impl constructor required] :as spec}]]
            (if-let [req-fn (make-requester class-name spec)]
              (let [name-kw (-> symb (name) (keyword))
                    symb-name (vary-meta symb merge (meta req-fn))]
                (intern 'clj-elasticsearch-native.core symb-name req-fn)
                (assoc acc name-kw req-fn))
              acc))
          {} specs))

(defn make-rest-fns-blueprint
  [specs]
  (reduce (fn [acc [class-name {:keys [symb constructor required
                                       rest-uri rest-method rest-default] :as spec}]]
            (if-let [klass (class-for-name class-name)]
              (let [signature (assoc
                               (select-keys spec [:rest-uri :rest-method :rest-default])
                               :params
                               (request-rest-signature klass))
                    clean-sig (->> signature
                                   (remove (fn [[k v]] (#{:listener-threaded? :format} v)))
                                   (into {}))]
                (assoc acc (-> symb (name) (keyword)) clean-sig))
              acc))
          {} specs))

(defonce implementation (make-implementation! specs/global-specs))

(extend-protocol specs/PCallAPI
  org.elasticsearch.node.internal.InternalNode
  (specs/make-api-call [this method-name options]
    (specs/make-api-call* implementation this method-name options))
  TransportClient
  (specs/make-api-call [this method-name options]
    (specs/make-api-call* implementation this method-name options))
  org.elasticsearch.client.node.NodeClient
  (specs/make-api-call [this method-name options]
    (specs/make-api-call* implementation this method-name options))
  ESNodeClient
  (specs/make-api-call [this method-name options]
    (specs/make-api-call* implementation this method-name options)))

(def base-wrapped-params-doc
  {:optional
   [[:listener "takes a listener object and makes the call async"]
    [:async? "if set to true, the call returns a Future instead of blocking"]]})

(defn make-wrapped-doc
  [msg specs]
  (let [params-str (format-params-doc
                    (update-in base-wrapped-params-doc
                               [:optional] concat specs))
        doc-str (format "%s\n%s" msg params-str)]
    doc-str))

(def
  ^{:doc (make-wrapped-doc
          "Gets mappings from cluster. Wrapper over cluster-state"
          [[:indices "seq of Strings"]
           [:types "seq of Strings"]])
    :arglists [['client 'params] ['params]]}
  get-mapping
  (make-requester-wrapper
   cluster-state
   :on-params (fn [{:keys [indices] :as params}]
                (merge params
                       {:filter-blocks? true
                        :filter-nodes? true
                        :filter-routing-table? true
                        :format :clj}
                       (when indices {:filtered-indices indices})))
   :on-response (fn [{:keys [types]} resp]
                  (let [mappings (get-in resp [:state :meta-data :indices] {})]
                    (into {}
                          (map (fn [[idx state]]
                                 [idx (if-not (empty? types)
                                        (select-keys (:mappings state) state)
                                        (:mappings state))])
                               mappings))))))

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

(defmethod specs/make-client :node
  [_ spec]
  (let [node (make-node spec)
                client (ESNodeClient. node (.client node))]
            client))

(defmethod specs/make-client :transport
  [_ spec]
  (let [client (make-transport-client spec)]
    client))

(defmacro with-node-client
  "opens a node client with given spec and executes the body before closing it"
  [server-spec & body]
  `(with-open [node# (specs/make-client :node ~server-spec)]
     (with-client node#
       (do ~@body))))

(defmacro with-transport-client
  "opens a transport client with given spec and executes the body before closing it"
  [server-spec & body]
  `(with-open [client# (specs/make-client :transport ~server-spec)]
     (with-client client#
       (do ~@body))))

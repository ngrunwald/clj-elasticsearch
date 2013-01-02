(ns clj-elasticsearch.client
  (:require [cheshire.core :as json]
            [clojure.string :as str]
            [gavagai.core :as gav])
  (:import [org.elasticsearch.node NodeBuilder]
           [org.elasticsearch.common.xcontent XContentFactory ToXContent$Params]
           [org.elasticsearch.common.settings ImmutableSettings ImmutableSettings$Builder]
           [org.elasticsearch.action.admin.indices.status IndicesStatusRequest]
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
           [java.lang.reflect Method Field]))

(def ^{:dynamic true} *client*)

(def ^:const local-ns (find-ns 'clj-elasticsearch.client))

(defprotocol FromJava
  "Protocol for conversion of Response Classes to many formats"
  (convert [response format] "convert response to given format. Format can be :json, :java, or :clj"))

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
  "produces a compatible fn for backward compatibility with es prior 0.19.0"
  []
  (if (after-0-19?)
    (fn [^FastByteArrayOutputStream os] (json/decode-smile (.. os bytes toBytes) true))
    (fn [^FastByteArrayOutputStream os] (json/decode-smile (.underlyingBytes os) true))))

(def compatible-decode-smile (make-compatible-decode-smile))

(defn- convert-xcontent
  [^org.elasticsearch.common.xcontent.ToXContent response empty-params & [format]]
  (if (= format :java)
    response
    (let [os (FastByteArrayOutputStream.)
          builder (if (= format :json)
                    (XContentFactory/jsonBuilder os)
                    (XContentFactory/smileBuilder os))]
      (.startObject builder)
      (.toXContent response builder empty-params)
      (.endObject builder)
      (.flush builder)
      (case format
        :json (.toString os "UTF-8")
        (compatible-decode-smile os)))))

(def translator
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
    ["org.elasticsearch.common.settings.ImmutableSettings"]]))

(def translate (partial gav/translate translator))

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

(defmacro def-converter
  ^{:private true}
  [fn-name class-name]
  (if-let [klass (class-for-name class-name)]
    (let [k-symb (symbol class-name)
          methods (.getMethods klass)
          getters-m (filter (fn [^Method m]
                              (let [n (.getName m)]
                                (and (re-find #"^get|^is" n)
                                     (= 0 (count (.getParameterTypes m)))
                                     (not (#{"getClass" "getShardFailures"} n)))))
                            methods)
          iterator? (some #{"iterator"} (map (fn [^Method m] (.getName m)) methods))
          sig (reduce (fn [acc ^Method m]
                        (let [m-name (.getName m)]
                          (assoc acc
                            (keyword (method->arg m))
                            (symbol (str "." m-name)))))
                      {} getters-m)
          response (with-meta (gensym "response") {:tag k-symb})]
      `(defn ~fn-name
         [~response & [format#]]
         (if (= format# :java)
           ~response
           (let [res# (hash-map
                       ~@(let [gets (for [[kw getter] sig]
                                      `(~kw (translate (~getter ~response) {})))
                               gets (if iterator?
                                      (conj gets  `(:iterator (iterator-seq (.iterator ~response))))
                                      gets)]
                           (apply concat gets)))]
             (case format#
               :json (json/generate-string res#)
               res#)))))))

(defn get-empty-params
  [class-name]
  (let [klass (Class/forName class-name)
        ^Field empty-params-field (first (filter (fn [^Field m]
                                            (= (.getName m) "EMPTY_PARAMS"))
                                          (.getFields klass)))
        empty-params (.get empty-params-field klass)]
    empty-params))

(defmacro def-xconverter
  ^{:private true}
  [fn-name class-name]
  (let [klass (Class/forName class-name)
        response (gensym "response")]
    `(let [empty# (get-empty-params ~class-name)]
       (defn ~fn-name
         [~(with-meta response {:tag klass}) & [format#]]
         (convert-xcontent ~response empty# format#)))))

(defmacro def-converters
  ^{:private true}
  [& conv-defs]
  `(do ~@(for [[nam klass typ] conv-defs
               :when (class-for-name klass)]
           `(do (~(if (= typ :xcontent) 'def-xconverter 'def-converter) ~nam ~klass)
                (extend ~(symbol klass)
                  FromJava
                  {:convert (fn [response# format#]
                              (~(symbol (str "clj-elasticsearch.client/" nam))
                               response# format#))})))))

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

(defn- is-execute-method?
  [^Class klass ^Method method]
  (let [return (.getReturnType method)
        parameters (into #{} (seq (.getParameterTypes method)))
        nb-params (count parameters)]
    (and (contains? parameters klass) (= nb-params 1))))

(defn- get-settable-methods
  ^Method
  [class-name]
  (let [klass (Class/forName class-name)
        methods (.getMethods klass)
        settable (filter #(is-settable-method? klass %) (seq methods))]
    settable))

(defn- get-execute-method
  ^Method
  [request-class-name client-class-name]
  (let [c-klass (Class/forName client-class-name)
        r-klass (Class/forName request-class-name)
        methods (.getMethods c-klass)
        executable (first (filter #(is-execute-method? r-klass %) (seq methods)))]
    executable))

(defn- request-signature
  [class-name]
  (let [methods (get-settable-methods class-name)
        args (map method->arg methods)]
    (zipmap (map keyword args)
            methods)))

(defn- acoerce
  [val]
  (if (or (vector? val) (list? val))
    (into-array val)
    val))

(defn select-vals
  [h ks]
  (for [k ks]
    (get h k)))

(defn- extract-source-val
  [coll k]
  (if-let [val (get coll k)]
    (if (map? val) (json/encode-smile val) val)))

(def search-type-map
  {:count SearchType/COUNT
   :dfs-query-and-fetch SearchType/DFS_QUERY_AND_FETCH
   :dfs-query-then-fetch SearchType/DFS_QUERY_THEN_FETCH
   :query-and-fetch SearchType/QUERY_AND_FETCH
   :query-then-fetch SearchType/QUERY_THEN_FETCH
   :scan SearchType/SCAN})

(defn extract-search-type
  [coll k]
  (if (keyword? k)
    (get search-type-map (get coll k))
    (get coll k)))

(defmacro defn-request
  ^{:private true}
  [fn-name request-class-name cst-args client-class-name]
  (if-let [r-klass (class-for-name request-class-name)]
    (let [r-symb (symbol request-class-name)
          c-symb (symbol client-class-name)
          sig (request-signature request-class-name)
          method (get-execute-method request-class-name client-class-name)
          m-name (symbol (str "." (.getName method)))
          args (remove (into #{} cst-args) (keys sig))
          arglists [['options]
                    ['client `{:keys [~@(map #(-> % name symbol)
                                             (conj args "listener" "format"))] :as ~'options}]]
          cst-gensym (take (count cst-args) (repeatedly gensym))
          signature (reduce (fn [acc [k ^Method v]] (assoc acc k (symbol (str "." (.getName v))))) {} sig)
          request (with-meta (gensym "request") {:tag r-symb})
          options (gensym "options")
          client (with-meta (gensym "client") {:tag c-symb})]
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
                  [~@cst-gensym] (map acoerce (select-vals options# [~@cst-args]))
                  ~request (new ~r-klass ~@cst-gensym)
                  ~options (dissoc options# ~@cst-args)]
              ~@(for [[k met] signature
                      :let [extract-val
                            (cond (#{:extra-source :source} k)
                                  extract-source-val
                                  (= :search-type k)
                                  extract-search-type
                                  :else get)]]
                  `(when (contains? ~options ~k)
                     (~met ~request (acoerce (~extract-val ~options ~k)))))
              (cond
               (get ~options :debug) ~request
               (get ~options :listener) (~m-name client# ~request (:listener ~options))
               :else (convert (.actionGet (~m-name client# ~request)) (:format ~options)))))
         ([options#]
            (~fn-name *client* options#))))))

(defmacro def-requests
  ^{:private true}
  [client-class-name & request-defs]
  `(do ~@(map (fn [req-def]
                `(defn-request ~@(concat req-def [client-class-name])))
              request-defs)))

(defn- convert-source
  [src]
  (cond
   (instance? java.util.HashMap src) (into {} (map (fn [^java.util.Map$Entry e] [(.getKey e)
                                                           (convert-source (.getValue e))]) src))
   (instance? java.util.ArrayList src) (into [] (map convert-source src))
   :else src))

(defn- convert-fields
  [^java.util.HashMap hm]
  (into {} (map (fn [^org.elasticsearch.index.get.GetField f]
                  [(.getName f) (convert-source (.getValue f))]) (.values hm))))

(defn- convert-get
  [^org.elasticsearch.action.get.GetResponse response & [format]]
  (if (= format :java)
    response
    (let [data (if (.exists response)
                 {:_index (.getIndex response)
                  :_type (.getType response)
                  :_id (.getId response)
                  :_version (.getVersion response)})
          data (if-not (.isSourceEmpty response)
                 (assoc data :_source (convert-source (.sourceAsMap response)))
                 data)
          data (let [fields (.getFields response)]
                 (if-not (empty? fields)
                   (assoc data :fields (convert-fields fields))
                   data))]
      (if (= format :json)
        (json/generate-string data)
        data))))

(def-converters
  (convert-indices-status "org.elasticsearch.action.admin.indices.status.IndicesStatusResponse" :xcontent)
  (convert-analyze "org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse" :xcontent)
  (convert-search "org.elasticsearch.action.search.SearchResponse" :xcontent)
  (convert-count "org.elasticsearch.action.count.CountResponse" :object)
  (convert-delete "org.elasticsearch.action.delete.DeleteResponse" :object)
  (convert-delete-by-query "org.elasticsearch.action.deletebyquery.DeleteByQueryResponse" :object)
  (convert-delete-template "org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateResponse" :object)
  (convert-index "org.elasticsearch.action.index.IndexResponse" :object)
  (convert-percolate "org.elasticsearch.action.percolate.PercolateResponse" :object)
  (convert-optimize "org.elasticsearch.action.admin.indices.optimize.OptimizeResponse" :object)
  (convert-clear-cache "org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheResponse" :object)
  (convert-create-index "org.elasticsearch.action.admin.indices.create.CreateIndexResponse" :object)
  (convert-delete-index "org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse" :object)
  (convert-delete-mapping "org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingResponse" :object)
  ;; for es < 0.20
  (convert-exists-index "org.elasticsearch.action.admin.indices.exists.IndicesExistsResponse" :object)
  ;; for es > 0.20
  (convert-exists-index "org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse" :object)
  (convert-flush-request "org.elasticsearch.action.admin.indices.flush.FlushResponse" :object)
  (convert-gateway-snapshot "org.elasticsearch.action.admin.indices.gateway.snapshot.GatewaySnapshotResponse" :object)
  (convert-put-mapping "org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse" :object)
  (convert-put-template "org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse" :object)
  (convert-refresh-index "org.elasticsearch.action.admin.indices.refresh.RefreshResponse" :object)
  (convert-update-index-settings "org.elasticsearch.action.admin.indices.settings.UpdateSettingsResponse" :object)
  (convert-cluster-health "org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse" :object)
  (convert-cluster-state "org.elasticsearch.action.admin.cluster.state.ClusterStateResponse" :object)
  (convert-node-info "org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse" :object)
  (convert-node-restart "org.elasticsearch.action.admin.cluster.node.restart.NodesRestartResponse" :object)
  (convert-node-shutdown "org.elasticsearch.action.admin.cluster.node.shutdown.NodesShutdownResponse" :object)
  (convert-node-stats "org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse" :object)
  (convert-update-cluster-settings "org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse" :object))

(extend-type org.elasticsearch.action.get.GetResponse
   FromJava
   (convert [response format] (convert-get response format)))

(def-requests "org.elasticsearch.client.internal.InternalClient"
  (index-doc "org.elasticsearch.action.index.IndexRequest" [])
  (search "org.elasticsearch.action.search.SearchRequest" [])
  (get-doc "org.elasticsearch.action.get.GetRequest" [:index])
  (count-docs "org.elasticsearch.action.count.CountRequest" [:indices])
  (delete-doc "org.elasticsearch.action.delete.DeleteRequest" [])
  (delete-by-query "org.elasticsearch.action.deletebyquery.DeleteByQueryRequest" [])
  (more-like-this "org.elasticsearch.action.mlt.MoreLikeThisRequest" [:index])
  (percolate "org.elasticsearch.action.percolate.PercolateRequest" [])
  (scroll "org.elasticsearch.action.search.SearchScrollRequest" [:scroll-id]))

(def-requests "org.elasticsearch.client.IndicesAdminClient"
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

(def-requests "org.elasticsearch.client.ClusterAdminClient"
  (cluster-health "org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest" [:indices])
  (cluster-state "org.elasticsearch.action.admin.cluster.state.ClusterStateRequest" [])
  (node-info "org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest" [])
  (node-restart "org.elasticsearch.action.admin.cluster.node.restart.NodesRestartRequest" [:nodes-ids])
  (node-shutdown "org.elasticsearch.action.admin.cluster.node.shutdown.NodesShutdownRequest" [:nodes-ids])
  (nodes-stats "org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest" [:nodes-ids])
  (update-cluster-settings "org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest" []))

(defn make-listener
  "makes a listener suitable as a callback for requests"
  [{:keys [on-failure on-response format]
    :or {format :clj
         on-failure (fn [e]
                      ;; TODO plug in es log
                      (println "error in listener:" e))}}]
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

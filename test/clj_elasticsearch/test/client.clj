(ns clj-elasticsearch.test.client
  (:use [clj-elasticsearch.client] :reload-all)
  (:use [clojure.test]
        [clojure.java.io]
        [cheshire.core]))

(defn delete-dir
  [path]
  (let [d (file path)]
    (if (.isDirectory d)
      (do
        (doseq [f (seq (.listFiles d))]
          (delete-dir f)
          (.delete f))
        (.delete d))
      (.delete d))))

(defn es-fixture
  [f]
  (let [node (make-node {:local-mode true :client-mode false})]
    (with-node-client {:local-mode true}
      (f))
    (.close node)
    (delete-dir "data")))

(use-fixtures :each es-fixture)

(def match-all {:query {:match_all {}}})

(def match-all-string
  (generate-string match-all))

(deftest utils
  (is (=
       (collapse-tree {:discovery {:zen {:ping {:unicast {:hosts ["a" "b"]}
                                                :multicast {:enabled false}}}}})
       {"discovery.zen.ping.multicast.enabled" false
        "discovery.zen.ping.unicast.hosts" ["a" "b"]})))

(deftest es-client
  (is (false? (:exists? (exists-index {:indices ["test"]}))))
  (is (:id (index-doc {:index "test" :type "tyu"
                       :source {:field1 ["toto" "tutu"] :field2 42
                                :field3 {:tyu {:foo "bar"}}}
                       :id "mid"})))
  (is (> (:successful-shards (refresh-index {:indices ["test"]})) 0))
  (is (true? (:exists? (exists-index {:indices ["test"]}))))
  (is (keyword? (:status (cluster-health {:indices ["test"]}))))
  (is (= 1 (:count (count-docs {:indices ["test"]}))))
  (let [c (atom nil)
        l (make-listener {:on-response (fn [r] (reset! c (:count r)))
                          :format :clj})]
    (count-docs {:indices ["test"] :listener l})
    (Thread/sleep 50)
    (is (= 1 @c)))
  (let [ft (count-docs {:indices ["test"] :async? true})]
    (is (future? ft))
    (is (= 1 (:count @ft)))
    (is (realized? ft))
    (is (not (future-cancelled? ft))))
  (let [status (index-status {:indices ["test"]})]
    (is (= 1 (get-in status [:indices :test :docs :num_docs]))))
  (let [d (get-doc {:index "test" :type "tyu" :id "mid" :fields ["field1" "field2" :field3]})]
    (is (nil? (:_source d)))
    (is (= {:tyu {:foo "bar"}} (get-in d [:fields :field3])))
    (is (= ["toto" "tutu"] (get-in d [:fields :field1]))))
  (let [d (get-doc {:index "test" :type "tyu" :id "mid"})]
    (is (nil? (:fields d)))
    (is (= {:tyu {:foo "bar"}} (get-in d [:_source :field3])))
    (is (= ["toto" "tutu"] (get-in d [:_source :field1]))))
  (is (get-in (first
               (get-in (search {:indices ["test"] :types ["tyu"]
                                :extra-source match-all :search-type :query-then-fetch})
                       [:hits :hits]))
              [:_source :field1])
      ["toto" "tutu"])
  (is (get-in
       (first
        (get-in (search {:indices ["test"] :search-type :query-then-fetch
                         :types ["tyu"] :extra-source match-all-string})
                       [:hits :hits]))
              [:_source :field1])
      ["toto" "tutu"])
  (is (re-find #":matches :version .* ?:type :index :id"
               (:doc (meta #'clj-elasticsearch.client/index-doc))))
  (let [payload {:index "test" :type "vvv"
                 :source {:foo "bar"}
                 :id "neo"}
        docv1 (index-doc payload)
        flag (atom false)
        _ (atomic-update-from-source (fn [o] (when-not @flag
                                              (index-doc (assoc payload :ooh "sneaky"))
                                              (reset! flag true))
                                       (assoc o :bar "baz"))
                                     {:index "test" :type "vvv" :id "neo"})
        _ (atomic-update-from-source (fn [_] {:foo "bar"}) {:index "test" :type "vvv" :id "geo"})]
    (is (= {:foo "bar" :bar "baz"}
           (:_source (get-doc {:index "test" :type "vvv" :id "neo"}))))
    (is (= {:foo "bar"}
           (:_source (get-doc {:index "test" :type "vvv" :id "geo"}))))
    (is (not (empty? (get (get-mapping {}) "test"))))
    (is (not (empty? (get @(get-mapping {:async? true}) "test"))))
    (is (empty? (get (get-mapping {:types ["wrong"]}) "test"))))

  (let [payload {:index "test" :type "abc"
                 :doc {:new "source"}
                 :id "upserted"}]
    (update-doc (assoc payload :doc-as-upsert? true))
    (is (= {:new "source"}
           (:_source (get-doc {:index "test" :type "abc" :id "upserted"}))))))

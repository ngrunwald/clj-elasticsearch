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

(def match-all
  (generate-string {"query" {"query_string" {"query" "*:*"}}}))

(deftest es-client
  (is (:id (index-doc {:index "test" :type "tyu"
                       :source (build-document {:field1 "toto" :field2 42})})))
  (is (> (:successful-shards (refresh-index {:indices ["test"]})) 0))
  (is (= 1 (:count (count-docs {:indices ["test"]}))))
  (is (get-in (first
               (get-in (search {:indices ["test"] :types ["tyu"] :extra-source match-all})
                       [:hits :hits]))
              [:_source :field1])
      "toto"))

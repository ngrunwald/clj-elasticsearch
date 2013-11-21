(defproject clj-elasticsearch-native "0.5.0-SNAPSHOT"
  :description "Native Java API client wrapper for Elasticsearch"
  :dependencies [[clj-elasticsearch "0.5.0-SNAPSHOT"]
                 [org.clojure/clojure "1.5.1"]
                 [cheshire "5.0.1"]
                 [gavagai "0.3.1"]]
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :profiles {:dev {:dependencies [[org.elasticsearch/elasticsearch "0.90.7"]]}
             :0.20 {:dependencies [[org.elasticsearch/elasticsearch "0.20.0"]]}
             :0.19 {:dependencies [[org.elasticsearch/elasticsearch "0.19.0"]]}}
  :url "https://github.com/ngrunwald/clj-elasticsearch"
  :codox {:src-dir-uri "https://github.com/ngrunwald/clj-elasticsearch/blob/v0.4.0-RC1"
          :src-linenum-anchor-prefix "L"})

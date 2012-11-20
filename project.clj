(defproject clj-elasticsearch "0.3.2"
  :description "Native Java API client wrapper for Elasticsearch"
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [cheshire "4.0.4"]
                 [gavagai "0.2.0"]]
  :plugins [[codox "0.6.3"]]
  :profiles {:dev {:dependencies [[org.elasticsearch/elasticsearch "0.19.11"]]}}
  :dev-dependencies [[codox "0.6.3"]
                     [org.elasticsearch/elasticsearch "0.19.11"]]
  :repositories {"sonatype.org" "http://oss.sonatype.org/content/repositories/releases/"}
  :url "https://github.com/ngrunwald/clj-elasticsearch"
  :codox {:src-dir-uri "https://github.com/ngrunwald/clj-elasticsearch/tree/master"
          :src-linenum-anchor-prefix "L"})

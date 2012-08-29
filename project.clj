(defproject clj-elasticsearch "0.2.1-SNAPSHOT"
  :description "Native Java API client wrapper for Elasticsearch"
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [cheshire "4.0.0"]
                 [gavagai "0.1.0-SNAPSHOT"]]
  :dev-dependencies [[codox "0.5.0"]
                     [org.elasticsearch/elasticsearch "0.19.9"]]
  :repositories { "sonatype.org" "http://oss.sonatype.org/content/repositories/releases/" }
  :url "https://github.com/ngrunwald/clj-elasticsearch")

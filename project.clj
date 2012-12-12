(defproject clj-elasticsearch "0.3.4-SNAPSHOT"
  :description "Native Java API client wrapper for Elasticsearch"
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [cheshire "5.0.0"]
                 [gavagai "0.2.0"]]
  :plugins [[codox "0.6.3"]]
  :profiles {:dev {:dependencies [[org.elasticsearch/elasticsearch "0.20.1"]]}
             :0.20 {:dependencies [[org.elasticsearch/elasticsearch "0.20.1"]]}
             :0.19 {:dependencies [[org.elasticsearch/elasticsearch "0.19.9"]]}
             :0.18 {:dependencies [[org.elasticsearch/elasticsearch "0.18.0"]]}}
  :dev-dependencies [[codox "0.6.3"]
                     [org.elasticsearch/elasticsearch "0.19.9"]]
  :repositories {"sonatype.org" "http://oss.sonatype.org/content/repositories/releases/"}
  :url "https://github.com/ngrunwald/clj-elasticsearch"
  :codox {:src-dir-uri "https://github.com/ngrunwald/clj-elasticsearch/tree/58fc3d19e5b108908fb74f8340fd055480223f5d"
          :src-linenum-anchor-prefix "L"}
  :warn-on-reflection true)

(defproject clj-elasticsearch "0.4.0-RC1"
  :description "Native Java API client wrapper for Elasticsearch"
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [cheshire "5.0.1"]
                 [gavagai "0.3.1"]]
  :plugins [[codox "0.6.4"]]
  :profiles {:dev {:dependencies [[org.elasticsearch/elasticsearch "0.20.5"]]}
             :0.20 {:dependencies [[org.elasticsearch/elasticsearch "0.20.0"]]}
             :0.19 {:dependencies [[org.elasticsearch/elasticsearch "0.19.0"]]}}
  :repositories {"sonatype.org" "http://oss.sonatype.org/content/repositories/releases/"}
  :url "https://github.com/ngrunwald/clj-elasticsearch"
  :codox {:src-dir-uri "http://github.com/ngrunwald/clj-elasticsearch/blob/master"
          :src-linenum-anchor-prefix "L"}
  :warn-on-reflection true)

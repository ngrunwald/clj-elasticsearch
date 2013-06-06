(defproject org.clojars.touch/clj-elasticsearch "0.4.0-RC2"
  :description "Native Java API client wrapper for Elasticsearch"
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [cheshire "5.2.0"]
                 [gavagai "0.3.1"]]
  :plugins [[codox "0.6.4"]]
  :profiles {:dev {:dependencies [[org.elasticsearch/elasticsearch "0.90.1"]]}}
  :repositories {"sonatype.org" "http://oss.sonatype.org/content/repositories/releases/"}
  :url "https://github.com/ngrunwald/clj-elasticsearch"
  :codox {:src-dir-uri "https://github.com/ngrunwald/clj-elasticsearch/blob/v0.4.0-RC1"
          :src-linenum-anchor-prefix "L"}
  :warn-on-reflection true)

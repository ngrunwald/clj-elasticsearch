# clj-elasticsearch

A clojure wrapper around the native Java Elasticsearch API. It aims to be as idiomatic as possible while remaining fast.

The API docs are available [here](http://ngrunwald.github.com/clj-elasticsearch).

## Usage

For the most basic use case:

```clojure
(use 'clj-elasticsearch)

(def es (make-client :transport {:hosts ["localhost:9300"] :cluster-name "elasticsearch"}))

(def index-res (index-doc es {:index "test" :type "test"
                         :source (build-doc {:field1 "foo" :field2 42})}))
(def nb-docs (:count (count-docs {:indices ["test"]})))
(def match-all "{\"query\":{\"query_string\":{\"query\":\"*:*\"}}}")
(def res (get-in (search es {:indices ["test"] :types ["test"] :extra-source match-all}) [:hits :hits]))
```
Vectors and hashes are converted to arrays. See the doc strings for the arguments used by each method and the javadocs for more details about their use. A convert function can be used to try to translate the returned objects to Clojure or other formats. It can be sprecified by the :format key when calling the various API methods.

You can also use the functions asynchronously by providing a callback listener with the :listener key, as such:

```clojure
(count-docs es {:indices ["test"]
                :listener (make-listener
                              {:on-failure (fn [e] (error e "error in es listener))
                               :on-success (fn [res] (println (convert res :clj)))})})
```
or in a simpler way with sane defaults:

```clojure
(count-docs es {:indices ["test"]
                :listener (listener (fn [res] (println (:count res))))})
```
A bit more details can be found in the tests.

## Compatibility

clj-elasticsearch is tested on all major versions from elasticsearch 0.18.0 onward. It might or might not on older versions.

## See Also

For other Elasticsearch Clojure clients:

* [elastisch](https://github.com/clojurewerkz/elastisch): An idiomatic Clojure wrapper for the Http API
* [esearch](https://github.com/mpenet/clj-esearch): An asynchronous client for the Http API

## License

Copyright (C) 2012 [Linkfluence](http://linkfluence.net)

Distributed under the Eclipse Public License, the same as Clojure.

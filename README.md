# clj-elasticsearch

A clojure wrapper around the native [Java elasticsearch API](http://www.elasticsearch.org/guide/reference/java-api/). It aims to be as idiomatic as possible while remaining fast and complete.

The API docs are available [here](http://ngrunwald.github.com/clj-elasticsearch).

## Installation

`clj-elasticsearch` is available as a Maven artifact from [Clojars](http://clojars.org/clj-elasticsearch).

However, the elasticsearch artefact must also be included, and as a rule of thumb you should always pull exactly the same version of the artifact as the one used by the server to which you want to connect (or at the very least the same major version, according to the [docs](http://www.elasticsearch.org/guide/reference/java-api/client.html)). Otherwise, you might get strange compatibility bugs, especially with the `Node` client.

```clojure
[clj-elasticsearch "0.4.0-RC1"]
[org.elasticsearch/elasticsearch "0.20.5"]
```

## Usage

For the most basic use case:

```clojure
(use 'clj-elasticsearch.client)

(def es (make-client :transport {:hosts ["localhost:9300"] :cluster-name "elasticsearch"}))

(def index-res (index-doc es {:index "test" :type "test"
                              :source {:field1 "foo" :field2 42}}))
(def nb-docs (:count (count-docs {:indices ["test"]})))
(def match-all {:query {:match_all {}}})
(def res (get-in (search es {:indices ["test"] :types ["test"] :extra-source match-all}) [:hits :hits]))
```

The difference between the `:node` and `:transport` clients are detailed in the guide [here](http://www.elasticsearch.org/guide/reference/java-api/client.html). Both are compatible with `clj-elasticsearch`.

Vectors and hashes are converted to arrays. See the doc strings for the arguments used by each method and the javadocs for more details about their use. A convert function can be used to try to translate the returned objects to Clojure or other formats. It can be sprecified by the :format key when calling the various API methods.

You can also use the functions asynchronously by providing a callback listener with the `:listener` key, as such:

```clojure
(count-docs es {:indices ["test"]
                :listener (make-listener
                              {:on-failure (fn [e] (error e "error in es listener"))
                               :on-success (fn [res] (println (convert res :clj)))})})
```
or in a simpler way by setting `:async?` to `true` and dereferencing the returned future:

```clojure
(let [ft (count-docs es {:indices ["test"] :async? true})
      ;; do work here, then when you need it:
      c (:count @ft)]
  (println "COUNT" c))
```
In this case, if the request was a failure, the corresponding Exception will be thrown when you `deref` the returned future. You can try/catch it as usual. You can also use 3-args version of `deref` to give a timeout to the request.

A bit more details on usage can be found in the [tests](https://github.com/ngrunwald/clj-elasticsearch/blob/master/test/clj_elasticsearch/test/client.clj).

## Compatibility

`clj-elasticsearch` is tested from elasticsearch `0.19.0` onward. It might or might not work on older versions.

## Related Posts and Info

* [Release Post](http://theblankscreen.net/blog/2013/02/22/first-public-release-of-clj-elasticsearch/) Some information on the how and why of this client

## See Also

For other elasticsearch Clojure clients:

* [Elastisch](https://github.com/clojurewerkz/elastisch): An idiomatic Clojure wrapper for the HTTP API
* [Esearch](https://github.com/mpenet/clj-esearch): An asynchronous client for the HTTP API

## License

Copyright (C) 2012, 2013 [Linkfluence](http://us.linkfluence.net)

Distributed under the Eclipse Public License, the same as Clojure.

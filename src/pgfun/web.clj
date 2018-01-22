(ns pgfun.web
  (:require [liberator.core :refer [resource defresource]]
            [ring.middleware.params :refer [wrap-params]]
            [clojure.java.io :as io]
            [clojure.data.json :as json]
            [compojure.core :refer [defroutes ANY]]
            [pgfun.core :as db])
  (:import (java.util Date)
           (java.text SimpleDateFormat)))


;; For PUT and POST check if the content type is json.
(defn check-content-type [ctx content-types]
  (if (#{:put :post} (get-in ctx [:request :request-method]))
    (or
     (some #{(get-in ctx [:request :headers "content-type"])}
           content-types)
     [false {:message "Unsupported Content-Type"}])
    true))

(defn body-as-string [ctx]
  (if-let [body (get-in ctx [:request :body])]
    (condp instance? body
      java.lang.String body
      (slurp (io/reader body)))))

;; For PUT and POST parse the body as json and store in the context
;; under the given key.
(defn parse-json [ctx key]
  (when (#{:put :post} (get-in ctx [:request :request-method]))
    (try
      (if-let [body (body-as-string ctx)]
        (let [data (json/read-str body)]
          [false {key data}])
        {:message "No body"})
      (catch Exception e
        (.printStackTrace e)
        {:message (format "IOException: %s" (.getMessage e))}))))

(defresource list-assets
  :allowed-methods [:get :post]
  :available-media-types ["application/json"]
  :handle-ok (fn [_] (json/write-str (db/query-assets)))
  :known-content-type? #(check-content-type % ["application/json"])
  :malformed? #(parse-json % ::data)
  :post! (fn [ctx] (let [entry (into {} (for [[key val] (ctx ::data)]
                                     [(keyword key) val]))
                         asset (entry :asset)
                         date (.parse (SimpleDateFormat. "yyyy-MM-dd") (entry :date))]
                     (db/store-ohlc asset date entry))))

(defresource get-asset [name]
  :allowed-methods [:get]
  :available-media-types ["text/plain"]
  :exists? (fn [_]
             (let [assets (db/query-assets)
                   asset (-> (filter (fn [asset] (= asset name)) assets)
                             not-empty)]
               (if-not (nil? asset)
                 {::entry asset})))
  :handle-ok (fn [_] (json/write-str (db/query-ohlc-aggregates name))))

(defroutes app
  (ANY "/data/ohlc" [] list-assets)
  (ANY ["/data/ohlc/:name"] [name] (get-asset name)))

(def handler
  (-> app
      wrap-params))

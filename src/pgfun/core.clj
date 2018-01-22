(ns pgfun.core
  "Provides some methods to store and retrieve Open, High, Low, Close data in postgres"
  (:require [clojure.java.jdbc :as sql]
            [clojure.set :as set]
            [clojure.string :as str]))

;; Consider HoneySQL

;; TODO: from config on disk
(def db {:dbtype "postgresql"
         :dbname "pgfun"
         :host "172.17.0.4"
         :port 5432
         :user "postgres"
         :password "postgres"})

(def ohlc-table-ddl
  (sql/create-table-ddl :ohlc
                        [[:asset "varchar(256)"]
                         [:open :real "NOT NULL"]
                         [:high :real "NOT NULL"]
                         [:low :real "NOT NULL"]
                         [:close :real "NOT NULL"]
                         [:date :date "NOT NULL"]
                         ["PRIMARY KEY" "(asset, date)"]]
                        {:conditional? true}))

;; Super cheesy way of creating the database table, find a better startup hook
(defn- init
  []
  (sql/db-do-commands db ohlc-table-ddl))

;; todo: move these functions somewhere else - "db.clj"?
(defn- to-sql-date
  [^java.util.Date date]
  (new java.sql.Date (.getTime date)))

(defn store-ohlc
  "Stores the given Open High Low Close data in the database."
  [asset ^java.util.Date date {:keys [:open :high :low :close]}]
  (init)
  (sql/insert! db :ohlc
               {:asset asset
                :open open
                :high high
                :low low
                :close close
                :date (to-sql-date date)}))

(defn query-assets
  "Returns a vector of the known assets."
  []
  (init)
  (let [sql-results (sql/query db ["SELECT DISTINCT asset FROM ohlc"])]
    (map (fn [blah] (:asset blah)) sql-results)))

;; raw query on asset, date ranges. This design is somewhat untenable in the long term. Would rather provide a map of filters/ranges if this gets any more complex.
(defn query-ohlc
  "Queries the OHLC table for rows that match the asset and date range."
  ([asset]
   (init)
   (sql/query db ["SELECT * FROM ohlc WHERE asset = ? " asset]))
  ([from-date to-date]
   (init)
   (sql/query db ["SELECT * FROM ohlc WHERE date >= ? AND date < ?"
                  (to-sql-date from-date) (to-sql-date to-date)]))
  ([asset from-date to-date]
   (init)
   (sql/query db ["SELECT * FROM ohlc WHERE asset = ? AND date >= ? AND date < ?"
                  asset (to-sql-date from-date) (to-sql-date to-date)])))

;; query and aggregate
(defn query-ohlc-aggregates
  "Returns a map of aggregrate name to a map of OHLC values e.g. {\"avg\" {:open 500.0, :high 523.926640926641, :low 475.7972972972973, :close 499.55212355212353}, \"min\" {:open 500.0, :high 500.0, :low 451.0, :close 480.0}, \"max\" {:open 500.0, :high 549.0, :low 500.0, :close 519.0}}"
  [asset]
  (init)
  (let [sql-results (into {} (sql/query db ["SELECT avg(open), avg(high), avg(low), avg(close), min(open), min(high), min(low), min(close), max(open), max(high), max(low), max(close) FROM ohlc WHERE asset = ?" asset]))
        groups ["avg" "min" "max"]
        aggregations [:open :high :low :close]
        ;; Sorted results as nested vectors with raw db results "avg_2", "min_4", etc
        sorted-results (map (fn [grouping]
                              (sort
                               (filter
                                (fn [[key val]]
                                  (str/starts-with? (name key) grouping)) sql-results))) groups)
        ;; Data structure conversion e.g. (([:avg 100] [:avg_2 101]) ([:min 98] [:min_2 102])) => ({:avg 100 :avg_2 101} {:min 98 :min_2 102})
        sorted-results-map (map
                            (fn [group]
                              (into {} (map (fn [[key val]] {key val}) group))) sorted-results)
        ;; Rename the keys to be the actual names (the ultimate goal) i.e. "avg" = "avg", "avg_2" = "high", "avg_3" = "low", "avg_4" = "close"
        final-groups (map (fn [group]
                            (set/rename-keys group
                                             (zipmap (into [] (-> group keys sort))
                                                     aggregations)))
                          sorted-results-map)]
    (zipmap groups final-groups)))

(ns pgfun.core
  "TODO: doc"
  (:gen-class)
  (:require [clojure.java.jdbc :as sql]
            [clojure.set :as set]
            [clojure.string :as str]))

(def db {:dbtype "postgresql"
         :dbname "pgfun"
         :host "172.17.0.4"
         :port 5432
         :user "postgres"
         :password "postgres"})

;; Consider HoneySQL

(defn -main
  "I don't do a whole lot."
  [x]
  (println x "Hello, World!"))

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

(comment (sql/db-do-commands db ohlc-table-ddl))

;; Starter functions/code
(defn add-days [num-days ^java.util.Date d]
  (.getTime (let [cal (java.util.Calendar/getInstance)]
              (doto cal
                (.setTime d)
                (.add java.util.Calendar/DATE num-days)))))

(defn getRandomOHLC [open max-high-adjust max-low-adjust max-close-adjust]
              (let [high (+ open (rand-int max-high-adjust))
                    low (- open (rand-int max-low-adjust))
                    close (+ open (- (rand-int (* 2 max-close-adjust)) max-close-adjust))]
                {:open open :high high :low low :close close}))
(comment 
  (take 500
        (map (fn [i] (let [date (add-days i (new java.util.Date))
                           ohlc (getRandomOHLC 500 50 50 20)]
                       (sql/insert! db :ohlc
                                    {:asset "goog"
                                     :open (:open ohlc)
                                     :high (:high ohlc)
                                     :low (:low ohlc)
                                     :close (:close ohlc)
                                     :date (new java.sql.Date (.getTime date))})))
             (range))))

(comment
  (sql/insert! db :ohlc {:asset "amz" :open 500.0 :high 505.25 :low 498.0 :close 501.25 :date (new java.sql.Date (System/currentTimeMillis))})
  
  (sql/insert! db :ohlc {:asset "amz" :open 500.0 :high 505.25 :low 498.0 :close 501.25 :date (new java.sql.Date (System/currentTimeMillis))})
  )

;; Real code

;pp; TODO: one more method to get all known assets

;; todo: move these functions somewhere else!! "db.clj"?
(defn to-sql-date
  [^java.util.Date date]
  (new java.sql.Date (.getTime date)))

(defn store-ohlc
  "Stores the given Open High Low Close data in the database."
  [asset ^java.util.Date date {:keys [:open :high :low :close]}]
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
  (let [sql-results (sql/query db ["SELECT DISTINCT asset FROM ohlc"])]
    (map (fn [blah] (:asset blah)) sql-results)))

;; raw query on asset, date ranges. This design is somewhat untenable in the long term. Would rather provide a map of filters/ranges if this gets any more complex.
(defn query-ohlc
  "Queries the OHLC table for rows that match the asset and date range."
  ([asset]
   (sql/query db ["SELECT * FROM ohlc WHERE asset = ? " asset]))
  ([from-date to-date]
   (sql/query db ["SELECT * FROM ohlc WHERE date >= ? AND date < ?"
                  (to-sql-date from-date) (to-sql-date to-date)]))
  ([asset from-date to-date]
   (sql/query db ["SELECT * FROM ohlc WHERE asset = ? AND date >= ? AND date < ?"
                  asset (to-sql-date from-date) (to-sql-date to-date)])))

;; query and aggregate
(defn query-ohlc-aggregates
  "Returns a map of aggregrate name to a map of OHLC values e.g. {\"avg\" {:open 500.0, :high 523.926640926641, :low 475.7972972972973, :close 499.55212355212353}, \"min\" {:open 500.0, :high 500.0, :low 451.0, :close 480.0}, \"max\" {:open 500.0, :high 549.0, :low 500.0, :close 519.0}}"
  [asset]
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

;;;;; just to preserve what i'm working on

;; this was manually pulled out of the sql results
(comment
  (def raw-results {:min 500.0, :min_2 500.0, :avg_3 475.62451361867704, :max_3 500.0, :min_3 451.0, :max 500.0, :max_4 519.0, :avg 500.0, :avg_2 524.0719844357976, :avg_4 499.53891050583655, :min_4 480.0, :max_2 549.0})
(def sorted-results (sort (filter (fn [[key val]] (str/starts-with? (name key) "avg")) raw-results)))
(def sorted-results-map (into {} (map (fn [[key val]] {key val}) sorted-results)))
(def final-results (set/rename-keys sorted-results-map (zipmap (into [] sorted-keys) [:open :high :low :close])))
)

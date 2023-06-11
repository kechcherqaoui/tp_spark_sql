package org.example;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
              .appName("tp-spark-sql")
              .master("local[*]")
              .getOrCreate();

        Dataset<Row> incidents = spark
              .read()
              .option("header", "true")
              .csv("incidents.csv");

        try {
            incidents.createTempView("incidentsTable");
        } catch (AnalysisException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        //Le nombre d’incidents par service.
        spark.sql("SELECT service, COUNT(id) AS total_incidents FROM incidentsTable " +
                    "GROUP BY service")
              .show();

        //Les deux années où il a y avait plus d’incidents.
        spark.sql("SELECT date_format(to_date(date, 'dd-MM-yyyy'), 'yyyy') AS year, COUNT(id) AS total_incidents " +
              "FROM incidentsTable " +
              "GROUP BY year " +
              "ORDER BY total_incidents DESC LIMIT 2").show();

        spark.stop();
    }
}
package com.mycompany;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;

import java.io.IOException;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) throws IOException, TableAlreadyExistsException {
        SparkSession spark = SparkSession.builder()
                .appName("Icebert Test")
                .master("local[*]")
                .getOrCreate();

        try {
            spark.conf().set("spark.sql.catalog.mycat", "org.apache.iceberg.spark.SparkCatalog");
            spark.conf().set("spark.sql.catalog.mycat.catalog-impl", "org.apache.iceberg.inmemory.InMemoryCatalog");
            spark.conf().set("spark.sql.catalog.mycat.warehouse", "target/");
            spark.sql("CREATE database if not exists mycat.default");
            Dataset<Row> ds = spark.read().parquet("./all_stock_data.parquet");
            ds.orderBy("Ticker", "Date").writeTo("mycat.default.test_table").using("iceberg").create();
            ds = spark.sql("select * from mycat.default.test_table limit 10");
            ds.show();
        } finally {
            spark.close();
        }
    }
}

package com.mycompany;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.io.IOException;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) throws IOException {
        SparkSession spark = SparkSession.builder().appName("Icebert Test")
                .master("local")
                .getOrCreate();

        try {
            spark.conf().set("spark.sql.catalog.mycat", "org.apache.iceberg.spark.SparkCatalog");
            spark.conf().set("spark.sql.catalog.mycat.catalog-impl", "org.apache.iceberg.inmemory.InMemoryCatalog");
            spark.sql("CREATE database if not exists mycat.default");
            spark.sql("CREATE TABLE mycat.default.test_table(id INT) using iceberg");
            spark.sql("insert into mycat.default.test_table values (1)");
            Dataset<Row> ds = spark.sql("select * from  mycat.default.test_table");
            System.out.println(ds.count());
            ds.show();
        } finally {
            spark.close();
        }
    }
}

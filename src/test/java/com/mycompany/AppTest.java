package com.mycompany;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.HashMap;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;

/**
 * Unit test for simple App.
 */
public class AppTest {

    /**
     * Rigorous Test :-)
     * 
     * @throws Exception
     */
    @Test
    public void shouldAnswerWithTrue() throws Exception {
        SparkSession spark = SparkSession.builder()
                .appName("Icebert Test")
                .master("local")
                .getOrCreate();

        try {
            spark.conf().set("spark.sql.catalog.mycat", "org.apache.iceberg.spark.SparkCatalog");
            spark.conf().set("spark.sql.catalog.mycat.catalog-impl", "org.apache.iceberg.inmemory.InMemoryCatalog");
            spark.sql("CREATE database if not exists mycat.default");
            spark.sql("CREATE TABLE mycat.default.test_table(id INT) using iceberg");
            spark.sql("insert into mycat.default.test_table values (1)");
            Dataset<Row> ds = spark.sql("select * from  mycat.default.test_table");
            ds.show();
            assertTrue(ds.count() > 0);
        } finally {
            spark.close();
        }
    }
}

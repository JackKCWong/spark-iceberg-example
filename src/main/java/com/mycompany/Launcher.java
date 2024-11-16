package com.mycompany;

import java.io.IOException;

import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

public class Launcher {
    public static void main(String[] args) throws IOException {
        SparkAppHandle handle = new SparkLauncher()
                .setSparkHome("/Users/jhuang/repos/spark-iceberg/spark-3.5.3-bin-hadoop3")
                .setAppResource("/Users/jhuang/Workspace/repos/spark-iceberg/spark-iceberg/target/spark-iceberg-1.0-SNAPSHOT-jar-with-dependencies.jar")
                .setMainClass("com.mycompany.App")
                .setMaster("local")
                .setVerbose(false)
                .setConf(SparkLauncher.DRIVER_MEMORY, "2g")
                .startApplication();

        while (!handle.getState().isFinal()) {
            System.out.println("Job state: " + handle.getState());
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // Handle exception
            }
        }

        System.out.println("Job finished with state: " + handle.getState());
    }

}

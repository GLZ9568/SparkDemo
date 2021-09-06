package com.datastax;

import com.datastax.spark.connector.DseConfiguration;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

/**
 * Created by Chun Gao on 17/8/21
 */

public class SparkConnector {

    private String output_file = "data.csv";
    private String source_keyspace = "test";
    private String source_table = "tab";
    private String target_keyspace = "test";
    private String target_table = "tab1";

    private SparkConf getSparkConf() {
        return new SparkConf();
    }

    private SparkConf getDseConf() {
        SparkConf dseConf = DseConfiguration.enableDseSupport(getSparkConf());
        dseConf.setAppName("SparkDemo");
        dseConf.set("spark.dseShuffle.sasl.port", "7447");
        dseConf.set("spark.dseShuffle.noSasl.port", "7437");
        return dseConf;
    }

    public SparkSession getSparkSession() {
        return SparkSession
                .builder()
                .config(getDseConf())
                .enableHiveSupport()
                .getOrCreate();
    }

    public void runTestQuery() {
        System.out.println("Running Spark Test Query");
        // create a Spark Session
        SparkSession sparkSession = this.getSparkSession();
        // create a dataset for the source table data
        Dataset source_ds = sparkSession
                .sql("SELECT * FROM " + source_keyspace + "." + source_table);
        System.out.println("Fetching Source Table Schema from DSE Spark Cluster:");
        // retrieve the source table schema
        StructType source_schema = sparkSession.read()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", source_keyspace)
                .option("table", source_table)
                .load().schema();
        // check the retrieved schema
        source_schema.printTreeString();
        System.out.println("Fetching Query Result from Source Table:");
        // check the retrieved data from source table
        source_ds.toJSON().show();
        System.out.println("Writing Result in to CSV file\n");
        // write the retrieved data into CSV file
        source_ds.write().format("com.databricks.spark.csv").save(output_file);
        // create a dataset for the data to be read from the created CSV file
        Dataset read_ds = sparkSession.read().format("com.databricks.spark.csv")
                .schema(source_schema).load(output_file);
        System.out.println("Reading Query Result from output CSV: ");
        // check the data read from the CSV file
        read_ds.toJSON().show();
        // write the data read from the CSV file to the target table
        read_ds.write().format("org.apache.spark.sql.cassandra")
                .option("keyspace", target_keyspace)
                .option("table", target_table)
                .mode(SaveMode.Append)
                .save();
        // create a dataset to retrieve the data from the target table
        Dataset target_ds = sparkSession
                .sql("SELECT * FROM " + target_keyspace + "." + target_table);
        System.out.println("Fetching Query Result from Target Table:");
        // check the data written to the target table
        target_ds.toJSON().show();
        System.out.println("Done Spark Query Test");
    }
}

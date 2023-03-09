package com.bugdbug.customsource.app;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.SparkSession;

public class CsvDataSourceRunner {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("data_source_test")
                .getOrCreate();

        //Dataset<Row> dataset = sparkSession.read().schema(getSchema()).format("com.bugdbug.customsource.csv.CSV").option("fileName", "/home/ubuntu/p/big-data-projects/Datasource spark3/src/test/resources/1000 Sales Records.csv").load();
        Dataset<Row> dataset = sparkSession.read().schema(getSchema()).format("bugdbug")
                .option("fileName", "/home/ubuntu/p/big-data-projects/Datasource spark3/src/test/resources/1000 Sales Records.csv").load();
        dataset.show();

    }

    private static StructType getSchema() {
        StructField[] structFields = new StructField[]{
                new StructField("Region", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Country", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Item Type", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Sales Channel", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Order Priority", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Order Date", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Order ID", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("Ship Date", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Units Sold", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("Unit Price", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("Unit Cost", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("Total Revenue", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("Total Cost", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("Total Profit", DataTypes.DoubleType, true, Metadata.empty())
        };
        return new StructType(structFields);
    }
}


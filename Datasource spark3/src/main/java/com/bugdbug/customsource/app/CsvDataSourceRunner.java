package com.bugdbug.customsource.app;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.functions.*;
import org.apache.spark.sql.SparkSession;
import java.util.Map;
import java.util.HashMap;

import java.nio.ByteBuffer;
import org.apache.arrow.vector.util.DecimalUtility;
import java.math.BigDecimal;

public class CsvDataSourceRunner {

    public static void main(String[] args) {

	if (args.length != 1) {
		System.out.println("java class csvfile");
		return;
	}

	String filepath = args[0];

        SparkSession sparkSession = SparkSession.builder()
                .appName("data_source_test")
                .getOrCreate();

        //Dataset<Row> dataset = sparkSession.read().schema(getSchema()).format("com.bugdbug.customsource.csv.CSV").option("fileName", "/home/ubuntu/p/big-data-projects/Datasource spark3/src/test/resources/1000 Sales Records.csv").load();
        Dataset<Row> dataset = sparkSession.read().schema(getSchema()).format("bugdbug")
                .option("fileName", filepath).load();

	//System.out.println("COUNT = " + dataset.count());
	/*
	dataset.createOrReplaceTempView("bug");
	Dataset<Row> regionset = sparkSession.sql("select avg(Unit_Price), max(Order_ID) from bug");
        regionset.show();
	*/

	Map<String, String> aggr = new HashMap<String, String>(){{ put("Unit_Price", "sum"); put("Total_Cost", "avg");}};
	dataset.groupBy("Item_Type").agg(aggr).show(false);

	byte[] b = new byte[32];
	b[15] = 1;
	b[31] = 2;
	ByteBuffer bb = ByteBuffer.wrap(b);
	BigDecimal dec1 = DecimalUtility.getBigDecimalFromByteBuffer(bb, 4, 16);
	BigDecimal dec2 = DecimalUtility.getBigDecimalFromByteBuffer(bb, 4, 16);

	System.out.println(dec1.toString() + " " + dec2.toString());



    }

    private static StructType getSchema() {
        StructField[] structFields = new StructField[]{
                new StructField("Region", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Country", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Item_Type", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Sales_Channel", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Order_Priority", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Order_Date", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Order_ID", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("Ship_Date", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Units_Sold", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("Unit_Price", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("Unit_Cost", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("Total_Revenue", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("Total_Cost", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("Total_Profit", DataTypes.DoubleType, true, Metadata.empty())
        };
        return new StructType(structFields);
    }
}


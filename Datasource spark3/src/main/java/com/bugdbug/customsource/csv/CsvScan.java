package com.bugdbug.customsource.csv;

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

public class CsvScan implements Scan {
    private StructType schema;
    private final Map<String, String> properties;
    private final CaseInsensitiveStringMap options;

    public CsvScan(StructType schema,
                   Map<String, String> properties,
                   CaseInsensitiveStringMap options) {

        this.schema = schema;
        this.properties = properties;
        this.options = options;
    }

    @Override
    public StructType readSchema() {
        //return schema;

	/* aggregate */
	StructField[] structFields = new StructField[]{
                new StructField("Item_Type", DataTypes.StringType, true, Metadata.empty()),
                new StructField("SUM(Total_Cost)", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("COUNT(Total_Cost)", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("SUM(Unit_Price)", DataTypes.DoubleType, true, Metadata.empty())
         };

	schema = new StructType(structFields);

	System.out.println(schema.toString());
	return schema;
    }

    @Override
    public String description() {
        return "csv_scan";
    }

    @Override
    public Batch toBatch() {
        return new CsvBatch(schema,properties,options);
    }
}

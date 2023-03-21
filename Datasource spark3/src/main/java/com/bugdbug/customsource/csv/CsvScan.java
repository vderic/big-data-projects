package com.bugdbug.customsource.csv;

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;

import java.util.Map;

public class CsvScan implements Scan {
    private StructType schema;
    private final Map<String, String> properties;
    private final CaseInsensitiveStringMap options;
    private Aggregation aggregation = null;

    public CsvScan(StructType schema,
                   Map<String, String> properties,
                   CaseInsensitiveStringMap options,
		   Aggregation aggregation) {

        this.schema = schema;
        this.properties = properties;
        this.options = options;
	this.aggregation = aggregation;
    }

    @Override
    public StructType readSchema() {
        //return schema;

	if (aggregation != null) {
	/* aggregate */
	StructField[] structFields = new StructField[]{
                new StructField("Item_Type", DataTypes.StringType, true, Metadata.empty()),
                new StructField("SUM(Total_Cost)", DataTypes.createDecimalType(14,4), true, Metadata.empty()),
                new StructField("COUNT(Total_Cost)", DataTypes.LongType, true, Metadata.empty()),
                new StructField("SUM(Unit_Price)", DataTypes.createDecimalType(14,4), true, Metadata.empty())
         };

	schema = new StructType(structFields);
	}

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

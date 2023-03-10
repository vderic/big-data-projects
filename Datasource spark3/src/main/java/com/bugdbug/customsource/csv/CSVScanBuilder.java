package com.bugdbug.customsource.csv;

import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.sql.connector.read.SupportsPushDownAggregates;
import org.apache.spark.sql.connector.read.SupportsPushDownV2Filters;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;
import org.apache.spark.sql.connector.expressions.aggregate.*;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc;
import org.apache.spark.sql.connector.expressions.Expression;


import java.util.Map;

public class CSVScanBuilder implements SupportsPushDownAggregates, SupportsPushDownV2Filters, SupportsPushDownRequiredColumns {
    private final StructType schema;
    private final Map<String, String> properties;
    private final CaseInsensitiveStringMap options;
    private Aggregation aggregation;

    public CSVScanBuilder(StructType schema,
                          Map<String, String> properties,
                          CaseInsensitiveStringMap options) {

        this.schema = schema;
        this.properties = properties;
        this.options = options;
	this.aggregation = null;
    }

    public boolean pushAggregation(Aggregation aggregation) {

	    System.out.println("pushAggregation: " + aggregation.toString());
	    AggregateFunc[] func = aggregation.aggregateExpressions();
	    Expression[] expr = aggregation.groupByExpressions();

	    System.out.println("Group By Columns");
	    for (int i = 0 ; i < expr.length ; i++) {
		    System.out.println(expr[i].describe());
	    }

	    System.out.println("Aggregate ");
	    for (int i = 0 ; i < func.length ; i++) {
		    System.out.println(func[i].describe());
		    if (func[i] instanceof Avg) {
			    System.out.println("AVG FOUND.. not supported");
			    return false;
		    }

		    if (func[i] instanceof Sum) {
			    System.out.println("SUM....here..");
		    }

	    }
	    System.out.println("END pushAggregation");

	    this.aggregation = aggregation;
	    return true;
    }

    /* always return false as kite will send multiple rows with the same key and Spark need to group the data again.
     * Never able to fully complete grouping */
    public boolean supportCompletePushDown(Aggregation aggregation) {

	    /*
	    System.out.println("supportCompletePushDown: " + aggregation.toString());
	    AggregateFunc[] func = aggregation.aggregateExpressions();
	    Expression[] expr = aggregation.groupByExpressions();

	    System.out.println("Group By Columns");
	    for (int i = 0 ; i < expr.length ; i++) {
		    System.out.println(expr[i].describe());
	    }

	    System.out.println("Aggregate ");
	    for (int i = 0 ; i < func.length ; i++) {
		    System.out.println(func[i].describe());
		    if (func[i] instanceof Avg) {
			    System.out.println("AVG FOUND.. not supported");
			    return false;
		    }

		    if (func[i] instanceof Sum) {
			    System.out.println("SUM....here..");
		    }

	    }
	    System.out.println("END supportCompletePushDown(Aggregation)");
	    */
	    return false;
    }

    public Predicate[] pushPredicates(Predicate[] predicates) {
	    return predicates;
    }

    public Predicate[] pushedPredicates() {
	    return new Predicate[0];
    }
    	
    public void pruneColumns(StructType requiredSchema) {
	    System.out.println("pruneColumns: " + requiredSchema.toString());

    }
   

    @Override
    public Scan build() {
        return new CsvScan(schema,properties,options, aggregation);
    }
}

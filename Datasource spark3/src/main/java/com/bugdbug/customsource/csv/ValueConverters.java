package com.bugdbug.customsource.csv;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.Decimal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.math.BigDecimal;

public class ValueConverters {

    public static List<Function> getConverters(StructType schema) {
        StructField[] fields = schema.fields();
        List<Function> valueConverters = new ArrayList<>(fields.length);
        Arrays.stream(fields).forEach(field -> {
            if (field.dataType().equals(DataTypes.StringType)) {
                valueConverters.add(UTF8StringConverter);
            } else if (field.dataType().equals(DataTypes.IntegerType))
                valueConverters.add(IntConverter);
            else if (field.dataType().equals(DataTypes.DoubleType))
                valueConverters.add(DoubleConverter);
	    else if (field.dataType() instanceof DecimalType) {
		    System.out.println("decimal.. convertoer");
		    valueConverters.add(DecimalConverter);
	    }
            else if (field.dataType().equals(DataTypes.LongType))
                valueConverters.add(LongConverter);
        });
        return valueConverters;
    }


    public static Function<String, UTF8String> UTF8StringConverter = UTF8String::fromString;
    public static Function<String, Double> DoubleConverter = value -> value == null ? null : Double.parseDouble(value);
    public static Function<String, Integer> IntConverter = value -> value == null ? null : Integer.parseInt(value);
    public static Function<String, Decimal> DecimalConverter = value -> value == null ? null : Decimal.apply(new BigDecimal(value), 10, 4);
    public static Function<String, Long> LongConverter = value -> value == null ? null : Long.parseLong(value);

}

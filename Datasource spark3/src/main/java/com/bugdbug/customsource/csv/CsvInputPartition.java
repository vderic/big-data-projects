package com.bugdbug.customsource.csv;

import org.apache.spark.sql.connector.read.InputPartition;

public class CsvInputPartition implements InputPartition {
	private final Integer[] fragid;
	private final String host;

	public CsvInputPartition(Integer []fragid, String host) {
		this.fragid = fragid;
		this.host = host;
	}

	@Override
	public String[] preferredLocations() {
		return new String[]{host};
	}

	public Integer[] getFragId() {
		return fragid;
	}
}

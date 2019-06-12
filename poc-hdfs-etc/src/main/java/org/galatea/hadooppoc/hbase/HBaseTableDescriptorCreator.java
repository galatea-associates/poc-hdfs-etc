package org.galatea.hadooppoc.hbase;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;

public class HBaseTableDescriptorCreator {

	private HBaseTableDescriptorCreator() {

	}

	public static HBaseTableDescriptorCreator newTableDescriptorCreator() {
		return new HBaseTableDescriptorCreator();
	}

	public HTableDescriptor createDescriptor(final String tableName, final String... columnFamilies) {
		HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
		for (String columnFamily : columnFamilies) {
			tableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
		}
		return tableDescriptor;
	}

}

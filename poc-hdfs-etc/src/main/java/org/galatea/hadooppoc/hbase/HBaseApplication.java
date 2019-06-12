package org.galatea.hadooppoc.hbase;

import org.apache.hadoop.hbase.TableName;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HBaseApplication {

//	@SneakyThrows
//	public static void main(final String[] args) {
//
////		HBaseAccessor accessor = HBaseAccessor.newDefaultAccessor();
////		accessor.checkHbaseAvailable();
//		try (HBaseAccessor hBaseAccessor = HBaseAccessor.newDefaultAccessor()) {
////			deleteTable(hBaseAccessor, "sampleSwapTable");
////			createTable(hBaseAccessor, "sampleSwapTable", "swapPositions", "swapContracts");
////			log.debug("TEST!!!");
//			Put put = new Put(Bytes.toBytes("row1"));
//			put.addImmutable(Bytes.toBytes("swapPositions"), Bytes.toBytes("ric"), Bytes.toBytes("GOOGL.OQ"));
//			put.addImmutable(Bytes.toBytes("swapPositions"), Bytes.toBytes("knowledgeDate"),
//					Bytes.toBytes("2016-11-17"));
//			put.addImmutable(Bytes.toBytes("swapPositions"), Bytes.toBytes("effectiveDate"),
//					Bytes.toBytes("2016-11-20"));
//			put.addImmutable(Bytes.toBytes("swapContracts"), Bytes.toBytes("startDate"), Bytes.toBytes("2016-05-10"));
//			hBaseAccessor.putTable("sampleSwapTable", put);
//		}
//	}

	private static void deleteTable(final HBaseAccessor accessor, final String tableName) {
		try {
			accessor.deleteTable(TableName.valueOf(tableName));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void createTable(final HBaseAccessor accessor, final String tableName,
			final String... columnFamilies) {
		try {
			accessor.createTable(HBaseTableDescriptorCreator.newTableDescriptorCreator().createDescriptor(tableName,
					columnFamilies));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

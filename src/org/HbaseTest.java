package org;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.thrift.generated.Hbase.createTable_args;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseTest {
	static Configuration config = null;
	static {
		config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "slave1,slave2,slave3");
		config.set("hbase.zookeeper.property.clientPort", "2181");
	}

	public void createTable(String tableName, String[] familys) {
		try {
			HBaseAdmin admin = new HBaseAdmin(config);
			if (admin.tableExists(tableName)) {
				System.out.println(tableName
						+ " is already exists,Please create another table!");
			} else {
				HTableDescriptor desc = new HTableDescriptor(tableName);
				for (int i = 0; i < familys.length; i++) {
					HColumnDescriptor family = new HColumnDescriptor(familys[i]);
					desc.addFamily(family);
				}
				admin.createTable(desc);
				System.out.println("Create table \'" + tableName + "\' OK!");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void createTableSplit(String tableName, String[] familys) {
		try {
			HBaseAdmin admin = new HBaseAdmin(config);
			if (admin.tableExists(tableName)) {
				System.out.println(tableName
						+ " is already exists,Please create another table!");
			} else {
				HTableDescriptor desc = new HTableDescriptor(tableName);
				for (int i = 0; i < familys.length; i++) {
					HColumnDescriptor family = new HColumnDescriptor(familys[i]);
					desc.addFamily(family);
				}
				admin.createTable(desc, "a0".getBytes(), "a10000".getBytes(), 3);
				System.out.println("Create table \'" + tableName + "\' OK!");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void createTableSplit2(String tableName, String[] familys) {
		try {
			HBaseAdmin admin = new HBaseAdmin(config);
			if (admin.tableExists(tableName)) {
				System.out.println(tableName
						+ " is already exists,Please create another table!");
			} else {
				HTableDescriptor desc = new HTableDescriptor(tableName);
				for (int i = 0; i < familys.length; i++) {
					HColumnDescriptor family = new HColumnDescriptor(familys[i]);
					desc.addFamily(family);
				}
				byte[][] regions = new byte[][] { Bytes.toBytes("a3333333"),
						Bytes.toBytes("a6666666") };
				// 表示有三个region分别放入key：
				// [1] start key: , end key: A
				// [2] start key: A, end key: D
				// [3] start key: D, end key:
				admin.createTable(desc, regions);
				System.out.println("Create table \'" + tableName + "\' OK!");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void deleteTable(String tableName) {
		try {
			HBaseAdmin admin = new HBaseAdmin(config);
			if (!admin.tableExists(tableName)) {
				System.out.println(tableName + " is not exists!");
			} else {
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
				System.out.println(tableName + " is delete!");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// public HTableInterface getTable(String tableName) throws IOException {
	// // 普通获取表
	// // HTable table = new HTable(config, tableName);
	// // 通过连接池获取表
	// HTablePool pool = new HTablePool(config, 1000);
	// HTableInterface table = pool.getTable(tableName);
	// return table;
	// }

	public void insertData(String tableName, String rowKey, String family,
			String qualifier, String value) {
		HTableInterface table = null;
		try {
			HTablePool pool = new HTablePool(config, 1000);
			table = pool.getTable(tableName);
			Put put = new Put(Bytes.toBytes(rowKey));
			put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier),
					Bytes.toBytes(value));
			table.put(put);
			System.out.println("insert a data successful!");
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public void deleteData(String tableName, String rowKey) {
		HTableInterface table = null;
		try {
			HTablePool pool = new HTablePool(config, 1000);
			table = pool.getTable(tableName);
			Delete del = new Delete(Bytes.toBytes(rowKey));
			table.delete(del);
			System.out.println("delete a data successful");
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public void queryAll(String tableName) {
		HTableInterface table = null;
		try {
			HTablePool pool = new HTablePool(config, 1000);
			table = pool.getTable(tableName);
			Scan scan = new Scan();
			scan.setStartRow("a1".getBytes());
			scan.setStopRow("a20".getBytes());
			ResultScanner scanner = table.getScanner(scan);
			for (Result row : scanner) {
				System.out.println("\nRowkey: " + new String(row.getRow()));
				for (KeyValue kv : row.raw()) {
					System.out.print(new String(kv.getRow()) + " ");
					System.out.print(new String(kv.getFamily()) + ":");
					System.out.print(new String(kv.getQualifier()) + " = ");
					System.out.print(new String(kv.getValue()));
					System.out
							.print(" timestamp = " + kv.getTimestamp() + "\n");
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static void selectByFilter(String tableName, List<String> arr) {
		HTableInterface table = null;
		try {
			HTablePool pool = new HTablePool(config, 1000);
			table = pool.getTable(tableName);
			FilterList filterList = new FilterList(
					FilterList.Operator.MUST_PASS_ONE);
			Scan s1 = new Scan();
			for (String v : arr) { // 各个条件之间是“或”的关系，默认是“与”
				String[] s = v.split(",");
				filterList.addFilter(new SingleColumnValueFilter(Bytes
						.toBytes(s[0]), Bytes.toBytes(s[1]), CompareOp.EQUAL,
						Bytes.toBytes(s[2])));
				// 添加下面这一行后，则只返回指定的cell，同一行中的其他cell不返回
				// s1.addColumn(Bytes.toBytes(s[0]), Bytes.toBytes(s[1]));
			}
			s1.setFilter(filterList);
			// SingleColumnValueFilter 用于测试列值相等 (CompareOp.EQUAL ), 不等
			// (CompareOp.NOT_EQUAL),或范围 (e.g., CompareOp.GREATER).
			// 下面示例检查列值和字符串'values' 相等...
			// SingleColumnValueFilter f = new
			// SingleColumnValueFilter(Bytes.toBytes("cFamily"),
			// Bytes.toBytes("column"), CompareFilter.CompareOp.EQUAL,
			// Bytes.toBytes("values"));
			// SingleColumnValueFilter f = new
			// SingleColumnValueFilter(Bytes.toBytes("cFamily"),
			// Bytes.toBytes("column"), CompareFilter.CompareOp.EQUAL,new
			// SubstringComparator("values"));
			// s1.setFilter(f);
			// ColumnPrefixFilter 用于指定列名前缀值相等
			// ColumnPrefixFilter f = new
			// ColumnPrefixFilter(Bytes.toBytes("values"));
			// s1.setFilter(f);
			// MultipleColumnPrefixFilter 和 ColumnPrefixFilter 行为差不多，但可以指定多个前缀
			// byte[][] prefixes = new byte[][] {Bytes.toBytes("value1"),
			// Bytes.toBytes("value2")};
			// Filter f = new MultipleColumnPrefixFilter(prefixes);
			// s1.setFilter(f);
			// QualifierFilter 是基于列名的过滤器。
			// Filter f = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new
			// BinaryComparator(Bytes.toBytes("col5")));
			// s1.setFilter(f);
			// RowFilter 是rowkey过滤器,通常根据rowkey来指定范围时，使用scan扫描器的StartRow和StopRow
			// 方法比较好。Rowkey也可以使用。
			// Filter f = new
			// RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, new
			// RegexStringComparator(".*5$"));//正则获取结尾为5的行
			// s1.setFilter(f);

			ResultScanner rs = table.getScanner(s1);
			for (Result rr = rs.next(); rr != null; rr = rs
					.next()) {
				for (KeyValue kv : rr.list()) {
					System.out.println("row : " + new String(kv.getRow()));
					System.out.println("column : " + new String(kv.getFamily())
							+ ":" + new String(kv.getQualifier()));
					System.out.println("value : " + new String(kv.getValue()));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public void queryByRowKey(String tableName, String rowKey) {
		HTableInterface table = null;
		try {
			HTablePool pool = new HTablePool(config, 1000);
			table = pool.getTable(tableName);
			Get get = new Get(rowKey.getBytes());
			Result row = table.get(get);
			for (KeyValue kv : row.raw()) {
				System.out.print(new String(kv.getRow()) + " ");
				System.out.print(new String(kv.getFamily()) + ":");
				System.out.print(new String(kv.getQualifier()) + " = ");
				System.out.print(new String(kv.getValue()));
				System.out.print(" timestamp = " + kv.getTimestamp() + "\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) {
		HbaseTest ht = new HbaseTest();
//		ht.createTable("htest", new String[] { "fcol1", "fcol2" });
		ht.createTableSplit("htest", new String[]{"fcol1","fcol2"});
//		ht.createTableSplit2("htest", new String[]{"fcol1","fcol2"});
//		ht.deleteTable("htest");
//		for (int i = 0; i < 10000000; i++) {
//			ht.insertData("htest", "a"+i, "fcol1", "c1", "aaa"+i);
//			ht.insertData("htest", "a"+i, "fcol1", "c2", "bbb"+i);
//		}
//		ht.deleteData("htest", "a1");
//		ht.queryByRowKey("htest", "a1");
//		ht.queryAll("htest");
//		List list=new ArrayList();
//		list.add("fcol1,c1,aaa1");
//		list.add("fcol1,c1,aaa2");
//		list.add("fcol1,c1,aaa3");
//		ht.selectByFilter("htest",list );
	}
}

package org;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class Test {
	static Configuration config = null;
	static {
		config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "slave1,slave2,slave3");
		config.set("hbase.zookeeper.property.clientPort", "2181");
	}
	public static final String tableName = "webpage";
	public static final String colfmeta = "meta";
	public static final String coltitle = "title";
	public static final String colsummary = "summary";
	public static final String coldate = "date";
	public static final String colfinfo = "info";
	public static final String colinfo = "info";
	public static String rowkey = "www.tarena.com.cn";

	public static void main(String[] args) {
		HTableInterface table = null;
		try {
			HBaseAdmin admin = new HBaseAdmin(config);
			if (admin.tableExists(tableName)) {
				System.out.println("table is already exists!");
			} else {
				HTableDescriptor desc = new HTableDescriptor(tableName);
				HColumnDescriptor family = new HColumnDescriptor(colfmeta);
				desc.addFamily(family);
				family = new HColumnDescriptor(colfinfo);
				desc.addFamily(family);
				admin.createTable(desc);
				HTablePool pool = new HTablePool(config, 1000);
				table = pool.getTable(tableName);

				table.setAutoFlush(false);
				table.setWriteBufferSize(5);

				byte[] buffer = new byte[1024];
				Random r = new Random();
				List<Put> lp = new ArrayList<Put>();
				for (int i = 0; i < 20; ++i) {
					Put p = new Put(Bytes.toBytes(rowkey + '/' + i));
					r.nextBytes(buffer);
					p.add(colfmeta.getBytes(), coltitle.getBytes(),
							("test" + i).getBytes());
					p.add(colfmeta.getBytes(), colsummary.getBytes(), buffer);
					p.add(colfmeta.getBytes(), coldate.getBytes(),
							"2014-5-5".getBytes());
					p.add(colinfo.getBytes(), colinfo.getBytes(), buffer);
					// p.setWriteToWAL(false);
					lp.add(p);
					if (i >= 10) {
						rowkey = "www.xxx.com.cn/" + i;
					}

				}
				table.put(lp);
				lp.clear();
				Scan s1 = new Scan();
				Filter f = new RowFilter(
						CompareFilter.CompareOp.GREATER_OR_EQUAL,
						new RegexStringComparator("www.tarena.com.cn*"));// 正则获取结尾为5的行
				s1.setFilter(f);
				s1.addColumn(colfmeta.getBytes(), coltitle.getBytes());
				ResultScanner rs = table.getScanner(s1);
				for (Result row : rs) {
					for (KeyValue kv : row.list()) {
						System.out.println("row : " + new String(kv.getRow()));
						System.out.println("column : "
								+ new String(kv.getFamily()) + ":"
								+ new String(kv.getQualifier()));
						System.out.println("value : "
								+ new String(kv.getValue()));
					}
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

}

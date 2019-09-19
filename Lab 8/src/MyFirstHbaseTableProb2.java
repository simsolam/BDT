package hbaseTest;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;

public class MyFirstHbaseTableProb2 {
	
	private static final String TABLE_NAME = "user";
	private static final String CF_DEFAULT = "personal_details";
	private static final String CF_PROFESSIONAL = "prof_details";

	public static void main(String... args) throws IOException
	{

		Configuration config = HBaseConfiguration.create();

		try (Connection connection = ConnectionFactory.createConnection(config);
				Admin admin = connection.getAdmin())
		{
			HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
			tableDescriptor.addFamily(new HColumnDescriptor(CF_DEFAULT).setCompressionType(Algorithm.NONE));
			tableDescriptor.addFamily(new HColumnDescriptor(CF_PROFESSIONAL));

			if (!admin.tableExists(tableDescriptor.getTableName()))
			{
				System.out.println("Table " + TABLE_NAME + " doesn't exist!");
				return;
			}

			//---- Get Table
			Table tbl = connection.getTable(TableName.valueOf(TABLE_NAME));
			//------ scanning all rows.
			Scan scan = new Scan();

			System.out.println("Scan all rows for user "+ CF_DEFAULT+" :Name is Bob");
			byte[] rKey = null;
			byte[] salary = null;
			ResultScanner scanner = tbl.getScanner(scan);
			for (Result result : scanner) {
				byte[] valueBytes = result.getValue(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes("Name"));
				if (Bytes.toString(valueBytes).equals("Bob")) {
					rKey = result.getRow();
					salary = result.getValue(Bytes.toBytes(CF_PROFESSIONAL), Bytes.toBytes("salary"));
					System.out.println("user Bob rowKey:" + Bytes.toString(rKey) + ", salary:" + Bytes.toString(salary));
					break;
				}
			}
			
			// promote Bob to "Sr. Engineer"
			Put put = new Put(rKey);
			put.addColumn(Bytes.toBytes(CF_PROFESSIONAL), Bytes.toBytes("Designation"), Bytes.toBytes("Sr. Engineer"));
			tbl.put(put);
			String salaryNw = String.valueOf(Double.valueOf(Bytes.toString(salary)) * 1.05);
			put.addColumn(Bytes.toBytes(CF_PROFESSIONAL), Bytes.toBytes("salary"), Bytes.toBytes(salaryNw));
			tbl.put(put);
			
			System.out.println(" Promoted!");
		}
	}
}

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

public class MyFirstHbaseTableProb3 {
	
	private static final String TABLE_NAME = "user";
	private static final String CF_DEFAULT = "personal_details";
	private static final String CF_PROFESSIONAL = "prof_details";

		public static void main(String... args) throws IOException
		{

			Configuration config = HBaseConfiguration.create();

			try (Connection connection = ConnectionFactory.createConnection(config);
					Admin admin = connection.getAdmin())
			{
				HTableDescriptor tableDes = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
				tableDes.addFamily(new HColumnDescriptor(CF_DEFAULT).setCompressionType(Algorithm.NONE));
				tableDes.addFamily(new HColumnDescriptor(CF_PROFESSIONAL));

				if (!admin.tableExists(tableDes.getTableName()))
				{
					System.out.println("Table " + TABLE_NAME + " doesn't exist!");
					return;
				}

				//Get table
				Table tbl = connection.getTable(TableName.valueOf(TABLE_NAME));
				// ------ scanning all rows.
				Scan scan = new Scan();
				ResultScanner scanner = tbl.getScanner(scan);
				int numRows = 0;
				for (Result row : scanner) {
					numRows++;
				}
				System.out.println("Number of rows: " + numRows);
			}
		}
}

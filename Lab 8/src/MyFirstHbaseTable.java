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

public class MyFirstHbaseTable {
	
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

			System.out.print("Creating table.... ");

			if (admin.tableExists(tableDescriptor.getTableName()))
			{
				admin.disableTable(tableDescriptor.getTableName());
				admin.deleteTable(tableDescriptor.getTableName());
			}
			admin.createTable(tableDescriptor);

			//employee
			String[] fields = new String[3];
			fields[0] = new String("1%John%Boston%Manager%150000");
			fields[1] = new String("2%Mary%New York%Sr. Engineer%130000");
			fields[2] = new String("3%Bob%Fremont%Jr. Engineer%90000");
			
			Table tbl = connection.getTable(TableName.valueOf(TABLE_NAME));
			for(String s:fields){
				String[] field = s.split("%");
				Put put = new Put(Bytes.toBytes(field[0]));
				put.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes("Name"), Bytes.toBytes(field[1]));
				tbl.put(put);
				put.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes("City"), Bytes.toBytes(field[2]));
				tbl.put(put);
				put.addColumn(Bytes.toBytes(CF_PROFESSIONAL), Bytes.toBytes("Designation"), Bytes.toBytes(field[3]));
				tbl.put(put);
				put.addColumn(Bytes.toBytes(CF_PROFESSIONAL), Bytes.toBytes("salary"), Bytes.toBytes(field[4]));
				tbl.put(put);
				
			}

			System.out.println(" Done!");
		}
	}
}

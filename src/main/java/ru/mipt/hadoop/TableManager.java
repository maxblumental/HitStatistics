package ru.mipt.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

public class TableManager {

    private final String tableName;
    private final String[] columns;
    private int count = 0;

    public TableManager(String tableName, String[] columns) throws IOException {
        this.tableName = tableName;
        this.columns = columns;
        init();
    }

    private void init() throws IOException {
        Configuration con = HBaseConfiguration.create();

        HBaseAdmin admin = new HBaseAdmin(con);

        if (admin.tableExists(tableName)) {
            System.out.println("Table already exists.");
            return;
        }

        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

        tableDescriptor.addFamily(new HColumnDescriptor("cf"));

        admin.createTable(tableDescriptor);
        System.out.println("Table \"" + tableName + "\" was created.");

        for (String column : columns) {
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(column);
            admin.addColumn(tableName, columnDescriptor);
            System.out.println("Added column: " + column);
        }

        System.out.println("Initialization finished.");
    }

    public void put(String[] values) throws IOException {
        Configuration config = HBaseConfiguration.create();

        HTable hTable = new HTable(config, tableName);

        Put p = new Put(Bytes.toBytes("row" + Integer.toString(++count)));


        for (int i = 0; i < columns.length; i++) {
            p.add(Bytes.toBytes("cf"), Bytes.toBytes(columns[i]), Bytes.toBytes(values[i]));
        }

        hTable.put(p);
        System.out.printf("data updated (row%d): %s\n", count, Arrays.toString(values));

        hTable.close();
    }
}
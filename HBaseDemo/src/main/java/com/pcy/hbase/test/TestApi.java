package com.pcy.hbase.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author PengChenyu
 * @since 2021-09-14 10:47:48
 */
public class TestApi {

    private static Connection connection = null;
    private static Admin admin = null;

    static {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void close() {
        if (admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 创建命名空间
     *
     * @param namespaceName
     */
    public static void createNamespace(String namespaceName) {
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespaceName).build();
        try {
            admin.createNamespace(namespaceDescriptor);
            System.out.println(String.format("%s命名空间创建成功", namespaceName));
        } catch (IOException e) {
            System.out.println(String.format("%s命名空间创建失败", namespaceName));
            e.printStackTrace();
        }
    }

    /**
     * 判断表是否存在
     *
     * @param tableName
     * @return
     * @throws IOException
     */
    public static boolean isTableExists(String tableName) {
        try {
            return admin.tableExists(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 创建表
     * 注意就API已经过时，现在使用的是新API
     *
     * @param tableName
     * @param cfs
     */
    public static void createTable(String tableName, String... cfs) {
        if (cfs.length <= 0) {
            System.out.println("请设置列族信息");
            return;
        }
        if (isTableExists(tableName)) {
            System.out.println("表已存在");
            return;
        }
        // 创建表描述器
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
        for (String col : cfs) {
            ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.of(col);
            tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
        }
        TableDescriptor tableDescriptor = tableDescriptorBuilder.build();
        try {
            admin.createTable(tableDescriptor);
            System.out.println("创建表成功：" + tableName);
        } catch (IOException e) {
            System.out.println("创建表失败：" + tableName);
            e.printStackTrace();
        }
    }

    /**
     * 删除表，先不可用再删除
     *
     * @param tableName
     */
    public static void deleteTable(String tableName) {
        if (isTableExists(tableName)) {
            try {
                admin.disableTable(TableName.valueOf(tableName));
                admin.deleteTable(TableName.valueOf(tableName));
                System.out.printf("删除？：{}%n", isTableExists(tableName));
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            System.out.printf("{}不存在%n", tableName);
        }
    }

    /**
     * 插入数据
     *
     * @param tableName       表名
     * @param rowKey
     * @param columnFamily    列族
     * @param columnQualifier 列名
     * @param columnValue     值
     */
    public static void putRowData(String tableName, String rowKey, String columnFamily, String columnQualifier, String columnValue) {
        try {
            // 获取表对象
            Table table = connection.getTable(TableName.valueOf(tableName));
            // 创建PUT对象
            Put put = new Put(Bytes.toBytes(rowKey));
            // 给Put对象赋值
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnQualifier), Bytes.toBytes(columnValue));
            // 放入table
            table.put(put);
            table.close();
        } catch (IOException e) {
            System.out.println(String.format("插入%s失败", tableName));
            e.printStackTrace();
        }
    }

    public static void getAllRowData(String tableName, String rowKey, String columnFamily, String columnQualifier) {
        try {
            // 获取表对象
            Table table = connection.getTable(TableName.valueOf(tableName));
            // 创建Get对象
            Get get = new Get(Bytes.toBytes(rowKey));
            // 可以指定列族和列名
            get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnQualifier));
            Result result = table.get(get);
            result.listCells().stream().forEach(
                    x -> System.out.println(
                            "rowKey:" + Bytes.toString(CellUtil.cloneRow(x)) + "\t" +
                                    "cf:" + Bytes.toString(CellUtil.cloneFamily(x)) + "\t" +
                                    "cq:" + Bytes.toString(CellUtil.cloneQualifier(x)) + "\t" +
                                    "cv:" + Bytes.toString(CellUtil.cloneValue(x))

                    )
            );
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) throws IOException {
        //createNamespace("TestNamespace");
        //System.out.println(isTableExists("student1"));
        //createTable("class", "id", "name", "time");
        //deleteTable("class");
        //putRowData("student", "1001", "info", "name", "jim");
        getAllRowData("student", "1001", "info", "name");
        close();

    }
}

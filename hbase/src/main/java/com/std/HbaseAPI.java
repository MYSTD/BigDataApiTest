package com.std;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * ClassName:HbaseAPI
 * Description:
 *
 * @date:2023/10/19 11:01
 * @author:STD
 */
public class HbaseAPI {

    public static void main(String[] args) throws IOException {

//        for (int i = 0; i < 3; i++) {
//            testPut(null,"user","100"+i,"u1","name","user"+i);
//            testPut(null,"user","100"+i,"u1","age","12"+i);
//            testPut(null,"user","100"+i,"u2","sex","man");
//        }

//        testCreateTable(null,"user","u1","u2");
//        testAddColumnFamily(null,"user","f1","f2");
        testDeleteColumnFamily(null,"user","f1","f2");

//        testPut(null,"user","1002","u1","age","18");
//        testGet(null,"user","1002");
//        testDelete(null, "user", "1002", "u1", "name");
//        testScan(null,"user","1000","1002!");

//        testScanWithFilter(null, "user");


        HbaseConnection.closeConnection();
    }


    /**
     * DML - scan
     *
     * 能否基于非rowkey字段进行数据的过滤扫描?  可以。 但是基于非rowkey列的过滤查询是全表扫描。  效率低， 性能差。
     *   需求一: 查找 name = "std"的数据
     *
     *   需求二: 查找 age >= 20 的数据
     *
     *   需求三:  需求一 and 需求二
     *
     *
     *   select  * from  table where id = ?
     *
     *   select * from table where name = ? and age >= ?
     */
    public static void testScanWithFilter(String spaceName, String tableName) throws IOException {
        // 获取连接
        Connection connection = HbaseConnection.getConnection();
        // 获取表名对象
        TableName tn = TableName.valueOf(spaceName, tableName);
        Table table = connection.getTable(tn);
        Scan scan = new Scan();

        // 设置name过滤器
        SingleColumnValueFilter nameFilter = new SingleColumnValueFilter(
                Bytes.toBytes("u1"),Bytes.toBytes("name"), CompareOperator.EQUAL,Bytes.toBytes("std"));
        // 过滤掉没有查找字段的行（逻辑上的行）
        nameFilter.setFilterIfMissing(true);

        // 设置age过滤器
        SingleColumnValueFilter ageFilter = new SingleColumnValueFilter(
                Bytes.toBytes("u1"),Bytes.toBytes("age"), CompareOperator.GREATER_OR_EQUAL,Bytes.toBytes("20"));
        ageFilter.setFilterIfMissing(true);

        // nameFilter and nameFilter
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL,nameFilter,ageFilter);
        scan.setFilter(filterList);

        ResultScanner results = table.getScanner(scan);
        for (Result result : results) {
            //获取所有的Cell对象
            List<Cell> cells = result.listCells();
            printCells(cells);
            System.out.println(" ------------------------------- ");
        }

        table.close();


    }

    /**
     * DML - scan
     *
     * Shell:
     *   scan 'namespaceName:tableName' , {STARTROW=>'' , STOPROW=>''}
     */
    public static void testScan(String spaceName, String tableName,String startRow,String stopRow) throws IOException {
        // 获取连接
        Connection connection = HbaseConnection.getConnection();
        // 获取表名对象
        TableName tn = TableName.valueOf(spaceName, tableName);
        Table table = connection.getTable(tn);

        Scan scan = new Scan();
        // 设置范围
        scan.withStartRow(Bytes.toBytes(startRow)).withStopRow(Bytes.toBytes(stopRow));
        ResultScanner results = table.getScanner(scan);
        for (Result result : results) {
            //获取所有的Cell对象
            List<Cell> cells = result.listCells();
            printCells(cells);
            System.out.println(" ------------------------------- ");
        }
        table.close();

    }


    /**
     * DML - get
     *
     * Shell : get 'namespaceName:tableName' , 'rk'
     */
    public static void testGet(String spaceName, String tableName, String rk) throws IOException {
        // 获取连接
        Connection connection = HbaseConnection.getConnection();
        // 获取表名对象
        TableName tn = TableName.valueOf(spaceName, tableName);
        Table table = connection.getTable(tn);
        Get get = new Get(Bytes.toBytes(rk));
        Result result = table.get(get);
        //获取所有的Cell对象
        List<Cell> cells = result.listCells();
        printCells(cells);
        table.close();
    }

    // 打印cells（逻辑上的一行数据）
    public static void printCells(List<Cell> cells){
        for (Cell cell : cells) {
            String kv = Bytes.toString(CellUtil.cloneRow(cell)) + " : " +
                    Bytes.toString(CellUtil.cloneFamily(cell)) + " : " +
                    Bytes.toString(CellUtil.cloneQualifier(cell)) + " : " +
                    Bytes.toString(CellUtil.cloneValue(cell));
            System.out.println(kv);
        }
    }

    /**
     * DML - 删除
     *
     * Shell :
     * delete  'namespaceName:tableName' , 'rk' , 'cf:cl'
     *
     * deleteall 'namespaceName:tableName' , 'rk' , 'cf:cl'（删除所有版本）
     *
     * deleteall 'namespaceName:tableName' , 'rk'
     */
    public static void testDelete(String spaceName, String tableName, String rk, String cf, String cl) throws IOException {
        // 获取连接
        Connection connection = HbaseConnection.getConnection();
        // 获取表名对象
        TableName tn = TableName.valueOf(spaceName, tableName);
        Table table = connection.getTable(tn);

        Delete delete = new Delete(Bytes.toBytes(rk));
        //Delete
//        delete.addColumn(Bytes.toBytes(cf) , Bytes.toBytes(cl));

        //DeleteColumn(删除指定列的所有版本)
//        delete.addColumns(Bytes.toBytes(cf), Bytes.toBytes(cl));

        //DeleteFamily
//        delete.addFamily(Bytes.toBytes(cf));

        table.delete(delete);

        table.close();
    }

    /**
     * DML - 新增、修改
     *
     * Shell: put 'namespaceName:tableName' , 'rk' , 'cf:cl' , 'value'
     * rk:rowKey 表示行键
     */
    public static void testPut(String spaceName, String tableName, String rk, String cf, String cl, String value) throws IOException {
        // 获取连接
        Connection connection = HbaseConnection.getConnection();
        // 获取表名对象
        TableName tn = TableName.valueOf(spaceName, tableName);
        // table主要用于实现DWL操作
        Table table = connection.getTable(tn);
        Put put = new Put(Bytes.toBytes(rk));

        // 直接添加一行
//        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cl),Bytes.toBytes(value));

        // 封装成cell
        Cell cell = new KeyValue(Bytes.toBytes(rk), Bytes.toBytes(cf), Bytes.toBytes(cl), Bytes.toBytes(value));
        put.add(cell);

        table.put(put);
        table.close();
    }

    /**
     * DDL 删除列族
     *
     * Shell : alter 't1' , 'delete' => 'f1'
     */
    public static void testDeleteColumnFamily(String spaceName, String tableName, String... cfs) throws IOException {
        // 获取连接
        Connection connection = HbaseConnection.getConnection();
        // 获取表名对象
        TableName tn = TableName.valueOf(spaceName, tableName);
        Admin admin = connection.getAdmin();

        for (String cf : cfs) {
            admin.deleteColumnFamily(tn,Bytes.toBytes(cf));
        }
        System.out.println(admin.getDescriptor(tn));
        admin.close();
    }

    /**
     * DDL 添加列族
     *
     * Shell : alter 't1' , {NAME=>'f1'} ,  {NAME=>'f3', VERSIONS=>2}
     *
     */
    public static void testAddColumnFamily(String spaceName, String tableName, String... cfs) throws IOException {
        // 获取连接
        Connection connection = HbaseConnection.getConnection();
        // 获取表名对象
        TableName tn = TableName.valueOf(spaceName, tableName);
        Admin admin = connection.getAdmin();

        for (String cf : cfs) {
            ColumnFamilyDescriptor columnFamilyDescriptor =
                    ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf)).build();
            admin.addColumnFamily(tn,columnFamilyDescriptor);
        }

        admin.close();
    }


    /**
     * DDL 创建表
     *
     * Shell : create 'namespaceName:tableName' , 'cfs...'
     * cfs：ColumnFamily，表示列族
     */
    public static void testCreateTable(String spaceName, String tableName, String... cfs) throws IOException {
        // 获取连接
        Connection connection = HbaseConnection.getConnection();
        // 获取表名对象
        TableName tn = TableName.valueOf(spaceName, tableName);
        // admin主要用于实现DDL操作
        Admin admin = connection.getAdmin();

        if (admin.tableExists(tn)) {
            throw new RuntimeException("表以存在！");
        }
        if (cfs == null || cfs.length == 0) {
            throw new RuntimeException(" 至少指定一个列族！");
        }

        // 通过类方法，获取自身对象
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tn);
        for (String cf : cfs) {
            ColumnFamilyDescriptor columnFamilyDescriptor
                    = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf)).build();
            tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
        }

        // 建造者模式，获取对象
        TableDescriptor tableDescriptor = tableDescriptorBuilder.build();
        admin.createTable(tableDescriptor);
        System.out.println("创建表" + tableName + "成功");
        admin.close();
    }
}

package com.std;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
* ClassName:HbaseConnection
* Description:
* @date:2023/10/19 9:35
* @author:STD
*/public class HbaseConnection {

    private static final Connection connection;

    // 单例构建
    static {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum" , "hadoop102,hadoop103,hadoop104");
        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Connection getConnection(){
        return connection;
    }

    public static void closeConnection() throws IOException {
        if(connection != null && !connection.isClosed()){
            try {
                connection.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static void main(String[] args) {
        System.out.println(getConnection().getClass().getName());
    }
}

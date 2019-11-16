package wen;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
//向表中添加数据, rowkey="0001"  f1:name = "zhangsan"  f1:name = 18  f1:id = 25 f2:address = "地球人"
public class AddData {
    public static void main(String[] args) throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","Node1,Node2,Node3");
        Connection connection = ConnectionFactory.createConnection(configuration);

        //获取表
        Table table = connection.getTable(TableName.valueOf("emp17124080228"));


        Put put = new Put("Rain".getBytes());
        put.addColumn("member_id".getBytes(),"id".getBytes(),Bytes.toBytes(31));
        put.addColumn("info".getBytes(),"age".getBytes(), Bytes.toBytes(28));
        put.addColumn("info".getBytes(),"birthday".getBytes(),Bytes.toBytes("1990-05-01"));
        put.addColumn("info".getBytes(),"industry".getBytes(),Bytes.toBytes("architect"));
        put.addColumn("address".getBytes(),"city".getBytes(),Bytes.toBytes("ShenZhen"));
        put.addColumn("address".getBytes(),"country".getBytes(),Bytes.toBytes("China"));


        //执行添加操作
        table.put(put);
        //释放资源
        table.close();

        connection.close();
    }
}

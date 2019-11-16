package wen;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import java.io.IOException;

public class CrreateHBaseTable {
    public static void main(String[] args) throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        //连接hbase集群不需要指定hbase主节点的ip和端口
        //只需指定各节点zooleeper的ip和端口,只指定一台zookeeper也可以
        configuration.set("hbase.zookeeper.quorum","Node1:8020,Node2:8020,Node3:8020");
        configuration.set("hbase.rootdir", "hdfs://Node1:8020/hbase");
        configuration.set("hbase.cluster.distributed", "true");
        //创建连接池对象
        Connection connection = ConnectionFactory.createConnection(configuration);

        //对数据库进行DDL操作时,要获取管理员对象
        Admin admin = connection.getAdmin();

        //指定我们创建的表的名字
        TableName myuser = TableName.valueOf("emp17124080228");
        HTableDescriptor hTableDescriptor = new HTableDescriptor(myuser);

        //指定表的列族
        HColumnDescriptor f1 = new HColumnDescriptor("member_id");
        HColumnDescriptor f2 = new HColumnDescriptor("address");
        HColumnDescriptor f3 = new HColumnDescriptor("info");
        hTableDescriptor.addFamily(f1);
        hTableDescriptor.addFamily(f2);
        hTableDescriptor.addFamily(f3);

        //创建表
        admin.createTable(hTableDescriptor);

        //释放资源
        admin.close();
        connection.close();
    }
}

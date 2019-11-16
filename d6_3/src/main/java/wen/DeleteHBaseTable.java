package wen;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
//删除myuser表
public class DeleteHBaseTable {
    public static void main(String[] args) throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        //连接hbase集群不需要指定hbase主节点的ip和端口
        //只需指定各节点zooleeper的ip和端口,只指定一台zookeeper也可以
        configuration.set("hbase.zookeeper.quorum","node01:2181,node02:2181,node03:2181");

        //创建连接池对象
        Connection connection = ConnectionFactory.createConnection(configuration);

        //对数据库进行DDL操作时,要获取管理员对象
        Admin admin = connection.getAdmin();

        TableName myuser = TableName.valueOf("myuser");
        admin.disableTable(myuser);
        admin.deleteTable(myuser);

        //释放资源
        admin.close();
        connection.close();
    }
}

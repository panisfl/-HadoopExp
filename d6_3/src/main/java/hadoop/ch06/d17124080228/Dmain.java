package hadoop.ch06.d17124080228;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
public class Dmain {
    private static Connection connection = null;
    private static Admin admin = null;
    static {
        try {
            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum","Node1");
            configuration.set("hbase.rootdir", "hdfs://Node1:8020/hbase");
            configuration.set("hbase.cluster.distributed", "true");
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static boolean isTableExist(String tableName) throws IOException {
        boolean exists = admin.tableExists(TableName.valueOf(tableName));
        //返回结果
        return exists;
    }
    public static void close()  {
        if(admin != null);{
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if(connection!=null);
        try {
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void createTable(String tableName,String... cfs) throws IOException {
        if (cfs.length<=0){
            System.out.println("请设置列族信息");
            return;
        }
        if (isTableExist(tableName)){
            System.out.println(tableName+"表已存在");
            return;
        }
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        for (String cf : cfs) {
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }

        admin.createTable(hTableDescriptor);
    }
    public static void insert(String rowKey, String tableName,
                              String[] column1, String[] value1, String[] column2, String[] value2,
                              String[] column3, String[] value3) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        HColumnDescriptor[] columnFamilies = table.getTableDescriptor().getColumnFamilies();
        for (int i = 0; i < columnFamilies.length; i++) {
            String f = columnFamilies[i].getNameAsString();
            if (f.equals("member_id")) {
                for (int j = 0; j < column1.length; j++) {
                    put.addColumn(Bytes.toBytes(f), Bytes.toBytes(column1[j]), Bytes.toBytes(value1[j]));
                }
            }
            if (f.equals("address")) {
                for (int j = 0; j < column2.length; j++) {
                    put.addColumn(Bytes.toBytes(f), Bytes.toBytes(column2[j]), Bytes.toBytes(value2[j]));
                }
            }
            if (f.equals("info")) {
                for (int j = 0; j < column3.length; j++) {
                    put.addColumn(Bytes.toBytes(f), Bytes.toBytes(column3[j]), Bytes.toBytes(value3[j]));
                }
            }
        }
        table.put(put);
        table.close();
    }
    public static void get(String tableName,String rowKey,String cf,String cn) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        // get.addFamily(Bytes.toBytes(cf));
        get.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn));
        Result result = table.get(get);
        for (Cell cell : result.rawCells()) {
            System.out.println(" 行键："+Bytes.toString(CellUtil.cloneRow(cell))+", 列族："+Bytes.toString(CellUtil.cloneFamily(cell))+
                    ", 列名："+Bytes.toString(CellUtil.cloneQualifier(cell))+
                    ", 值："+Bytes.toString(CellUtil.cloneValue(cell)));
        }
    }
    public static void scan(String tableName,String cf,String cn) throws IOException {
        //获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));
//        Scan scan = new Scan(Bytes.toBytes("Rain"), Bytes.toBytes("1003"));
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn));
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println(" 行键："+Bytes.toString(CellUtil.cloneRow(cell))+ ", 列族："+Bytes.toString(CellUtil.cloneFamily(cell))+
                        ", 列名："+Bytes.toString(CellUtil.cloneQualifier(cell))+
                        ", 值："+Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        table.close();
    }
    public static void scanall(String tableName) throws IOException {
        //获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner resultScanner = table.getScanner(scan);
        ResultScanner rs = null;
        try {
            rs = table.getScanner(scan);
            for (Result r : rs) {
                for (Cell cell : r.listCells()) {
                    System.out.println("------------------------------------");
                    System.out.println("行键: " + new String(CellUtil.cloneRow(cell)));
                    System.out.println("列族: " + new String(CellUtil.cloneFamily(cell)));
                    System.out.println("列名: " + new String(CellUtil.cloneQualifier(cell)));
                    System.out.println("值 : " + new String(CellUtil.cloneValue(cell)));
                }
            }
        } finally {
            rs.close();
        }
        table.close();
    }
    public static void main(String[] args) throws IOException {
        String[] column1 = { "id" };
        String[] value1 = {"28"};
        String[] column2 = { "city", "country" };
        String[] value2 = { "Zhongshan", "China" };
        String[] column3 = { "age", "birthday","industry" };
        String[] value3 = { "28", "1990-05-01","architect" };
        System.out.println(isTableExist("stu"));
        System.out.println("--------------创建表--------------");
        createTable( "stu","member_id","address","info");
        System.out.println("创建表是否成功"+isTableExist("stu"));
        insert("Rain","stu",column1,value1,column2,value2,column3,value3);
        System.out.println("---------获取info列族信息---------");
        get("stu","Rain","info","age");
        get("stu","Rain","info","birthday");
        get("stu","Rain","info","industry");
        System.out.println("----查看扫描info:birthday列数据---");
        scan("stu","info","birthday");
        System.out.println("-----------查看全部数据-----------");
        scanall("stu");
        close();
    }
}

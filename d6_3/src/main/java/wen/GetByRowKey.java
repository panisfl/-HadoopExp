package wen;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.util.List;
//查询rowkey为0003的f1列族所有数据
public class GetByRowKey {
    public static void main(String[] args) throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","Node1,Node2,Node3");
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table table = connection.getTable(TableName.valueOf("emp17124080228"));

        //通过Get对象,指定rowkey,默认查询这个rowkey的所有数据
        Get get = new Get(Bytes.toBytes("Rain"));

        //限制只查询f1列族下面所有列的值
        get.addFamily("info".getBytes());

        //执行get查询,返回一个result对象,所有rowkey为0003的单元格的所有信息都会都封装在这个对象中
        Result result = table.get(get);

        //将每个单元格的所有信息封装在cell对象内,再放入集合中
        List<Cell> cells = result.listCells();
        //遍历cells, 看cell中封装了哪些信息
        for (Cell cell : cells) {
            byte[] value = CellUtil.cloneValue(cell);//获取单元格的值
            byte[] rowkey = CellUtil.cloneRow(cell);//获取单元格对应的rowkey
            byte[] columnName = CellUtil.cloneQualifier(cell);//获取单元格所属的列名
            byte[] familyName = CellUtil.cloneFamily(cell);//获取单元格所属的列族名

            System.out.println(Bytes.toString(familyName));
            System.out.println(Bytes.toString(columnName));
            System.out.println(Bytes.toString(rowkey));

            //列名是Id和列名是age的列对应的value整数int类型,要判断一下转成int类型
            if("age".equals(Bytes.toString(columnName))){
                System.out.println(Bytes.toInt(value));
            }else {
                System.out.println(Bytes.toString(value));
            }
        }
        table.close();
        connection.close();
    }
}


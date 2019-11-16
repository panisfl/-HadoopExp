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
//查询rowkey为[0003,0007)之间的数据
public class ScanRangeData {
    public static void main(String[] args) throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","Node1,Node2,Node3");
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table table = connection.getTable(TableName.valueOf("emp17124080228"));

        Scan scan = new Scan();//默认会全表扫描,所有下面要添加约束

        //扫描f1列族

        //扫描f2列族的phone字段
        scan.addColumn("info".getBytes(),"birthday".getBytes());

        //设定扫描的起始行
        scan.setStartRow("Rain".getBytes());
        scan.setStopRow("Rain".getBytes());

        //通过getScan查询获得表中符合要求的数据, 返回的是多条数据
        ResultScanner scanner = table.getScanner(scan);

        //遍历ResultScanner 得到每一条数据，每一条数据都是封装在result对象里面了
        for (Result result : scanner) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                byte[] familyName = CellUtil.cloneFamily(cell);
                byte[] qualifierName = CellUtil.cloneQualifier(cell);
                byte[] rowkey = CellUtil.cloneRow(cell);
                byte[] valueArr = CellUtil.cloneValue(cell);

                if("age".equals(Bytes.toString(qualifierName))){
                    int value = Bytes.toInt(valueArr);
                    System.out.println(Bytes.toString(familyName)+"\t"
                            +Bytes.toString(qualifierName)+"\t"
                            +Bytes.toString(rowkey)+"\t"
                            +value);
                }else {
                    String value = Bytes.toString(valueArr);
                    System.out.println(Bytes.toString(familyName)+"\t"
                            +Bytes.toString(qualifierName)+"\t"
                            +Bytes.toString(rowkey)+"\t"
                            +value);
                }
            }
        }

        table.close();
        connection.close();
    }
}

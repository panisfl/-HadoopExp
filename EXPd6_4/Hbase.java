package hadoop.ch06.d17124080228;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.*;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
//import org.apache.hadoop.fs.FileSystem;

/*先去hbase中创建word表
 *     create 'word' ,'content'
 *     put 'word' ,'1' ,'content:info' ,'a a a b b b'
 *
 *
 * 关于上述word表结构仅通过下方的代码推出，课本没有对本代码过多描述
 * */


public class Hbase {
    public static class TokenizerMapper
            extends TableMapper<Text, IntWritable> {
        //key相当于行键，value 一行记录
        public void map(ImmutableBytesWritable key, Result value, Context context
        ) throws IOException, InterruptedException {


            //这里应该是索引到列族content下的列名info
            String data = Bytes.toString(value.getValue(Bytes.toBytes("address"), Bytes.toBytes("city")));
            //赋值给data后安装空格分割
            String [] words = data.split(" ");
            for (String w:words){
                context.write(new Text(w) , new IntWritable(1));
            }

        }
    }



    public static class IntSumReducer
            extends TableReducer<Text,IntWritable,ImmutableBytesWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(Text k3, Iterable<IntWritable> v3,
                           Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable i : v3) {
                sum += i.get();
            }

            //输出：表中的一条记录Put对象
            //使用单词作为行键

            Put put = new Put(Bytes.toBytes(k3.toString()));
            put.addColumn(Bytes.toBytes("content"),Bytes.toBytes("count"),Bytes.toBytes(String.valueOf(sum)));

//                写入Hbase
            context.write(new ImmutableBytesWritable(Bytes.toBytes(k3.toString())), put);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //设置zookeeper
        conf.set("hbase.zookeeper.quorum", "Node1");
        conf.set("hbase.rootdir", "hdfs://Node1:8020/hbase");//写入配置
        conf.set("hbase.cluster.distributed", "true");//写入配置

        Job job = Job.getInstance(conf);//这里推特只能用conf

        job.setJarByClass(Hbase.class);
        //定义一个扫描器
        Scan scan = new Scan();
        //定义一个扫描器的数据
        scan.addColumn(Bytes.toBytes("address"),Bytes.toBytes("city"));
        //指定Map,输入是表word
        TableMapReduceUtil.initTableMapperJob("member17124080228" ,scan , TokenizerMapper.class,
                Text.class , IntWritable.class ,job);
        //指定Map,输出是表result
        TableMapReduceUtil.initTableReducerJob("result17124080228" , IntSumReducer.class,job);
        job.waitForCompletion(true);
    }
}






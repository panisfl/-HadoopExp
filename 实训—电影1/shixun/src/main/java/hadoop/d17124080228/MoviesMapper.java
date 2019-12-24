package hadoop.d17124080228;

import com.alibaba.fastjson.JSON;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class MoviesMapper extends Mapper<LongWritable,Text,MovieInfo,NullWritable>{
    @Override
    protected void map(LongWritable key1,Text value1,Context context) throws IOException, InterruptedException{
        String json=value1.toString();
        //Fastjson 转换 json 字符串为 Java 对象
        MovieInfo movi= JSON.parseObject(json,MovieInfo.class);

        //查询张耀扬
        if(movi.getActor().contains("张耀扬")){
            context.write(movi,NullWritable.get());
        }else{}

    }

}
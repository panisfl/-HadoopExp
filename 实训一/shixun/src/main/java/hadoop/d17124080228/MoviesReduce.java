package hadoop.d17124080228;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MoviesReduce extends Reducer<MovieInfo,NullWritable,Text,NullWritable> {
    int count = 0;
    protected void reduce(MovieInfo k3, Iterable<NullWritable> v3, Context context)
            throws IOException, InterruptedException {
        if(count < 5){
            context.write(new Text(k3.toString()),NullWritable.get());
            count++;
        }
    }
}

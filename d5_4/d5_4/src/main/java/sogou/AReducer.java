package sogou;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AReducer extends Reducer<NullWritable,pokemon,NullWritable,Text> {

    @Override
    protected void reduce(NullWritable k3, Iterable<pokemon> v3,
                          Context context) throws IOException, InterruptedException {
        String line=null;
        for (pokemon v1 : v3) {
            line = v1.toString();
            context.write(k3, new Text(line));
        }
    }
}
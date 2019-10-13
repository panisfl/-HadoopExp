package hadoop.ch05.d17124080228;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SalaryTotalReducer extends Reducer<NullWritable,Employe,NullWritable,Text> {

    @Override
    protected void reduce(NullWritable k3, Iterable<Employe> v3,
                          Context context) throws IOException, InterruptedException {
        String line=null;
        for (Employe v : v3) {
            line = v.toString();
            context.write(k3, new Text(line));
        }
    }
}

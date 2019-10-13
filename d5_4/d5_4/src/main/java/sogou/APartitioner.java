package sogou;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class APartitioner extends Partitioner<NullWritable, pokemon>{

    @Override
    public int getPartition(NullWritable k2, pokemon v2, int arg) {



        if(v2.getURL() == 2 && v2.getDian() == 1) {

            return 1%arg;
        }else if (v2.getURL() == 1 && v2.getDian() == 1 ){

            return 2%arg;
        }else {

            return 3%arg;
        }
    }
}

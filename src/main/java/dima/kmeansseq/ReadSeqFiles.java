package dima.kmeansseq;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;
import org.apache.mahout.math.VectorWritable;

public class ReadSeqFiles {

    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub
        int counter = 0;
        Configuration conf = new Configuration();
        // Path path = new
        // Path("/home/mhuelfen/Documents/stratos_workspace/vectortest/input/part-r-00000");
         Path path = new
         Path("hdfs://localhost:9000/user/mhuelfen/input/vectors");
//        Path path = new Path(
//                "hdfs://localhost:9000/user/mhuelfen/input/small__vectors");
        for (Pair<Writable, VectorWritable> record

        : new SequenceFileIterable<Writable, VectorWritable>(path, true, conf)) {
            // System.out.println("F" + record.getFirst());
            // System.out.println("S" + record.getSecond().get());

            counter++;
//            System.out.println(counter + " " + record.getSecond().get());
        }
        System.out.println("Found vectors: " + counter);

    }

}

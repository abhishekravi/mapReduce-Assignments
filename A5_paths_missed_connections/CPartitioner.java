import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Used to partition the data into the correct reducer.
 * @author Abhishek Ravi Chandran
 */
public class CPartitioner extends Partitioner<CustomKey, CWritable> {
    
    @Override
    public int getPartition(CustomKey key, CWritable val, int numPartitions) {
		int hash = key.cyear.hashCode();
        return Math.abs(hash % numPartitions);
    }
 
}

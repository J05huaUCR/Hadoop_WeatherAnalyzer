package weatherAnalyzerPackage;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class AnchorPartitioner extends Partitioner<AnchorKey,Text> {

  @Override
  public int getPartition(AnchorKey anchorKey, Text text, int numPartitions) {
      return anchorKey.getJoinKey().hashCode() % numPartitions;
  }
}

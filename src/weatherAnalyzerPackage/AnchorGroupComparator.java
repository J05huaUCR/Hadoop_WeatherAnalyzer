package weatherAnalyzerPackage;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class AnchorGroupComparator extends WritableComparator {

  public AnchorGroupComparator() {
      super(AnchorKey.class,true);
  }

  @Override
  public int compare(WritableComparable a, WritableComparable b) {
    AnchorKey anchorKey1 = (AnchorKey)a;
    AnchorKey anchorKey2 = (AnchorKey)b;
    return anchorKey1.getJoinKey().compareTo(anchorKey2.getJoinKey());
  }
}


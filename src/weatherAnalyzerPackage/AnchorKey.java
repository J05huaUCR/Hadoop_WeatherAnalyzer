package weatherAnalyzerPackage;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AnchorKey implements Writable, WritableComparable<AnchorKey> {

  private Text joinKey = new Text();
  private IntWritable tag = new IntWritable();

  public AnchorKey() {
  }
  
  public void set(String key, int tag){
    this.joinKey.set(key);
    this.tag.set(tag);
  }

  @Override
  public int compareTo(AnchorKey anchorKey) {
      int compareValue = this.joinKey.compareTo(anchorKey.getJoinKey());
      if(compareValue == 0 ){
          compareValue = this.tag.compareTo(anchorKey.getTag());
      }
     return compareValue;
  }

  public static AnchorKey read(DataInput in) throws IOException {
      AnchorKey taggedKey = new AnchorKey();
      taggedKey.readFields(in);
      return taggedKey;
  }

  @Override
  public void write(DataOutput out) throws IOException {
      joinKey.write(out);
      tag.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
      joinKey.readFields(in);
      tag.readFields(in);
  }

  public Text getJoinKey() {
      return joinKey;
  }

  public IntWritable getTag() {
      return tag;
  }

}
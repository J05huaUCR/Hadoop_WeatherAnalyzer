package weatherAnalyzerPackage;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import java.io.IOException;

public class ReducerForJoin extends Reducer<AnchorKey, Text, NullWritable, Text> {

  private Text joinedText = new Text();
  private StringBuilder builder = new StringBuilder();
  private NullWritable nullKey = NullWritable.get();

  @Override
  protected void reduce(AnchorKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      
      int count = 0;
      String keyPassedIn = key.getJoinKey().toString();
      String stationInfo = "";
      for (Text value : values) {
        if (count == 0) {
          // First entry is station info,
          stationInfo = value.toString();
          builder.append(keyPassedIn + ",");
          builder.append(value.toString());
          count = 1;
        } else {
          // write out station plus record
          builder.append(keyPassedIn + ",");
          builder.append(stationInfo);
          builder.setLength(builder.length()-1); 
          String tempValue = value.toString();
          tempValue = tempValue.substring(1, tempValue.length());
          tempValue = "," + tempValue + "\n";
          builder.append(tempValue);
        }
      }
      joinedText.set(builder.toString());
      context.write(nullKey, joinedText);
      builder.setLength(0);

  }

}

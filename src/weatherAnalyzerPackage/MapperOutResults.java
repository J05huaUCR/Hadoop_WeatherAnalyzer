package weatherAnalyzerPackage;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class MapperOutResults extends Mapper<LongWritable, Text, Text, Text> {
  private Text newKey = new Text();
  private Text newValue = new Text();

  /*
   * Map to STATE
   */   
  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    
    // Assign values and output
    String line = value.toString();
    line = line.substring(line.indexOf("["), line.length());  
    newKey.set(value.toString().substring(0,7));
    newValue.set(line);
    context.write(newKey, newValue);
  }

}

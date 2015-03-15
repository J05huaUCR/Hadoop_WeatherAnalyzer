package weatherAnalyzerPackage;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapStateData extends Mapper<LongWritable, Text, Text, Text> {
  private Text newKey = new Text();
  private Text newValues = new Text();

  /*
   * Map to STATE
   */   
  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

    // Convert value to a String
    String line = value.toString();
    line = line.substring(line.indexOf("["), line.length());    
    
    if (line.length() > 0 && line.substring(0,1).equals("[") ) { // JSON string passed in
      
      // [{"AVGPRCP":17.309917,"STATE":"AK","AVGTEMP":10.797153,"MONTH":1}]
     
      /* Minimal JSON */
      JsonArray minJsonArray = JsonArray.readFrom( line );
      JsonObject minJsonObject = minJsonArray.get(0).asObject();
      
      // Retrieve Values
      String state = minJsonObject.get("STATE").asString();
      if (state.isEmpty() ) {
        state = "XX";
      }
            
      // Assign values and output
      newKey.set(state);
      newValues.set(line); 
      context.write(newKey, newValues);
    }
  }
}

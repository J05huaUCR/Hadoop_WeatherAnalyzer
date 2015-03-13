package weatherAnalyzerPackage;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.io.IOException;

public class MapperOutResults extends Mapper<LongWritable, Text, Text, Text> {
  private Text newKey = new Text();
  private Text newValue = new Text();

  /*
   * Map to STATE
   */   
  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

    System.out.println("MAPPER_RESULTS: key:|" + key.toString() + "|," + value.toString());
    
    // Assign values and output
    String line = value.toString();
    line = line.substring(line.indexOf("["), line.length());  
    String keyBuilder = "A" + key.toString() + ",";
    newKey.set(keyBuilder);
    newValue.set(line);
    
    System.out.println("MAPPER: key|" + keyBuilder + "|, value:|" + line + "|");
    context.write(newKey, newValue);
    
    /* Convert value to a String
    String line = value.toString();
    line = line.substring(line.indexOf("["), line.length());    
    
    if (line.length() > 0 && line.substring(0,1).equals("[") ) { // JSON string passed in
      
      // [{"AVGPRCP":17.309917,"STATE":"AK","AVGTEMP":10.797153,"MONTH":1}]
      
      // Parse into JSON Data
      Object objJSON = JSONValue.parse(line);
      JSONArray jsonData=(JSONArray)objJSON;
      JSONObject obj=(JSONObject)jsonData.get(0);
      
      // Retrieve Values
      String state = (String) obj.get("STATE");
      if (state.isEmpty() ) {
        state = "XX";
      }
            
      // Assign values and output
      newKey.set(key.toString());
      context.write(newKey, value);
    }
  */
  }

}

package weatherAnalyzerPackage;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import java.io.IOException;

public class MapperUSdata extends Mapper<LongWritable, Text, Text, Text> {
  private Text newKey = new Text();
  private Text newValues = new Text();

  /*
   * Map to ID-WBAN-STATE-MONTH
   */   
  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

    // Convert value to a String
    String line = value.toString();
    line = line.substring((line.indexOf(",") + 1), line.length());
    
    //System.out.println("Mapper key: | " + key.toString() + " |, string ("+line.length()+"): | " + line + " |");
    
    
    if (line.length() > 0 && line.substring(0,1).equals("[") ) { // JSON string passed in
      /* Parse into JSON Data*/
      //JSONParser parser =new JSONParser();
      //Object objJSON = parser.parse(line);
      Object objJSON = JSONValue.parse(line);
      JSONArray jsonData=(JSONArray)objJSON;
      JSONObject obj=(JSONObject)jsonData.get(0);
      
      // Retrieve Values
      //String stnID = (String) obj.get("STN");
      //String wban = (String) obj.get("WBAN");
      String state = (String) obj.get("STATE");
      String month = (String) obj.get("YEARMODA");
      
      // Check for state
      if (state.isEmpty()) {
        // no State, skip
        //System.err.println( "NO STATE");
      } else {
        month = month.substring(4,6);
        
        // Output JSON as a string for next pass
        String output = jsonData.toJSONString();
        
        // Assign values and output
        newKey.set(state + "-" + month);
        newValues.set(output); 
        context.write(newKey, newValues);
      }
    }
    
  }
}

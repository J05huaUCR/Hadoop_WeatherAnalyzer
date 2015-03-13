package weatherAnalyzerPackage;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.io.IOException;

public class ReducerUSdata extends Reducer<Text, Text, Text, Text> {

  private Text usData = new Text();
  private StringBuilder builder = new StringBuilder();

  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    System.out.println("REDUCER");
    int count = 0;
    int recordMonth = 0;
    float avgTemp = (float) 0.0;
    String state = ""; // get state from values
    String month = ""; // get month from values
    
    for (Text value : values) {
      
      // Convert value to a String
      String line = value.toString();
      
      // Parse into JSON Data
      Object objJSON = JSONValue.parse(line);
      JSONArray jsonData=(JSONArray)objJSON;
      JSONObject obj=(JSONObject)jsonData.get(0);
      
      // Retrieve Values
      state = (String) obj.get("STATE");
      month = (String) obj.get("YEARMODA");
      month = month.substring(4,6);

      float temp = (float) obj.get("TEMP");
      avgTemp = avgTemp + temp; // add up all values for the month
      
      count++;
    }
    
    // output STATE, MONTH, AVGTEMP
    avgTemp = avgTemp / count; // get average temp for this month
    JSONObject outputData = new JSONObject();
    outputData.put("STATE", state);
    outputData.put("MONTH", recordMonth);
    outputData.put("AVGTEMP", avgTemp);
    String jsonOutput = outputData.toJSONString();
    
    usData.set(jsonOutput);
    context.write(key, usData);

  }

}

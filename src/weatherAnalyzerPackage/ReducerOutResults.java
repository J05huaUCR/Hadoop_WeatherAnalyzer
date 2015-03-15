package weatherAnalyzerPackage;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.io.IOException;

public class ReducerOutResults extends Reducer<Text, Text, Text, Text> {
  private Text newKey = new Text();
  private Text newValues = new Text();
  private StringBuilder outputResultsString = new StringBuilder();

  @SuppressWarnings({ })
  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    
    for (Text value : values) {
      
      // [{"MAX_PRCP":91.900085,"MAX_PRCP_MONTH":11,"STATE":"MN","MAX_TEMP":71.12945,
      // "AVG_TEMP":14.466904,"MIN_TEMP":9.70477,"AVG_PRCP_DIFF":10.390265,"MAX_TEMP_MONTH":7,
      // "AVG_PRCP":91.88434,"MIN_PRCP_MONTH":6,"AVG_TEMP_DIFF":61.42468000000001,"MIN_TEMP_MONTH":2,
      // "MIN_PRCP":81.50982}]|
     // System.out.println("OUTPUT REDUCER: key:|" + key.toString() + "|, value:|"+ value.toString() + "");
      
      
      /*
       * Parse into JSON Data
      */
      
      Object outputDataObj = JSONValue.parse(value.toString());
      JSONArray outputJSONData =(JSONArray)outputDataObj;
      JSONObject outputJSONObj = new JSONObject();
      outputJSONObj=(JSONObject)outputJSONData.get(0);
      //System.err.println("SIZE: |" + outputJSONObj.get(0) + "|");
      
      outputResultsString.setLength(0);
      outputResultsString.append("STATE: " + outputJSONObj.get("STATE") + ",");
      outputResultsString.append("AVG_TEMP: " + outputJSONObj.get("AVG_TEMP") + ",");
      outputResultsString.append("AVG_TEMP_DIFF: " + outputJSONObj.get("AVG_TEMP_DIFF") + ",");
      outputResultsString.append("MAX_TEMP: " + outputJSONObj.get("MAX_TEMP") + ",");
      outputResultsString.append("MAX_TEMP_MONTH: " + outputJSONObj.get("MAX_TEMP_MONTH") + ",");
      outputResultsString.append("MIN_TEMP: " + outputJSONObj.get("MIN_TEMP") + ",");
      outputResultsString.append("MIN_TEMP_MONTH: " + outputJSONObj.get("MIN_TEMP_MONTH") + ",");
      outputResultsString.append("AVG_PRCP_DIFF: " + outputJSONObj.get("AVG_PRCP_DIFF") + ",");
      outputResultsString.append("AVG_PRCP: " + outputJSONObj.get("AVG_PRCP") + ",");
      outputResultsString.append("MAX_PRCP: " + outputJSONObj.get("MAX_PRCP") + ",");
      outputResultsString.append("MAX_PRCP_MONTH: " + outputJSONObj.get("MAX_PRCP_MONTH") + ",");
      outputResultsString.append("MIN_PRCP" + outputJSONObj.get("MIN_PRCP") + ",");
      outputResultsString.append("MIN_PRCP_MONTH: " + outputJSONObj.get("MIN_PRCP_MONTH"));

      //System.out.println("VALUES: " + outputResultsString.toString());
      newKey.set(key.toString());
      newValues.set(outputResultsString.toString());

      //stateDate.set(jsonArray.toJSONString());
      context.write(newKey, newValues);
    }
    
  }
}

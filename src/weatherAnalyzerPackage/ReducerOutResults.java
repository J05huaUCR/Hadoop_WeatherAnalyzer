package weatherAnalyzerPackage;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

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

      /*
       * Parse into JSON Data
      */    
      JsonArray minJsonArray = JsonArray.readFrom( value.toString() );
      JsonObject minJsonObject = minJsonArray.get(0).asObject();
      
      outputResultsString.setLength(0);
      outputResultsString.append("STATE: " + minJsonObject.get("STATE") + ",");
      outputResultsString.append("AVG_TEMP: " + minJsonObject.get("AVG_TEMP") + ",");
      outputResultsString.append("AVG_TEMP_DIFF: " + minJsonObject.get("AVG_TEMP_DIFF") + ",");
      outputResultsString.append("MAX_TEMP: " + minJsonObject.get("MAX_TEMP") + ",");
      outputResultsString.append("MAX_TEMP_MONTH: " + minJsonObject.get("MAX_TEMP_MONTH") + ",");
      outputResultsString.append("MIN_TEMP: " + minJsonObject.get("MIN_TEMP") + ",");
      outputResultsString.append("MIN_TEMP_MONTH: " + minJsonObject.get("MIN_TEMP_MONTH") + ",");
      outputResultsString.append("AVG_PRCP_DIFF: " + minJsonObject.get("AVG_PRCP_DIFF") + ",");
      outputResultsString.append("AVG_PRCP: " + minJsonObject.get("AVG_PRCP") + ",");
      outputResultsString.append("MAX_PRCP: " + minJsonObject.get("MAX_PRCP") + ",");
      outputResultsString.append("MAX_PRCP_MONTH: " + minJsonObject.get("MAX_PRCP_MONTH") + ",");
      outputResultsString.append("MIN_PRCP" + minJsonObject.get("MIN_PRCP") + ",");
      outputResultsString.append("MIN_PRCP_MONTH: " + minJsonObject.get("MIN_PRCP_MONTH"));

      //System.out.println("VALUES: " + outputResultsString.toString());
      newKey.set(key.toString());
      newValues.set(outputResultsString.toString());

      //stateDate.set(jsonArray.toJSONString());
      context.write(newKey, newValues);
    }
    
  }
}

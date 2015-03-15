package weatherAnalyzerPackage;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class ReduceStateData extends Reducer<Text, Text, Text, Text> {
  private Text newKey = new Text();
  private Text stateDate = new Text();

  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {

    double maxTemp = (double) 0.0;
    double minTemp = (double) 9999.0;
    double avgTemp = (double) 0.0;
    double tempDiff = (double) 0.0;
    double maxPrcp = (double) 0.0;
    double minPrcp = (double) 9999.0;
    double avgPrcp = (double) 0.0;
    double prcpDiff = (double) 0.0;
    int maxTempMonth = 0;
    int minTempMonth = 0;
    int maxPrcpMonth = 0;
    int minPrcpMonth = 0;   
    
    String state = ""; // get state from values
    int month = 0; // get month from values
    
    for (Text value : values) {
      
      // [{"AVGPRCP":17.309917,"STATE":"AK","AVGTEMP":10.797153,"MONTH":1}]
      
      /* Minimal JSON */
      JsonArray minJsonArray = JsonArray.readFrom( value.toString() );
      JsonObject minJsonObject = minJsonArray.get(0).asObject();
      
      // Retrieve Values
      long l = minJsonObject.get("MONTH").asLong();
      month = (int) l;
      
      if (month > 0) {
        
        // Get State
        state = minJsonObject.get("STATE").asString();      
        
        try {
          //avgTemp = Double.parseDouble(obj.get("AVGTEMP").toString());
          avgTemp = minJsonObject.get("AVGTEMP").asDouble();
        } catch (Exception e) {
          // 
        }
       
        try {
          //avgPrcp = Double.parseDouble(obj.get("AVGPRCP").toString());
          //avgPrcp = Double.parseDouble(minJsonObject.get("AVGPRCP").asString());
          avgPrcp = minJsonObject.get("AVGPRCP").asDouble();
        } catch (Exception e) {
          //
        }
       
        // Check for max temp
        if (avgTemp > maxTemp) {
          maxTemp = avgTemp;
          maxTempMonth = month;
        }
        
        // Check for min temp
        if ( minTemp > avgTemp) {
          minTemp = avgTemp;
          minTempMonth = month;
        }
        
        // Check for max precipitation
        if (avgPrcp > maxPrcp) {
          maxPrcp = avgPrcp;
          maxPrcpMonth = month;
        }
        
        // Check for min precipitation
        if ( minPrcp > avgPrcp) {
          minPrcp = avgPrcp;
          minPrcpMonth = month;
        } 
      }     
    }
    
    if (month > 0 && !state.equals("XX") ) {
      // output STATE DATA to JSON
      tempDiff = maxTemp - minTemp;
      prcpDiff = maxPrcp - minPrcp; 
      double roundOff = (double) Math.round(tempDiff * 1000) / 1000;
      roundOff = Math.round(roundOff * 1000);
      roundOff = Math.round(roundOff) + 1000000;
      
      String buildKey = roundOff + "";
      buildKey = buildKey.substring(0, (buildKey.length() - 2));
      newKey.set( buildKey + "," );

      /* Simple JSON */
      JsonObject outputData = new JsonObject();
      outputData.add("STATE", state);
      outputData.add("MAX_TEMP", maxTemp);
      outputData.add("MAX_TEMP_MONTH", maxTempMonth);
      outputData.add("MIN_TEMP", minTemp);
      outputData.add("MIN_TEMP_MONTH", minTempMonth);
      outputData.add("AVG_TEMP", avgTemp);
      outputData.add("AVG_TEMP_DIFF", tempDiff);
      outputData.add("MAX_PRCP", maxPrcp);
      outputData.add("MAX_PRCP_MONTH", maxPrcpMonth);
      outputData.add("MIN_PRCP", minPrcp);
      outputData.add("MIN_PRCP_MONTH", minPrcpMonth);
      outputData.add("AVG_PRCP", avgPrcp);
      outputData.add("AVG_PRCP_DIFF", prcpDiff);
      JsonArray jsonArray = new JsonArray();
      jsonArray.add(outputData);
      
      System.out.println("JSON OUTPUT: " + buildKey + ":" + jsonArray.toString());
      
      stateDate.set(jsonArray.toString());
      context.write(newKey, stateDate);
    }
  }
}

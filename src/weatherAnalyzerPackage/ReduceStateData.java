package weatherAnalyzerPackage;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.io.IOException;

public class ReduceStateData extends Reducer<Text, Text, Text, Text> {
  private Text newKey = new Text();
  private Text stateDate = new Text();

  @SuppressWarnings({ "unchecked" })
  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

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
      //System.out.println("STATE INPUT: " + value.toString());
      
      // Parse into JSON Data
      Object objJSON = JSONValue.parse(value.toString());
      JSONArray jsonData=(JSONArray)objJSON;
      JSONObject obj=(JSONObject)jsonData.get(0);
      //System.out.println("STATE INPUT:" + jsonData.toString());
      
      // Retrieve Values
      long l = (Long) obj.get("MONTH");
      month = (int) l;
      
      if (month > 0) {
        state = (String) obj.get("STATE");
        //avgTemp = (double) obj.get("AVGTEMP"); // JDK 1.7
        //avgPrcp = (double) obj.get("AVGPRCP"); // JDK 1.7
        
        try {
          avgTemp = Double.parseDouble(obj.get("AVGTEMP").toString());
        } catch (Exception e) {
          // 
        }
       
        try {
          avgPrcp = Double.parseDouble(obj.get("AVGPRCP").toString());
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
    
    if (month > 1 && !state.equals("XX") ) {
      // output STATE DATA to JSON
      tempDiff = maxTemp - minTemp;
      prcpDiff = maxPrcp - minPrcp; 
      double roundOff = (double) Math.round(tempDiff * 1000) / 1000;
      roundOff = Math.round(roundOff * 1000);
      roundOff = Math.round(roundOff) + 1000000;
      
      String buildKey = roundOff + "";
      buildKey = buildKey.substring(0, (buildKey.length() - 2));
      newKey.set( buildKey + "," );
    
      JSONObject outputData = new JSONObject();
      outputData.put("STATE", state);
      outputData.put("MAX_TEMP", maxTemp);
      outputData.put("MAX_TEMP_MONTH", maxTempMonth);
      outputData.put("MIN_TEMP", minTemp);
      outputData.put("MIN_TEMP_MONTH", minTempMonth);
      outputData.put("AVG_TEMP", avgTemp);
      outputData.put("AVG_TEMP_DIFF", tempDiff);
      outputData.put("MAX_PRCP", maxPrcp);
      outputData.put("MAX_PRCP_MONTH", maxPrcpMonth);
      outputData.put("MIN_PRCP", minPrcp);
      outputData.put("MIN_PRCP_MONTH", minPrcpMonth);
      outputData.put("AVG_PRCP", avgPrcp);
      outputData.put("AVG_PRCP_DIFF", prcpDiff);
      JSONArray jsonArray = new JSONArray();
      jsonArray.add(outputData);
      
      stateDate.set(jsonArray.toJSONString());
      context.write(newKey, stateDate);
    }
  }
}

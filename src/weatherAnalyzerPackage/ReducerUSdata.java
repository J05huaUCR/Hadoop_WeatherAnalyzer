package weatherAnalyzerPackage;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;*/


import java.io.IOException;

public class ReducerUSdata extends Reducer<Text, Text, Text, Text> {
  private Text newKey = new Text();
  private Text usData = new Text();

  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

    int recordMonth = 0;
    int countTemp = 0;
    int countPrcp = 0;
    float runningTemp = (float) 0.0;
    float runningPrcp = (float) 0.0;
    float avgTemp = (float) 0.0;
    float avgPrcp = (float) 0.0;
    float maxTemp = (float) 0.0;
    float minTemp = (float) 9999.9;
    float maxPrcp = (float) 0.0;
    float minPrcp = (float) 9999.9;
    String maxTempDate = "XX";
    String minTempDate = "XX";
    String maxPrcpDate = "XX";
    String minPrcpDate = "XX";
    String state = ""; // get state from values
    String month = ""; // get month from values
    
    for (Text value : values) {
      
      // Parse into JSON Data
      JsonArray minJsonArray = JsonArray.readFrom( value.toString() );
      JsonObject minJsonObject = minJsonArray.get(0).asObject();
      
      // Retrieve Values
      state = minJsonObject.get("STATE").asString();
      if (state.isEmpty() || state.equals("STATE")) {
        state = "XX";
      } 
      
      //month = (String) obj.get("YEARMODA");
      month = minJsonObject.get("YEARMODA").asString();
      if (!month.isEmpty()) {
        month = month.substring(4,6);
        recordMonth = Integer.parseInt(month);
      } else {
        month = "00";
      }
      
      newKey.set(state + "-" + month);
      
      // Get temps
      String tempString = minJsonObject.get("TEMP").asString();
      if (tempString != null && !tempString.isEmpty()) {
        float temp = Float.parseFloat( minJsonObject.get("TEMP").asString() );
        runningTemp = runningTemp + temp; // add up all values for the month
        if (temp > maxTemp) { // new maxmimum temp
          maxTemp = temp;
          maxTempDate = minJsonObject.get("YEARMODA").asString();
        }
        
        if (temp < minTemp) { // new minimum temp
          minTemp = temp;
          minTempDate = minJsonObject.get("YEARMODA").asString();
        }
        countTemp++;
      }   
      
      /* Get PRCP, DEAL WITH LETTERS
       * 
       *  A - 6 hours worth of precipitation
       *    B - 12 hours...
       *    C - 18 hours...
       *    D - 24 hours...
       *    E - 12 hours... (slightly different from B but the same for this project).
       *    F - 24 hours ... (slightly different from D but the same for this project).
       *    G - 24 hours ... (slightly different from D but the same for this project).
       *    H - station recorded a 0 for the day (although there was some recorded instance of precipitation).
       *    I - station recorded a 0 for the day (and there was NO recorded instance of precipitation).
       */
      String prcpString = minJsonObject.get("PRCP").asString();
      if (!prcpString.isEmpty()) {
        
        String flagPrcp = prcpString.substring((prcpString.length() - 1), prcpString.length());
        String tempPrcp = prcpString.substring(0,(prcpString.length() - 2));
        
        float prcp = Float.parseFloat( tempPrcp );
        if (flagPrcp.equals("A")) {
          prcp = prcp * 4;
        } else if (flagPrcp.equals("B") || flagPrcp.equals("E") ) {
          prcp = prcp * 2;
        } else if (flagPrcp.equals("C") ) {
          prcp = prcp * (float)1.33;
        } else if (flagPrcp.equals("D") || flagPrcp.equals("F") || flagPrcp.equals("G") || flagPrcp.equals("G")) {
          flagPrcp.equals("G");
        } else {
          prcp = (float) 9999.0;
        }

        if (prcp < 9000) {
          runningPrcp = runningPrcp + prcp; // add up all values for the month
          countPrcp++;
          
          if (prcp > maxPrcp) { // new maxmimum precipitation
            maxPrcp = prcp;
            maxPrcpDate = minJsonObject.get("YEARMODA").asString();
          }
          
          if (prcp < minPrcp) { // new minimum precipitation
            minPrcp = prcp;
            minPrcpDate = minJsonObject.get("YEARMODA").asString();
          }
        }
      } 
    }
    
    // output STATE, MONTH, AVGTEMPm AVGPRCP
    avgTemp = runningTemp / countTemp; // get average temp for this month
    if (countPrcp > 0) {
      avgPrcp = runningPrcp / countPrcp; // get average temp for this month
    } else {
      avgPrcp = (float) 0.0;
    }
    
    /* Minimal JSON */
    JsonObject outputData = new JsonObject();
    outputData.add("STATE", state);
    outputData.add("MONTH", recordMonth);
    outputData.add("AVGTEMP", avgTemp);
    outputData.add("MAXTEMP", maxTemp);
    outputData.add("MAXTEMP_DATE", maxTempDate);
    outputData.add("MINTEMP", minTemp);
    outputData.add("MINTEMP_DATE", minTempDate);
    outputData.add("AVGPRCP", avgPrcp);
    outputData.add("MAXPRCP", maxPrcp);
    outputData.add("MAXPRCP_DATE", maxPrcpDate);
    outputData.add("MINPRCP", minPrcp);
    outputData.add("MINPRCP_DATE", minPrcpDate);
    JsonArray jsonArray = new JsonArray();
    jsonArray.add(outputData);
    
    usData.set(jsonArray.toString());
    context.write(newKey, usData);
  }
}

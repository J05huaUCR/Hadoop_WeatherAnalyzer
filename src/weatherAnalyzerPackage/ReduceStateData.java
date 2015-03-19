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
    
    /* [{"AVGPRCP":17.309917,"STATE":"AK","AVGTEMP":10.797153,"MONTH":1}]
     * INPUT: 
     * "STATE":"AK","MONTH":1,
     * "AVGTEMP":6.099615,"MAXTEMP":52.1,"MAXTEMP_DATE":"20090118",
     * "MINTEMP":-57.2,"MINTEMP_DATE":"20090108",
     * "AVGPRCP":0.19740666,"MAXPRCP":6.4,"MAXPRCP_DATE":"20090119",
     * "MINPRCP":0,"MINPRCP_DATE":"20090131"}
     */

    double avgTemp = (double) 0.0;
    double maxTemp = (double) 0.0;
    double minTemp = (double) 9999.0;
    double maxRecordTemp = (double) 0.0;
    double minRecordTemp = (double) 9999.0;
    double tempDiff = (double) 0.0;
    double maxPrcp = (double) 0.0;
    double minPrcp = (double) 9999.0;
    double avgPrcp = (double) 0.0;
    double maxRecordPrcp = (double) 0.0;
    double minRecordPrcp = (double) 9999.0;
    double prcpDiff = (double) 0.0;
    int maxTempMonth = 0;
    int minTempMonth = 0;
    int maxPrcpMonth = 0;
    int minPrcpMonth = 0;   
    
    Double parseMaxRecordTemp = 0.0;
    Double parseMinRecordTemp = 9999.9;
    String parseMaxRecordTempDate = "XX";
    String parseMinRecordTempDate = "XX";

    Double parseMaxRecordPrcp = 0.0;
    Double parseMinRecordPrcp = 9999.9;
    String parseMaxRecordPrcpDate = "XX";
    String parseMinRecordPrcpDate = "XX";
    
    String maxRecordTempDate = "XX";
    String minRecordTempDate = "XX";
    String maxRecordPrcpDate = "XX";
    String minRecordPrcpDate = "XX";
    
    String state = ""; // get state from values
    int month = 0; // get month from values
    
    for (Text value : values) {
           
      JsonArray minJsonArray = JsonArray.readFrom( value.toString() );
      JsonObject minJsonObject = minJsonArray.get(0).asObject();
      
      // Retrieve Values
      long l = minJsonObject.get("MONTH").asLong();
      month = (int) l;
      
      state = minJsonObject.get("STATE").asString();   
      
      if (month > 0) {
        
        // Get State
        try {
          avgTemp = minJsonObject.get("AVGTEMP").asDouble();
          parseMaxRecordTemp = minJsonObject.get("MAXTEMP").asDouble();
          parseMinRecordTemp = minJsonObject.get("MINTEMP").asDouble();
          parseMaxRecordTempDate = minJsonObject.get("MAXTEMP_DATE").asString();
          parseMinRecordTempDate = minJsonObject.get("MINTEMP_DATE").asString();
          
          avgPrcp = minJsonObject.get("AVGPRCP").asDouble();
          parseMaxRecordPrcp = minJsonObject.get("MAXPRCP").asDouble();
          parseMinRecordPrcp = minJsonObject.get("MINPRCP").asDouble();
          parseMaxRecordPrcpDate = minJsonObject.get("MAXPRCP_DATE").asString();
          parseMinRecordPrcpDate = minJsonObject.get("MINPRCP_DATE").asString();

        } catch (Exception e) {
          System.out.println("STATE " + state + " had errors.....................");
        }
        
        // Check for max avg temp
        if (avgTemp > maxTemp) {
          maxTemp = avgTemp;
          maxTempMonth = month;
        }
        
        // Check for min avg temp
        if ( avgTemp < minTemp ) {
          minTemp = parseMinRecordTemp;
          minTempMonth = month;
        }
        
        // Check for max record temp
        if (parseMaxRecordTemp > maxRecordTemp) {
          maxRecordTemp = parseMaxRecordTemp;
          maxRecordTempDate = parseMaxRecordTempDate;
        }
        
        // Check for min record temp
        if ( parseMinRecordTemp < minTemp ) {
          minRecordTemp = avgTemp;
          minRecordTempDate = parseMinRecordTempDate;
        }
        
        // Check for max avg precipitation
        if (avgPrcp > maxPrcp) {
          maxPrcp = avgPrcp;
          maxPrcpMonth = month;
        }
        
        // Check for min avg precipitation
        if ( avgPrcp < minPrcp) {
          minPrcp = avgPrcp;
          minPrcpMonth = month;
        } 
        
        // Check for max record temp
        if (parseMaxRecordPrcp > maxRecordPrcp) {
          maxRecordPrcp = parseMaxRecordPrcp;
          maxRecordPrcpDate = parseMaxRecordPrcpDate;
        }
        
        // Check for max record temp
        if (parseMinRecordPrcp < minRecordPrcp) {
          minRecordPrcp = parseMinRecordPrcp;
          minRecordPrcpDate = parseMinRecordPrcpDate;
        }
      }     
    } // End Parsing Months
    
    
    
    if (!state.equals("XX") ) {
      
      // output STATE DATA to JSON
      tempDiff = maxTemp - minTemp;
      prcpDiff = maxPrcp - minPrcp; 
      double roundOff = (double) Math.round(tempDiff * 1000) / 1000;
      roundOff = Math.round(roundOff * 1000);
      roundOff = Math.round(roundOff) + 1000000;
      
      String buildKey = roundOff + "";
      buildKey = buildKey.substring(0, (buildKey.length() - 2));
      newKey.set( buildKey + "," );

      JsonObject outputData = new JsonObject();
      outputData.add("STATE", state);
      outputData.add("AVG_TEMP", avgTemp);
      outputData.add("AVG_TEMP_DIFF", tempDiff);
      outputData.add("MAX_TEMP", maxTemp);
      outputData.add("MAX_TEMP_MONTH", maxTempMonth);
      outputData.add("MAX_RECORD_TEMP", maxRecordTemp);
      outputData.add("MAX_RECORD_TEMP_DATE", maxRecordTempDate);
      outputData.add("MIN_TEMP", minTemp);
      outputData.add("MIN_TEMP_MONTH", minTempMonth);
      outputData.add("MIN_RECORD_TEMP", minRecordTemp);
      outputData.add("MIN_RECORD_TEMP_DATE", minRecordTempDate);
      outputData.add("AVG_PRCP", avgPrcp);
      outputData.add("AVG_PRCP_DIFF", prcpDiff);
      outputData.add("MAX_PRCP", maxPrcp);
      outputData.add("MAX_PRCP_MONTH", maxPrcpMonth);
      outputData.add("MAX_RECORD_PRCP", maxRecordPrcp);
      outputData.add("MAX_RECORD_PRCP_DATE", maxRecordPrcpDate);
      outputData.add("MIN_PRCP", minPrcp);
      outputData.add("MIN_PRCP_MONTH", minPrcpMonth);
      outputData.add("MIN_RECORD_PRCP", minRecordPrcp);
      outputData.add("MIN_RECORD_PRCP_DATE", minRecordPrcpDate);

      JsonArray jsonArray = new JsonArray();
      jsonArray.add(outputData);
      
      stateDate.set(jsonArray.toString());
      context.write(newKey, stateDate);
    }
  }
}

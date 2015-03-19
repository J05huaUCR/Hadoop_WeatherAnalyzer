package weatherAnalyzerPackage;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue; */

import java.io.IOException;

public class MapperUSdata extends Mapper<LongWritable, Text, Text, Text> {
  private Text newKey = new Text();
  private Text newValues = new Text();

  /*
   * Map to STATE-MONTH
   */   
  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

    /* Incoming string
     * 007005-99999,[{
     * "STATION NAME":"CWOS 07005",
     * "CTRY":"",
     * "STATE":"",
     * "LAT":"",
     * "LON":"",
     * "ELEV":"",
     * "BEGIN":"20120127",
     * "END":"20120127",
     * "YEARMODA":"20000001",
     * "TEMP":"9999",
     * "MAX":"9999",
     * "MIN":"9999",
     * "PRCP":"9999I"
     * }]
     */
    // Convert value to a String
    String line = value.toString();
    line = line.substring((line.indexOf(",") + 1), line.length());    
    
    if (line.length() > 0 && line.substring(0,1).equals("[") ) { // JSON string passed in
           
      /* Parse into JSON Data*/
      JsonArray minJsonArray = JsonArray.readFrom( line );
      JsonObject minJsonObject = minJsonArray.get(0).asObject();
      
      // Retrieve Values
      String country = minJsonObject.get("CTRY").asString();
      if (country.isEmpty() ) {
        country = "XX";
        minJsonObject.set("CTRY",country);
      }
      
      String state = minJsonObject.get("STATE").asString();
      if (state.isEmpty() ) {
        state = "XX";
        minJsonObject.set("STATE",state);
      }
      
      String latitude = minJsonObject.get("LAT").asString();
      if (latitude.isEmpty()) {
        latitude = "9999.9";
        minJsonObject.set("LAT",latitude);
      } 
      
      String longitude = minJsonObject.get("LON").asString();
      if (longitude.isEmpty()) {
        longitude = "9999.9";
        minJsonObject.set("LON",longitude);
      }

      String month = minJsonObject.get("YEARMODA").asString();
      if (!month.isEmpty()) {
        month = month.substring(4,6);
      } else {
        month = "00";
      }
      
      // Test for result we're interested in
      if (country.equals("US") && !state.equals("XX")){ // a US with a valid state
        
        // Assign values and output
        newKey.set(state + "-" + month);
        minJsonArray.set(0,minJsonObject); // update Json array
        newValues.set(minJsonArray.toString()); 
        context.write(newKey, newValues);

      } else {
        StationLocater findStation = new StationLocater();
        if (findStation.find(latitude, longitude) > 0) {
          
          // Station found, update Json with values found
          minJsonObject.set("CTRY", findStation.getCountry());
          minJsonObject.set("STATE", findStation.getState());
          
          // US, CQ, GQ, RM, WQ, UM, FM,PS,KT
          if (findStation.getCountry().equals("US") || 
              findStation.getCountry().equals("CQ") || 
              findStation.getCountry().equals("GQ") || 
              findStation.getCountry().equals("RM") || 
              findStation.getCountry().equals("WQ") || 
              findStation.getCountry().equals("UM") || 
              findStation.getCountry().equals("FM") || 
              findStation.getCountry().equals("PS") || 
              findStation.getCountry().equals("KT") ) {
            // Assign values and output
            newKey.set(state + "-" + month);
            minJsonArray.set(0,minJsonObject); // update Json array
            newValues.set(minJsonArray.toString()); 
            context.write(newKey, newValues);
          }
        }
      }
    }
  }
}

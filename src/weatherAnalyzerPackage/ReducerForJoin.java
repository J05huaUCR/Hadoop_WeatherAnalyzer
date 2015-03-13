package weatherAnalyzerPackage;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Reducer.Context;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.io.IOException;

public class ReducerForJoin extends Reducer<AnchorKey, Text, NullWritable, Text> {

  private Text joinedText = new Text();
  private NullWritable nullKey = NullWritable.get();

  @SuppressWarnings("unchecked")
  @Override
  protected void reduce(AnchorKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    
      String keyPassedIn = key.getJoinKey().toString();
      
      // Instantiate a default JSON data object to write to output
      String outputDataInfo = "[{\"STATION NAME\":\"\",\"CTRY\":\"\",\"STATE\":\"\",\"LAT\":\"\",\"LON\":\"\",\"ELEV\":\"\",\"BEGIN\":\"\",\"END\":\"\",\"YEARMODA\":\"\",\"TEMP\":\"\",\"MAX\":\"\",\"MIN\":\"\",\"PRCP\":\"\"}]";
      Object outputDataObj = JSONValue.parse(outputDataInfo);
      JSONArray outputJSONData =(JSONArray)outputDataObj;
      JSONObject outputJSONObj = new JSONObject();
      outputJSONObj=(JSONObject)outputJSONData.get(0);
      
      /*
       * STATION NAME,CTRY,STATE,LAT,LON,ELEV,BEGIN,END,YEARMODA,TEMP,MAX,MIN,PRCP
       */
      for (Text value : values) {

        // Create Temporary JSON
        Object tempParseObj = JSONValue.parse( value.toString() );
        JSONArray tempValuesArray=(JSONArray)tempParseObj;
        JSONObject tempValueObj = new JSONObject();
        tempValueObj=(JSONObject)tempValuesArray.get(0);
        
        if (tempValueObj.size() > 4) { // Station
          // update station data in output object
          outputJSONObj.put("STATION NAME", tempValueObj.get("STATION NAME"));
          outputJSONObj.put("CTRY", tempValueObj.get("CTRY"));
          outputJSONObj.put("STATE", tempValueObj.get("STATE"));
          outputJSONObj.put("LAT", tempValueObj.get("LAT"));
          outputJSONObj.put("LON", tempValueObj.get("LON"));
          outputJSONObj.put("ELEV", tempValueObj.get("ELEV"));
          outputJSONObj.put("BEGIN", tempValueObj.get("BEGIN"));
          outputJSONObj.put("END", tempValueObj.get("END"));
        } else { // readings
          // Update readings in output object
          outputJSONObj.put("YEARMODA", tempValueObj.get("YEARMODA"));
          outputJSONObj.put("TEMP", tempValueObj.get("TEMP"));
          outputJSONObj.put("MAX", tempValueObj.get("MAX"));
          outputJSONObj.put("MIN", tempValueObj.get("MIN"));
          outputJSONObj.put("PRCP", tempValueObj.get("PRCP"));
        }
        
        // Emit results
        joinedText.set(keyPassedIn + "," + outputJSONObj.toString());
        context.write(nullKey, joinedText);
     }
  }
}

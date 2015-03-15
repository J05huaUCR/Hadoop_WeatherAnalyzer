package weatherAnalyzerPackage;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class ReducerForJoin extends Reducer<AnchorKey, Text, NullWritable, Text> {

  private Text joinedText = new Text();
  private NullWritable nullKey = NullWritable.get();

  @Override
  protected void reduce(AnchorKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    
      String keyPassedIn = key.getJoinKey().toString();
      
      // Instantiate a default JSON data object to write to output
      String outputDataInfo = "[{\"STATION NAME\":\"MISSING\",\"CTRY\":\"XX\",\"STATE\":\"XX\",\"LAT\":\"9999\",\"LON\":\"9999\",\"ELEV\":\"9999\",\"BEGIN\":\"20000001\",\"END\":\"20000001\",\"YEARMODA\":\"20000001\",\"TEMP\":\"9999\",\"MAX\":\"9999\",\"MIN\":\"9999\",\"PRCP\":\"9999I\"}]";
      
      /* Minimal JSON */
      JsonArray outputJSONData = JsonArray.readFrom( outputDataInfo );
      JsonObject outputJSONObj = outputJSONData.get(0).asObject();
      
      /*
       * STATION NAME,CTRY,STATE,LAT,LON,ELEV,BEGIN,END,YEARMODA,TEMP,MAX,MIN,PRCP
       */
      for (Text value : values) {
        
        /* Create Temporary JSON Minimal JSON */
        JsonArray tempValuesArray = JsonArray.readFrom( value.toString() );
        JsonObject tempValueObj = tempValuesArray.get(0).asObject();
       
        
        if (tempValueObj.size() > 5) { // Station
          // update station data in output object
          
          /* Minimal JSON */
          outputJSONObj.set("STATION NAME", tempValueObj.get("STATION NAME"));
          outputJSONObj.set("CTRY", tempValueObj.get("CTRY"));
          outputJSONObj.set("STATE", tempValueObj.get("STATE"));
          outputJSONObj.set("LAT", tempValueObj.get("LAT"));
          outputJSONObj.set("LON", tempValueObj.get("LON"));
          outputJSONObj.set("ELEV", tempValueObj.get("ELEV"));
          outputJSONObj.set("BEGIN", tempValueObj.get("BEGIN"));
          outputJSONObj.set("END", tempValueObj.get("END"));
          
          //System.out.println("UPDATED STATION:" + outputJSONObj.toString());
        } else { // readings
          // Update readings in output object
          
          /* Minimal JSON */
          outputJSONObj.set("YEARMODA", tempValueObj.get("YEARMODA"));
          outputJSONObj.set("TEMP", tempValueObj.get("TEMP"));
          outputJSONObj.set("MAX", tempValueObj.get("MAX"));
          outputJSONObj.set("MIN", tempValueObj.get("MIN"));
          outputJSONObj.set("PRCP", tempValueObj.get("PRCP"));
          
          //System.out.println("UPDATED RECORD:" + outputJSONObj.toString());
        }
        
        // Emit results
        outputJSONData.set(0, outputJSONObj);
       // System.out.println("RECORD OUTPUT:" + outputJSONObj.toString());
        joinedText.set(keyPassedIn + "," + outputJSONData.toString());
        context.write(nullKey, joinedText);
        
     }
  }
}

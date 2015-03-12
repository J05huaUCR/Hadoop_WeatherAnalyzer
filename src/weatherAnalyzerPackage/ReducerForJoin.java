package weatherAnalyzerPackage;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Reducer.Context;

import java.io.IOException;

public class ReducerForJoin extends Reducer<AnchorKey, Text, NullWritable, Text> {

  private Text joinedText = new Text();
  private StringBuilder builder = new StringBuilder();
  private NullWritable nullKey = NullWritable.get();

  @Override
  protected void reduce(AnchorKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      
      int count = 0;
      String keyPassedIn = key.getJoinKey().toString();
      String defaultStationInfo = "{\"STATION NAME\":\"\",\"CTRY\":\"\",\"STATE\":\"\",\"LAT\":\"\",\"LON\":\"\",\"ELEV\":\"\",\"BEGIN\":\"\",\"END\":\"\"}";
      String defaultStationReadings = "{\"YEARMODA\":\"\",\"TEMP\":\"\",\"MAX\":\"\",\"MIN\":\"\"}"; 
      String stationInfo = "";
      String tempValue = "";
      
      for (Text value : values) {
        
        tempValue = value.toString();
        
        if (count == 0) { // reading first value
          
          // First entry should be station info, {"S
          // if not, station record.
          if (tempValue.substring(2,3).equals("S")) {
            // This value is station info, append blank readings for now, will 
            // filter on next pass
            stationInfo = tempValue;
            builder.append(keyPassedIn + "," + tempValue);
            
            // backup 1 char to kill "}" and end if station
            builder.setLength(builder.length()-1); 
            
            // add default reading
            builder.append("," + defaultStationReadings.substring(1, defaultStationReadings.length())); 
            
            
          } else {
            // this value is readings data, set station to default, append reading
            // from here out, using default (blank) blank station data
            stationInfo = defaultStationInfo;
            builder.append(keyPassedIn + "," + defaultStationInfo);

            builder.setLength(builder.length()-1); // backup 1 char to kill "}" and end if station
            builder.append("," + tempValue.substring(1, tempValue.length())); // add reading
          }
          
          // Emit results
          joinedText.set(builder.toString());
          context.write(nullKey, joinedText);
          builder.setLength(0);
          count = 1;
        } else { // reading other values
          
          // write out station plus record, {"Y
          builder.append(keyPassedIn + ",");
          builder.append(stationInfo);
          builder.setLength(builder.length()-1); // backup 1 char to kill "}" and end if station
          builder.append("," + tempValue.substring(1, tempValue.length())); // add reading
          
          joinedText.set(builder.toString());
          context.write(nullKey, joinedText);
          builder.setLength(0);
        }
     }
  }
}

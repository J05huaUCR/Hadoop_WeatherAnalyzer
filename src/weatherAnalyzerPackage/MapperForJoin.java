package weatherAnalyzerPackage;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.IOException;
import java.util.StringTokenizer;

public class MapperForJoin extends Mapper<LongWritable, Text, AnchorKey, Text> {
  private AnchorKey taggedKey = new AnchorKey();
  private Text data = new Text();
  private int joinOrder;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
      FileSplit fileSplit = (FileSplit)context.getInputSplit();
      joinOrder = Integer.parseInt(context.getConfiguration().get(fileSplit.getPath().getName()));
  }

  @SuppressWarnings("unchecked")
  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String tempKey = "";
    String line = value.toString();
    String flag = line.substring(0, 1);
    JSONObject jsonData = new JSONObject(); // Create JSON object to hold data
    
    /* Check for CSV */
    if (flag.equals("\"")) {
      // HANDLING STATION INFO
      // USAF (0) WBAN (1) STATION NAME (2) CTRY(3) STATE (4) LAT (5) LON
      // (6) ELEV(M) (7) BEGIN (8) END (9) .

      StringTokenizer st = new StringTokenizer(line, ",");// use comma as token separator
      int tokenNumber = 0;

      /* Put values into JSON object */
      while (st.hasMoreTokens()) {
        
        String tokenString = st.nextToken().toString();
        tokenString = tokenString.replaceAll("\"", ""); // Strip Quotes
        tokenString = tokenString.replaceAll(",", "-"); // strip comma if present and replace with dash
        tokenString = tokenString.replaceAll("\\+", ""); // strip plus sign if present 
        
        /* Output JSON format value */
        switch (tokenNumber) {
          case 0: // Key
            tempKey += tokenString.replaceAll("\"", ""); // Strip Quotes
            break;

          case 1:
            tempKey += "-" + tokenString.replaceAll("\"", ""); // Strip Quotes
            break;

          case 2:
            jsonData.put("STATION NAME", tokenString.replaceAll("\"", ""));
            break;

          case 3:
            jsonData.put("CTRY", tokenString.replaceAll("\"", ""));
            break;

          case 4:
            jsonData.put("STATE", tokenString.replaceAll("\"", ""));
            break;

          case 5:
            jsonData.put("LAT", tokenString.replaceAll("\"", ""));
            break;

          case 6:
            jsonData.put("LON", tokenString.replaceAll("\"", ""));
            break;

          case 7:
            jsonData.put("ELEV", tokenString.replaceAll("\"", ""));
            break;

          case 8:
            jsonData.put("BEGIN", tokenString.replaceAll("\"", ""));
            break;

          case 9:
            jsonData.put("END", tokenString.replaceAll("\"", ""));
            break;

        } // End Switch
        tokenNumber++;
      }
    } else  { // TXT
      
      /*  [ 0] STN---  = The station ID (USAF)
       *  [ 1] WBAN   = NCDC WBAN number
       *  [ 2] YEARMODA   = The datestamp
       *  [ 3] TEMP = The average temperature for the day, followed by the number of recordings
       *  [ 4] DEWP = Ignore for this project
       *  [ 5] SLP = Ignore for this project
       *  [ 6] STP = Ignore for this project
       *  [ 7] VISIB = Ignore for this project (Visibility)
       *  [ 8] WDSP = Ignore for this project
       *  [ 9] MXSPD = Ignore for this project
       *  [10] GUST = Ignore for this project    
       *  [18] MAX = Ignore for this project (Max Temperature for the day)
       *  [18] MIN = Ignore for this project (Min Temperature for the day)
       *  [19] PRCP = Ignore for this project (Precipitation)
       *  [20] NDP = Ignore for this project
       *  [21] FRSHTT = Ignore for this project
       */
      
      if (!line.substring(0, 3).equals("STN") ) {
        line = line.replaceAll("\\s+", " "); // strip whitespace
        line = line.replaceAll("\\+", ""); // strip plus sign if present 
      }
      String[] valuesResult = line.split(" "); // break into values
      
      // Strip asterisk off of MAX/MIN temps if present
      for (int i = 0; i < valuesResult.length; i++) {
        valuesResult[i] = valuesResult[i].replaceAll("\\*", ""); // strip whitespace
      }

      // Set Key
      tempKey = valuesResult[0] + "-" + valuesResult[1];
      
      /* Set Value as JSON Object */
      jsonData.put("YEARMODA", valuesResult[2]);
      jsonData.put("TEMP", valuesResult[3]);
      jsonData.put("MAX", valuesResult[17]);
      jsonData.put("MIN", valuesResult[18]);
      jsonData.put("PRCP", valuesResult[19]);
    } 
    
    /* Create JSON Array to hold JSON object and output as string */
    JSONArray jsonArray = new JSONArray();
    jsonArray.add(jsonData);
    String jsonStringOutput = jsonArray.toJSONString();
    
    data.set(jsonStringOutput); // set data = to compiled data as String
    taggedKey.set(tempKey,joinOrder);
    context.write(taggedKey, data);
  }
}

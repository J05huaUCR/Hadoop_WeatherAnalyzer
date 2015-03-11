package weatherAnalyzerPackage;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
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

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String tempKey = "";
    String tempValue = "";
    String line = value.toString();
    String flag = line.substring(0, 1);
    JSONObject jsonData = new JSONObject();
    
    /* Check for CSV */
    if (flag.equals("\"")) {
      // USAF (0) WBAN (1) STATION NAME (2) CTRY(3) STATE (4) LAT (5) LON
      // (6) ELEV(M) (7) BEGIN (8) END (9)

      StringTokenizer st = new StringTokenizer(line, ",");// use comma as token separator
      int tokenNumber = 0;
      
      /* Put values into JSON object */
      while (st.hasMoreTokens() ) {
        String tokenString = st.nextToken().toString();
        tokenString = tokenString.replaceAll(",", "-"); // strip comma
        tokenString = tokenString.replaceAll("\\+", ""); // remove plus sign
        tokenString = tokenString.replaceAll("\"", ""); // remove quotes 
        
        switch (tokenNumber) {
          case 0: // Key
            jsonData.put("STN", tokenString);
            tempKey += tokenString; // Strip Quotes
            //tempValue += "{\"USAF\":\"" + tempKey + "\"";
            break;
  
          case 1:
            jsonData.put("WBAN", tokenString);
            tempKey += "-" + tokenString; // Strip Quotes
            //tempValue += ",\"WBAN\":\"" + tokenString.replaceAll("\"", "") + "\"";
            break;
  
          case 2:
            jsonData.put("STATION NAME", tokenString);
            /*
            tempValue +=
                ",\"STATION NAME\":\"" + tokenString.replaceAll("\"", "") + "\"";
                */
            break;
  
          case 3:
            jsonData.put("CTRY", tokenString);
            /*
            tempValue +=
                ",\"CTRY\":\"" + tokenString.replaceAll("\"", "") + "\"";
                */
            break;
  
          case 4:
            jsonData.put("STATE", tokenString);
            /*tempValue +=
                ",\"STATE\":\"" + tokenString.replaceAll("\"", "") + "\"";*/
            break;
  
          case 5:
            jsonData.put("LAT", tokenString);
            /*tempValue +=
                ",\"LAT\":\"" + tokenString.replaceAll("\"", "") + "\"";*/
            break;
  
          case 6:
            jsonData.put("LON", tokenString);
            /*tempValue +=
                ",\"LON\":\"" + tokenString.replaceAll("\"", "") + "\"";*/
            break;
  
          case 7:
            jsonData.put("ELEV", tokenString);
            /*tempValue +=
                ",\"ELEV\":\"" + tokenString.replaceAll("\"", "") + "\"";*/
            break;
  
          case 8:
            jsonData.put("BEGIN", tokenString);
            /*tempValue +=
                ",\"BEGIN\":\"" + tokenString.replaceAll("\"", "") + "\"";*/
            break;
  
          case 9:
            jsonData.put("END", tokenString);
            /*tempValue +=
                ",\"END\":\"" + tokenString.replaceAll("\"", "") + "\"}";*/
            break;

        }
        tokenNumber++;
      }

      /* Output JSON format value 
      while (st.hasMoreTokens()) {
        String tokenString = st.nextToken().toString();
        tokenString = tokenString.replaceAll(",", "-"); // strip comma
        
        
        switch (tokenNumber) {
          case 0: // Key
            tempKey += tokenString.replaceAll("\"", ""); // Strip Quotes
            tempValue += "{\"USAF\":\"" + tempKey + "\"";
            break;

          case 1:
            tempKey += "-" + tokenString.replaceAll("\"", ""); // Strip Quotes
            tempValue += ",\"WBAN\":\"" + tokenString.replaceAll("\"", "") + "\"";
            break;

          case 2:
            tempValue +=
                ",\"STATION NAME\":\"" + tokenString.replaceAll("\"", "")
                    + "\"";
            break;

          case 3:
            tempValue +=
                ",\"CTRY\":\"" + tokenString.replaceAll("\"", "") + "\"";
            break;

          case 4:
            tempValue +=
                ",\"STATE\":\"" + tokenString.replaceAll("\"", "") + "\"";
            break;

          case 5:
            tempValue +=
                ",\"LAT\":\"" + tokenString.replaceAll("\"", "") + "\"";
            break;

          case 6:
            tempValue +=
                ",\"LON\":\"" + tokenString.replaceAll("\"", "") + "\"";
            break;

          case 7:
            tempValue +=
                ",\"ELEV\":\"" + tokenString.replaceAll("\"", "") + "\"";
            break;

          case 8:
            tempValue +=
                ",\"BEGIN\":\"" + tokenString.replaceAll("\"", "") + "\"";
            break;

          case 9:
            tempValue +=
                ",\"END\":\"" + tokenString.replaceAll("\"", "") + "\"}";
            break;

        } // End Switch
        
        
        tokenNumber++;
      }
      */

    } else  { // TXT

      if (!line.substring(0, 3).equals("STN") ) {
        line = line.replaceAll("\\s+", " "); // strip whitespace
      }
      String[] valuesResult = line.split(" "); // break into values
      
      // Strip asterisk off of MAX/MIN temps if present
      for (int i = 0; i < valuesResult.length; i++) {
        valuesResult[i] = valuesResult[i].replaceAll("\\*", ""); // strip whitespace
      }

      // Set Key
      tempKey = valuesResult[0] + "-" + valuesResult[1];
      
      /* Set Value as JSON Object */
      jsonData.put("YEARMODA", valuesResult[2] );
      //tempValue += "{\"YEARMODA\":\"" + valuesResult[2] + "\"";
      //tempValue += ",\"WBAN\":\"" + valuesResult[1] + "\"";
      jsonData.put("TEMP", valuesResult[3] );
      //tempValue += ",\"TEMP\":\"" + valuesResult[3] + "\"";
      jsonData.put("MAX", valuesResult[17] );
      //tempValue += ",\"MAX\":\"" + valuesResult[17] + "\"";
      jsonData.put("MIN", valuesResult[18] );
      //tempValue += ",\"MIN\":\"" + valuesResult[18] + "\"}";
    } 
    
    data.set("[" + jsonData.toJSONString() + "]");
    taggedKey.set(tempKey,joinOrder);
    context.write(taggedKey, data);
  }
}
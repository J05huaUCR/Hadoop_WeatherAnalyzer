package weatherAnalyzerPackage;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

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
    
    /* Check for CSV */
    if (flag.equals("\"")) {
      // USAF (0) WBAN (1) STATION NAME (2) CTRY(3) STATE (4) LAT (5) LON
      // (6) ELEV(M) (7) BEGIN (8) END (9)

      StringTokenizer st = new StringTokenizer(line, ",");// use comma as token separator
      int tokenNumber = 0;

      while (st.hasMoreTokens()) {
        String tokenString = st.nextToken().toString();
        tokenString = tokenString.replaceAll(",", "-"); // strip comma
        
        /* Output JSON format value */
        switch (tokenNumber) {
          case 0: // Key
            tempKey += tokenString.replaceAll("\"", ""); // Strip Quotes
            //tempValue += "{\"KEY\":\"" + tempKey + "\"";
            break;

          case 1:
            tempKey += "-" + tokenString.replaceAll("\"", ""); // Strip Quotes
           // tempValue += "{\"WBAN\":\"" + tokenString.replaceAll("\"", "") + "\"";
            break;

          case 2:
            tempValue +=
                "{\"STATION NAME\":\"" + tokenString.replaceAll("\"", "")
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

    } else  { // TXT

      if (!line.substring(0, 3).equals("STN") ) {
        line = line.replaceAll("\\s+", " "); // strip whitespace
      }
      String[] valuesResult = line.split(" "); // break into values
      
      // Strip asterisk off of MAX/MIN temps if present
      for (int i = 0; i < valuesResult.length; i++) {
        valuesResult[i] = valuesResult[i].replaceAll("\\*", ""); // strip whitespace
        //System.out.println("Processing Value[" + i + "]:" + valuesResult[i] );
      }

      // Set Key
      tempKey = valuesResult[0] + "-" + valuesResult[1];
      
      /* Set Value as JSON Object */
      tempValue += "{\"YEARMODA\":\"" + valuesResult[2] + "\"";
      //tempValue += ",\"WBAN\":\"" + valuesResult[1] + "\"";
      tempValue += ",\"TEMP\":\"" + valuesResult[3] + "\"";
      tempValue += ",\"MAX\":\"" + valuesResult[17] + "\"";
      tempValue += ",\"MIN\":\"" + valuesResult[18] + "\"}";
    } 
    
    data.set(tempValue);
    taggedKey.set(tempKey,joinOrder);
    context.write(taggedKey, data);
  }
}
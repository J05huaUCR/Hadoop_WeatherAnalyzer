package weatherAnalyzerPackage;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReducerOutResults extends Reducer<Text, Text, Text, Text> {
  private Text newKey = new Text();
  private Text newValues = new Text();
  private StringBuilder outputResultsString = new StringBuilder();
  
  /*
   * Take in a double value plus the month as an integer
   * a string of the form: 98.0014 - June 
   */
  public String getTempMonth(double temp, int month) {
    String result = String.format("%.4f", temp);
    result += " - ";
    switch (month) {
      case 1:
        result += "January";
      break;
      
      case 2:
        result += "February";
      break;
      
      case 3:
        result += "March";
      break;
      
      case 4:
        result += "April";
      break;
      
      case 5:
        result += "May";
      break;
      
      case 6:
        result += "June";
      break;
      
      case 7:
        result += "July";
      break;
      
      case 8:
        result += "August";
      break;
      
      case 9:
        result += "September";
      break;
      
      case 10:
        result += "October";
      break;
      
      case 11:
        result += "November";
      break;
      
      case 12:
        result += "December";
      break;
    } 
    
    return result;
  }
  
  /*
   * Take in a double value plus the date as a string and output
   * a string of the form: 98.0014 - June 6 2006
   */
  public String getValDate(double value, String yearmoda) {
    
    String result = "";
    
    // Process Data
    if (value < 9000.0) {
      result = String.format("%.4f", value);
    } else {
      result += "NO READING";
    }
    
    result += " - ";
    
    // Process Data
    if (yearmoda.equals("XX")) {
      result += "INVALID DATE";
    } else {
      String tempYear  = yearmoda.substring(0,4);
      String tempMonth = yearmoda.substring(4,6);
      String tempDate  = yearmoda.substring(6,yearmoda.length());
      
      int year = Integer.parseInt(tempYear);
      int month = Integer.parseInt(tempMonth);
      int date = Integer.parseInt(tempDate);
      
      switch (month) {
        case 1:
          result += "January";
        break;
        
        case 2:
          result += "February";
        break;
        
        case 3:
          result += "March";
        break;
        
        case 4:
          result += "April";
        break;
        
        case 5:
          result += "May";
        break;
        
        case 6:
          result += "June";
        break;
        
        case 7:
          result += "July";
        break;
        
        case 8:
          result += "August";
        break;
        
        case 9:
          result += "September";
        break;
        
        case 10:
          result += "October";
        break;
        
        case 11:
          result += "November";
        break;
        
        case 12:
          result += "December";
        break;
      } 
      result += " " + date + " " + year;
    }
    
    return result;
  }
  
  @SuppressWarnings({ })
  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    
    for (Text value : values) {
      
      /*
       * Parse into JSON Data
      */    
      JsonArray minJsonArray = JsonArray.readFrom( value.toString() );
      JsonObject minJsonObject = minJsonArray.get(0).asObject();
      
      /* [{"STATE":"AL",
       * "AVG_TEMP":80.17147,
       * "AVG_TEMP_DIFF":65.206136,
       * "MAX_TEMP":80.406136,
       * "MAX_TEMP_MONTH":6,
       * "MAX_RECORD_TEMP":92.2,
       * "MAX_RECORD_TEMP_DATE":"20090622",
       * "MIN_TEMP":15.2,
       * "MIN_TEMP_MONTH":7,
       * "MIN_RECORD_TEMP":9999,
       * "MIN_RECORD_TEMP_DATE":"XX",
       * "AVG_PRCP":0.44260287,
       * "AVG_PRCP_DIFF":0.47791306,
       * "MAX_PRCP":0.56871766,
       * "MAX_PRCP_MONTH":3,
       * "MAX_RECORD_PRCP":8.246,
       * "MAX_RECORD_PRCP_DATE":"20090328",
       * "MIN_PRCP":0.0908046,
       * "MIN_PRCP_MONTH":7,
       * "MIN_RECORD_PRCP":0,
       * "MIN_RECORD_PRCP_DATE":"20090703"}]
       */

      outputResultsString.setLength(0);
      outputResultsString.append(",STATE:," + minJsonObject.get("STATE") + ",");
      outputResultsString.append("AVG_TEMP:," + String.format("%.4f", minJsonObject.get("AVG_TEMP").asDouble()) + ",");
      outputResultsString.append("AVG_TEMP_DIFF:," + String.format("%.4f", minJsonObject.get("AVG_TEMP_DIFF").asDouble()) + ",");
      outputResultsString.append("MAX_TEMP:," + getTempMonth(minJsonObject.get("MAX_TEMP").asDouble(), minJsonObject.get("MAX_TEMP_MONTH").asInt()) + ",");
      outputResultsString.append("MAX_RECORD_TEMP:," + getValDate(minJsonObject.get("MAX_RECORD_TEMP").asDouble(), minJsonObject.get("MAX_RECORD_TEMP_DATE").asString()) + ",");
      outputResultsString.append("MIN_TEMP:," + getTempMonth(minJsonObject.get("MIN_TEMP").asDouble(), minJsonObject.get("MIN_TEMP_MONTH").asInt()) + ",");
      outputResultsString.append("MIN_RECORD_TEMP:," + getValDate(minJsonObject.get("MIN_RECORD_TEMP").asDouble(), minJsonObject.get("MIN_RECORD_TEMP_DATE").asString()) + ",");
      outputResultsString.append("AVG_PRCP_DIFF:," + String.format("%.4f", minJsonObject.get("AVG_PRCP_DIFF").asDouble()) + ",");
      outputResultsString.append("AVG_PRCP:," + String.format("%.4f", minJsonObject.get("AVG_PRCP").asDouble()) + ",");
      outputResultsString.append("MAX_PRCP:," + getTempMonth(minJsonObject.get("MAX_PRCP").asDouble(), minJsonObject.get("MAX_PRCP_MONTH").asInt()) + ",");
      outputResultsString.append("MAX_RECORD_PRCP:," + getValDate(minJsonObject.get("MAX_RECORD_PRCP").asDouble(), minJsonObject.get("MAX_RECORD_PRCP_DATE").asString()) + ",");
      outputResultsString.append("MIN_PRCP:," + getTempMonth(minJsonObject.get("MIN_PRCP").asDouble(), minJsonObject.get("MIN_PRCP_MONTH").asInt()) + ",");
      
      newKey.set(key.toString());
      newValues.set(outputResultsString.toString());

      context.write(newKey, newValues);
    }
    
  }
}

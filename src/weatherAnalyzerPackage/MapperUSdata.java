package weatherAnalyzerPackage;

//import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.JSONArray;
//import org.apache.hadoop.mapreduce.Mapper.Context;
//import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;

import java.io.IOException;
import java.util.StringTokenizer;

public class MapperUSdata extends Mapper<Text, Text, Text, Text> {
  //private AnchorKey taggedKey = new AnchorKey();
  private Text newKey = new Text();
  private Text newValues = new Text();
  //private int joinOrder;
/*
  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
      FileSplit fileSplit = (FileSplit)context.getInputSplit();
      joinOrder = Integer.parseInt(context.getConfiguration().get(fileSplit.getPath().getName()));
  }
  */

  @Override
  protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
    String tempKey = key.toString();
    String tempValue = "";
    String line = value.toString();
    String flag = line.substring(0, 1);
    
    Object objJSON = JSONValue.parse(line);
    JSONArray jsonArray=(JSONArray)objJSON;
    //JSONObject obj=(JSONObject)jsonData.get(0); // to retrieve
    // System.out.println( obj.get("WBAN") ); // to access
    
    newValues.set(jsonArray.toJSONString()); // for mapping
    //taggedKey.set(tempKey,joinOrder);
    context.write(newKey, newValues);
  }
}
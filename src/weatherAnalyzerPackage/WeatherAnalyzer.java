package weatherAnalyzerPackage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.json.simple.JSONObject;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;

/**
* http://codingjunkie.net/mapreduce-reduce-joins/
* User: Bill Bejeck
* Date: 6/11/13
* Time: 9:27 PM
* 
*/
public class WeatherAnalyzer {
  
  public static int deleteDirectory(String directoryName) {
    File index = new File(directoryName);
    if (index.exists()) {
      String[]entries = index.list();
      for(String s: entries){
        File currentFile = new File(index.getPath(),s);
        currentFile.delete();
      }
      index.delete();
      return 0;
    } 
  return -1;
  }
  
  public static void main(String[] args) throws Exception {
    
    Configuration config = new Configuration();
    
    // Vars to hold parameter info
    String readingsDir,stationsDir, outputDir;
    readingsDir = stationsDir = "";
    outputDir = "output"; // Set as Default
    StringBuilder filePaths = new StringBuilder();
    
    // Retrieve and parse passed in parameters
    for (int i = 0; i < args.length; i++) {

      if (args[i].substring(0, 1).equals("-")) {
        String catchMe = args[i].substring(1, 2);
        
        if (catchMe.equals("r")) {
          readingsDir = args[i + 1];
        } else if (catchMe.equals("s")) {
          stationsDir = args[i + 1];
        } else if (catchMe.equals("o")) {
          outputDir = args[i + 1];
        } else {
          // Nothing
        }
      }
    }
    
    deleteDirectory(outputDir);
    
    // get list of station files 
    File stations = new File(stationsDir);
    ArrayList<String> stationsFileNames = new ArrayList<String>(Arrays.asList(stations.list()));
    
    // Add station files to paths
    config.set(stationsFileNames.get(0), "1"); // add file name and set order
    filePaths.append(stationsDir + "/" + stationsFileNames.get(0)).append(",");
    
    // get list of reading files
    File readings = new File(readingsDir);
    ArrayList<String> readingsFileNames = new ArrayList<String>(Arrays.asList(readings.list()));

    // Add readings files to paths
    for (int i = 0; i < readingsFileNames.size(); i++) {
      config.set(readingsFileNames.get(i), Integer.toString(i+2)); // add file name and set order
      filePaths.append(readingsDir + "/" + readingsFileNames.get(0)).append(",");
    }
    
    filePaths.setLength(filePaths.length() - 1);
    
     /*
      * Define ReduceJoin job
      * Maps stations and readings, filtering data as necessary and then joining
      */
    Job joinDataSets = new Job(config, "Join");
    joinDataSets.setJarByClass(WeatherAnalyzer.class);

    FileInputFormat.addInputPaths(joinDataSets, filePaths.toString());
    FileOutputFormat.setOutputPath(joinDataSets, new Path(outputDir));

    joinDataSets.setMapperClass(MapperForJoin.class);
    joinDataSets.setReducerClass(ReducerForJoin.class);
    joinDataSets.setPartitionerClass(AnchorPartitioner.class);
    joinDataSets.setGroupingComparatorClass(AnchorGroupComparator.class); // remove for many to many
    joinDataSets.setOutputKeyClass(AnchorKey.class);
    joinDataSets.setOutputValueClass(Text.class);
    if ( joinDataSets.waitForCompletion(true) ) {
      System.out.print("Join Done.\n");
    } else {
      System.err.print("Something went horribly wrong...\n");
    }
    System.exit(joinDataSets.waitForCompletion(true) ? 0 : 1);
    
    
    /*
     * Map results from previous run to consolidate by state, year/month, 
     * eliminating non-US results at this time
     * reduce to max/min values per month
     */
  }
}
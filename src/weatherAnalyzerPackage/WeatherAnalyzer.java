package weatherAnalyzerPackage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.json.simple.JSONObject;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;

/**
* http://codingjunkie.net/mapreduce-reduce-joins/
* User: Bill Bejeck
* Date: 6/11/13
* Time: 9:27 PM
*/
public class WeatherAnalyzer {
  
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
     /*
      * Maps stations and readings, filtering data as necessary and then joining
      */
    filePaths.setLength(filePaths.length() - 1);
    Job job = new Job(config, "WeatherAnalyzer");
    job.setJarByClass(WeatherAnalyzer.class);

    FileInputFormat.addInputPaths(job, filePaths.toString());
    FileOutputFormat.setOutputPath(job, new Path(outputDir));

    job.setMapperClass(MapperForJoin.class);
    job.setReducerClass(ReducerForJoin.class);
    job.setPartitionerClass(AnchorPartitioner.class);
    job.setGroupingComparatorClass(AnchorGroupComparator.class); // remove for many to many
    job.setOutputKeyClass(AnchorKey.class);
    job.setOutputValueClass(Text.class);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
    
    /*
     * Map results from previous run to consolidate by state, year/month, 
     * eliminating non-US results at this time
     * reduce to max/min values per month
     */
    
    

  }
}
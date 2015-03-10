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

/*
 * I've had a few people ask me questions about what the "correct" answer for this project is. 
 * There are a few pieces of variance on this project. For example:
 * 
 * 		1. We have one station ID that corresponds to several stations. 
 * 			Some groups are handling this in different ways
 * 		2. Some students have different ideas of how to "correctly" calculate average. 
 * 			This is okay. The important thing is that your results need to be correct 
 * 			given the choices that you have made. For a general idea of what results 
 * 			should look like, states like Utah, the Dakotas, and Minnesota should be near 
 * 			the bottom of the least (High variance in temperature) whereas states like 
 * 			Hawaii, California, and Puerto Rico should be near the top of the list (Stable 
 * 			year-round temperatures).
 * 
 * A note on the values for precipitation (For EXTRA CREDIT ONLY):
 * A value of 99.99 means that there is no data (You can either treat these as 0 or 
 * exclude them from your calculations, though the latter is the more correct option).
 * 
 * There is a letter at the end of the recordings. Here is a table of what the letters mean 
 * (Basically the letter tells you how long the precipitation was accumulated before recording).
 * 
 * 		A - 6 hours worth of precipitation
 * 		B - 12 hours...
 * 		C - 18 hours...
 * 		D - 24 hours...
 * 		E - 12 hours... (slightly different from B but the same for this project).
 * 		F - 24 hours ... (slightly different from D but the same for this project).
 * 		G - 24 hours ... (slightly different from D but the same for this project).
 * 		H - station recorded a 0 for the day (although there was some recorded instance of precipitation).
 * 		I - station recorded a 0 for the day (and there was NO recorded instance of precipitation).
 * 
 * How you treat these is up to you (just let me know in your README). A simple solution 
 * would be to multiply. For instance, if they recorded 12 hours worth of precipitation, 
 * multiply it by 2 to extrapolate 24 hours worth.
 * 
 * I didn't specifically say this, so please include in your report the output file 
 * from your run of your project. Should be a single file with 50-53 lines (Depending 
 * on if you count things like Puerto Rico as "states") with a schema similar to the 
 * following (You can add a header row if you would like):
 * 
 * 		STATE - abbreviation of the state, "CA"
 * 		HIGH MONTH NAME - month with the highest average, "JULY"
 * 		AVERAGE FOR THAT MONTH - "73.668"
 * 		**PRECIPITATION THAT MONTH
 * 		LOW MONTH NAME - month with the lowest average, "DECEMBER"
 * 		AVERAGE FOR THAT MONTH - "48.054"
 * 		**PRECIPITATION THAT MONTH
 * 		DIFFERENCE BETWEEN THE TWO - "25.614"
 * 		
 * 		**example of extra credit fields
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
    String readingsDir,stationsDir, joinDir, outputDir;
    readingsDir = stationsDir = "";
    joinDir = "output_join";
    outputDir = "output"; // Set as Default
    StringBuilder filePaths = new StringBuilder();
    
    // clear previous passes if present
    deleteDirectory(joinDir);
    deleteDirectory(outputDir);
    
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
    
    /*
     * Maps stations and readings, filtering data as necessary and then joining
     */
    
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
     */
    Job joinDataSets = new Job(config, "Join");
    joinDataSets.setJarByClass(WeatherAnalyzer.class);

    FileInputFormat.addInputPaths(joinDataSets, filePaths.toString());
    FileOutputFormat.setOutputPath(joinDataSets, new Path(joinDir));

    joinDataSets.setMapperClass(MapperForJoin.class);
    joinDataSets.setReducerClass(ReducerForJoin.class);
    joinDataSets.setPartitionerClass(AnchorPartitioner.class);
    joinDataSets.setGroupingComparatorClass(AnchorGroupComparator.class); // remove for many to many
    joinDataSets.setOutputKeyClass(AnchorKey.class);
    joinDataSets.setOutputValueClass(Text.class);
    if ( joinDataSets.waitForCompletion(true) ) {
    	System.out.print("Done.");
    	System.exit(0);
    } else {
    	System.err.print("Something went horribly wrong...");
    	System.exit(1);
    }
    // System.exit(job.waitForCompletion(true) ? 0 : 1);
    
    /*
     * Map results from previous run to consolidate by state, year/month, 
     * eliminating non-US results at this time
     * reduce to max/min values per month
     */
    
    // get list of joinedData files
    File dataSet = new File(joinDir);
    ArrayList<String> joinedData = new ArrayList<String>(Arrays.asList(dataSet.list()));

    // Add readings files to paths
    for (int i = 0; i < joinedData.size(); i++) {
      config.set(joinedData.get(i), Integer.toString(i+2)); // add file name and set order
      filePaths.append(joinDir + "/" + joinedData.get(0)).append(",");
    }
    
    // remove last comma
    filePaths.setLength(filePaths.length() - 1);
    
    Job getUSdata = new Job(config,"USdata");
    

  }
}


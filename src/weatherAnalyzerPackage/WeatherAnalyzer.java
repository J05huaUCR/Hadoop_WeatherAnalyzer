package weatherAnalyzerPackage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

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

/*
 * Project Summary:
 * For this project, we will have two datasets. The first provides station information 
 * for weather stations across the world. The second provides individual recordings for 
 * the stations over a 4-year period. The goal of the project is to find out which 
 * states in the US have the most stable temperature (i.e. their hottest month and 
 * coldest month have the least difference).
 * 
 * Formal Problem:
 * For stations within the United States, group the stations by state. For each state 
 * with readings, find the average temperature recorded for each month (ignoring year)
 * Find the months with the highest and lowest averages for that state. Order the 
 * states by the difference between the highest and lowest month average, ascending.
 * 
 * For each state, return:
 *    The state abbreviation, e.g. “CA”
 *    The average temperature and name of the highest month, e.g. “90, July”
 *    The average temperature and name of the lowest month, e.g. “50, January”
 *    The difference between the two, e.g. “40”
 *    
 * The fields are as follows:
 *    USAF = Air Force station ID. May contain a letter in the first position.
 *    WBAN = NCDC WBAN number
 *    STATION NAME = A text name for the station
 *    CTRY = FIPS country ID
 *    ST = State for US stations
 *    LAT = Latitude in thousandths of decimal degrees
 *    LON = Longitude in thousandths of decimal degrees
 *    ELEV = Elevation in meters
 *    BEGIN = Beginning Period Of Record (YYYYMMDD).
 *    END = Ending Period Of Record (YYYYMMDD).
 *    
 * Readings:
 *    STN---  = The station ID (USAF)
 *    WBAN   = NCDC WBAN number
 *    YEARMODA   = The datestamp
 *    TEMP = The average temperature for the day, followed by the number of recordings
 *    DEWP = Ignore for this project
 *    SLP = Ignore for this project
 *    STP = Ignore for this project
 *    VISIB = Ignore for this project (Visibility)
 *    WDSP = Ignore for this project
 *    MXSPD = Ignore for this project
 *    GUST = Ignore for this project    
 *    MAX = Ignore for this project (Max Temperature for the day)
 *    MIN = Ignore for this project (Min Temperature for the day)
 *    PRCP = Ignore for this project (Precipitation)
 *    NDP = Ignore for this project
 *    FRSHTT = Ignore for this project
 *    
 * Due Date:
 * This project will be due Thursday, March 19th before Midnight. 
 * Email me your submissions, one email per group.
 * 
 * Deliverables:
 *  [ ] A zipped file containing:
 *      [x] The script to run your job
 *      [ ] A README describing your project, including:
 *        [ ] Your usernames and node number.
 *        [ ] An overall description of how you chose to separate the 
 *           problem into different mapreduce jobs, with reasoning.
 *        [ ] A description of each mapreduce job including:
 *           [ ] What the job does 
 *           [ ] An estimate of runtime for the pass
 *        [ ] A description of how you chose to do the join(s)
 *        [ ] A description of anything you did extra for the project, 
 *           such as adding a combiner. If there is anything that you feel 
 *           deserves extra credit, put it here.
 *        [ ] All files needed for the script to run (Don’t include the 
 *           two original datasets).
 *           
 * The script should take as input three things: 
 *  [x] A folder containing the Locations file
 *  [x] A folder containing the Recordings files
 *  [x] An output folder
 *  
 *  I will provide these when I try to run your script. Please change 
 *  the hdfs interactions to be based on /user/sjaco002 before you submit 
 *  (it should be your own folder when you run it).
 *  
 * Think About:
 *    1. There are different ways to do joins in mapreduce. What can you use 
 *      from the datasets to inform your decision (e.g. size)? Please Google 
 *      joins in mapreduce for more information
 *    2. Make sure your parse correctly. One file is a csv, the other is not. 
 *      One file has a single row of Headers, the other has many.
 *    3. How many passes should you do? How mush work should each pass do?
 *    4. Make sure to start early. The cluster will get slower as more people 
 *      use it. A single pass of the data took me about 2 minutes while the 
 *      cluster was empty.
 *    
 * Potential For Extra Credit:
 * Please feel free to try to beef up your project for extra credit. There 
 * are many ways that you can do this. Here are a few examples:
 *  [ ] A good use of combiners
 *  [ ] A clever way to achieve faster execution time
 *  [ ] Enriching the data, e.g. including the average precipitation for the two months
 * 
 * Bigger Bonus:
 * [ ] Include the stations with “CTRY” as “US” that don’t have a state tag, 
 *    finding a way to estimate the state using a spatial distance with the 
 *    known stations. There are some stations that are Ocean Buoys so you may 
 *    want to have a maximum distance to be required in order to be included 
 *    in a state, or you could create a separate “state” representing the 
 *    “pacific” and “atlantic” ocean (Checked by using coordinates). 
 *    There is a lot of potential work here so the extra credit could be large).
 * 
 * Whatever you try to do let me know in your README. If you aren’t sure 
 * whether your idea is worth extra credit or not, just email me.
 *    
 * I've had a few people ask me questions about what the "correct" answer for this project is. 
 * There are a few pieces of variance on this project. For example:
 * 
 *    1. We have one station ID that corresponds to several stations. 
 *      Some groups are handling this in different ways
 *    2. Some students have different ideas of how to "correctly" calculate average. 
 *      This is okay. The important thing is that your results need to be correct 
 *      given the choices that you have made. For a general idea of what results 
 *      should look like, states like Utah, the Dakotas, and Minnesota should be near 
 *      the bottom of the least (High variance in temperature) whereas states like 
 *      Hawaii, California, and Puerto Rico should be near the top of the list (Stable 
 *      year-round temperatures).
 * 
 * A note on the values for precipitation (For EXTRA CREDIT ONLY):
 * A value of 99.99 means that there is no data (You can either treat these as 0 or 
 * exclude them from your calculations, though the latter is the more correct option).
 * 
 * There is a letter at the end of the recordings. Here is a table of what the letters mean 
 * (Basically the letter tells you how long the precipitation was accumulated before recording).
 * 
 *    A - 6 hours worth of precipitation
 *    B - 12 hours...
 *    C - 18 hours...
 *    D - 24 hours...
 *    E - 12 hours... (slightly different from B but the same for this project).
 *    F - 24 hours ... (slightly different from D but the same for this project).
 *    G - 24 hours ... (slightly different from D but the same for this project).
 *    H - station recorded a 0 for the day (although there was some recorded instance of precipitation).
 *    I - station recorded a 0 for the day (and there was NO recorded instance of precipitation).
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
 *    STATE - abbreviation of the state, "CA"
 *    HIGH MONTH NAME - month with the highest average, "JULY"
 *    AVERAGE FOR THAT MONTH - "73.668"
 *    **PRECIPITATION THAT MONTH
 *    LOW MONTH NAME - month with the lowest average, "DECEMBER"
 *    AVERAGE FOR THAT MONTH - "48.054"
 *    **PRECIPITATION THAT MONTH
 *    DIFFERENCE BETWEEN THE TWO - "25.614"
 *    
 *    **example of extra credit fields
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
    String readingsDir,stationsDir, joinDir, mapUSDir, outputDir;
    readingsDir = stationsDir = "";
    joinDir = "output_join";
    mapUSDir = "output_us_data";
    outputDir = "output"; // Set as Default
    
    
    
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
    
    // clear previous passes if present
    deleteDirectory(joinDir);
    deleteDirectory(mapUSDir);
    deleteDirectory(outputDir);
    
    /*
     * Define ReduceJoin job
     * Maps stations and readings, filtering data as necessary and then joining
    */
    
    // get list of station files 
    StringBuilder filePaths = new StringBuilder();
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
      System.out.print("Join Done.\n");
    } else {
      System.err.print("Something went horribly wrong...\n");
    }
     
    
    
    /*
     * Map results from previous run to consolidate by state, year/month, 
     * eliminating non-US results at this time
     * reduce to max/min values per month
    */
    Configuration conf2 = new Configuration();
    //Configuration conf2 = getConf();
    Job job2 = new Job(conf2, "Job 2");
    job2.setJarByClass(WeatherAnalyzer.class);

    job2.setMapperClass(MapperUSdata.class);
    job2.setReducerClass(ReducerUSdata.class);

    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);

    job2.setInputFormatClass(TextInputFormat.class);
    job2.setOutputFormatClass(TextOutputFormat.class);

    TextInputFormat.addInputPath(job2, new Path(joinDir));
    TextOutputFormat.setOutputPath(job2, new Path(mapUSDir));

    job2.waitForCompletion(true);
    /*
    Configuration job2config = new Configuration();
    StringBuilder joinPaths = new StringBuilder();
    
    // get list of joined US Data file
    File usData = new File(joinDir);
    ArrayList<String> joinedData = new ArrayList<String>(Arrays.asList(usData.list()));
    
    // Add station files to paths
    String joinedDataFilname = joinedData.get(0);
    job2config.set(joinedDataFilname, "1"); // add file name and set order
    joinPaths.append(joinDir + "/" + joinedDataFilname);     
      
    
     * Define Mapping data to US and Month, Reducing to State and Avg max/min for each month
     
    
    Job getUSdata = new Job(job2config,"USdata");
    getUSdata.setJarByClass(WeatherAnalyzer.class);
    
    System.out.println("INPUT PATH: | " + joinDir + " |");
    System.out.println("OUTPUT PATH: | " + mapUSDir + " |");
    
    // Set data paths
    FileInputFormat.addInputPaths(getUSdata, joinDir);
    FileOutputFormat.setOutputPath(getUSdata, new Path(mapUSDir));
        
    // Configure Job
    System.out.println("Defining MapperUSdata");
    getUSdata.setMapperClass(MapperUSdata.class);
    System.out.println("Defining ReducerUSdata");
    getUSdata.setReducerClass(ReducerUSdata.class);
    System.out.println("Setting Output Key");
    getUSdata.setOutputKeyClass(Text.class);
    System.out.println("Setting Output Value");
    getUSdata.setOutputValueClass(Text.class);
    
    // Fire Job
    if ( getUSdata.waitForCompletion(true) ) {
      System.out.print("Map/Reduce US Data Done.\n");
      System.exit(0);
    } else {
      System.err.print("Something went horribly wrong...\n");
      System.exit(1);
    }   
    */
    
    /*
     * Job 1
     
    Configuration conf = getConf();
    FileSystem fs = FileSystem.get(conf);
    Job job = new Job(conf, "Job1");
    job.setJarByClass(WeatherAnalyzer.class);

    job.setMapperClass(MapperUSdata.class);
    job.setReducerClass(ReducerUSdata.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    TextInputFormat.addInputPath(job, new Path(args[0]));
    TextOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

    job.waitForCompletion(true);
*/
    /*
     * Job 2
     
    Configuration conf2 = getConf();
    Job job2 = new Job(conf2, "Job 2");
    job2.setJarByClass(ChainJobs.class);

    job2.setMapperClass(MyMapper2.class);
    job2.setReducerClass(MyReducer2.class);

    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);

    job2.setInputFormatClass(TextInputFormat.class);
    job2.setOutputFormatClass(TextOutputFormat.class);

    TextInputFormat.addInputPath(job2, new Path(OUTPUT_PATH));
    TextOutputFormat.setOutputPath(job2, new Path(args[1]));

    job2.waitForCompletion(true) ? 0 : 1;*/
  }
}
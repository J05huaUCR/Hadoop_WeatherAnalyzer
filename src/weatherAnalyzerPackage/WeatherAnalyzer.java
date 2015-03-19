package weatherAnalyzerPackage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;


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
    String readingsDir,stationsDir, joinDir, mapUSDir, mapStateDir, outputDir, writeOut;
    readingsDir = stationsDir = writeOut = "";
    joinDir = "output_join";
    mapUSDir = "output_us_data";
    mapStateDir = "output_state_data";
    outputDir = "output"; // Set as Default
    final Double NANO2SEC = (double) 1000000000; // for converting nanoseconds to seconds
    Double duration = (Double) 0.0;
    long startTime = System.nanoTime();
    long endTime = System.nanoTime();
    long jobStartTime = System.nanoTime();
    long jobEndTime = System.nanoTime();
    Double jobDuration = (Double) 0.0;    
    
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
    
    //create a temporary file
    String timeStamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());
    File outputFile = new File("Timing_" + timeStamp + ".txt");
    System.out.println(outputFile.getCanonicalPath());
    BufferedWriter writeToOutput = null;
    writeToOutput = new BufferedWriter(new FileWriter(outputFile));
    writeToOutput.write("Weather Analyzer Timings:\n");
    writeToOutput.write("------------------------------------\n");
    
    /*
     *  clear previous passes if present
     
    deleteDirectory(joinDir);
    deleteDirectory(mapUSDir);
    deleteDirectory(mapStateDir);
    deleteDirectory(outputDir);*/
    
    /* =========================================================================
     * Job 1: Join Data Sets
     * Maps stations and readings, filtering data as necessary and then joining
     * ========================================================================= */
    
    // Begin timer
    jobStartTime = System.nanoTime();
    String filePaths = stationsDir + "/," + readingsDir + "/";
    
    Job joinDataSets = new Job(config, "Join");
    joinDataSets.setJarByClass(WeatherAnalyzer.class);

    FileInputFormat.addInputPaths(joinDataSets, filePaths);
    FileOutputFormat.setOutputPath(joinDataSets, new Path(joinDir));

    joinDataSets.setMapperClass(MapperForJoin.class);
    joinDataSets.setReducerClass(ReducerForJoin.class);
    joinDataSets.setPartitionerClass(AnchorPartitioner.class);
    joinDataSets.setGroupingComparatorClass(AnchorGroupComparator.class); // remove for many to many
    joinDataSets.setOutputKeyClass(AnchorKey.class);
    joinDataSets.setOutputValueClass(Text.class);
    if ( joinDataSets.waitForCompletion(true) ) {
      jobEndTime = System.nanoTime();   
      jobDuration = (double) ((jobEndTime - jobStartTime) / NANO2SEC);  
      writeOut = "Job 1: Join data sets completed in " + String.format("%.4f", jobDuration) + "secs.\n";
      System.out.print(writeOut);
    } else {
      writeOut = "Job 1: Something went horribly wrong...\n";
      System.err.print(writeOut);
    }
    writeToOutput.write(writeOut);
     
    /* =========================================================================
     * Job 2: Map to US Data
     * Map results from previous run to consolidate by state, year/month, 
     * eliminating non-US results at this time
     * reduce to max/min values per month
     * ========================================================================= */
   
    // Begin Timer
    jobStartTime = System.nanoTime();
    
    Configuration getStateDataConf = new Configuration();
    Job getStateData = new Job(getStateDataConf, "Get States' data");
    getStateData.setJarByClass(WeatherAnalyzer.class);

    getStateData.setMapperClass(MapperUSdata.class);
    getStateData.setReducerClass(ReducerUSdata.class);

    getStateData.setOutputKeyClass(Text.class);
    getStateData.setOutputValueClass(Text.class);

    getStateData.setInputFormatClass(TextInputFormat.class);
    getStateData.setOutputFormatClass(TextOutputFormat.class);

    TextInputFormat.addInputPath(getStateData, new Path(joinDir));
    TextOutputFormat.setOutputPath(getStateData, new Path(mapUSDir));

    // Run Job
    if ( getStateData.waitForCompletion(true) ) {

      jobEndTime = System.nanoTime();   
      jobDuration = (double) ((jobEndTime - jobStartTime) / NANO2SEC);  
      writeOut = "Job 2: Filter to US data completed in " + String.format("%.4f", jobDuration) + "secs.\n";
      System.out.print(writeOut);
    
    } else {
      System.err.println("Job 2: Something went horribly wrong...");
    }
    
    writeToOutput.write(writeOut);
   
    /* =========================================================================
     * Job 3: Map to States Data
     * Map results from previous run to consolidate by state, year/month, 
     * eliminating non-US results at this time
     * reduce to max/min values per month
     * ========================================================================= */
    
    jobStartTime = System.nanoTime();
    
    Configuration compileDataConf = new Configuration();
    Job compileData = new Job(compileDataConf, "Get States data");
    compileData.setJarByClass(WeatherAnalyzer.class);

    compileData.setMapperClass(MapStateData.class);
    compileData.setReducerClass(ReduceStateData.class);

    compileData.setOutputKeyClass(Text.class);
    compileData.setOutputValueClass(Text.class);

    compileData.setInputFormatClass(TextInputFormat.class);
    compileData.setOutputFormatClass(TextOutputFormat.class);
    //compileData.setSortComparatorClass(Text.class);

    TextInputFormat.addInputPath(compileData, new Path(mapUSDir));
    TextOutputFormat.setOutputPath(compileData, new Path(mapStateDir));

    // Run Job
    if ( compileData.waitForCompletion(true) ) {
      
      jobEndTime = System.nanoTime();   
      jobDuration = (double) ((jobEndTime - jobStartTime) / NANO2SEC);  
      writeOut = "Job 3: Compile to State Data completed in " + String.format("%.4f", jobDuration) + "secs.\n";
      System.out.print(writeOut);
      
    } else {
      writeOut = "Job 3: Something went horribly wrong...\n";
      System.err.println(writeOut);
    }
    
    writeToOutput.write(writeOut);
    
    /* =========================================================================
     * Job 4: Output final results
     * Take output State data, map on temp difference and output results
     * ========================================================================= */
    
    jobStartTime = System.nanoTime();
    
    Configuration outputResulstsConf = new Configuration();
    Job outputResults = new Job(outputResulstsConf, "Output results");
    outputResults.setJarByClass(WeatherAnalyzer.class);

    outputResults.setMapperClass(MapperOutResults.class);
    outputResults.setReducerClass(ReducerOutResults.class);

    outputResults.setOutputKeyClass(Text.class);
    outputResults.setOutputValueClass(Text.class);

    outputResults.setInputFormatClass(TextInputFormat.class);
    outputResults.setOutputFormatClass(TextOutputFormat.class);

    TextInputFormat.addInputPath(outputResults, new Path(mapStateDir));
    TextOutputFormat.setOutputPath(outputResults, new Path(outputDir));

    // Run Job
    if ( outputResults.waitForCompletion(true) ) {
      
      jobEndTime = System.nanoTime();   
      jobDuration = (double) ((jobEndTime - jobStartTime) / NANO2SEC);  
      writeOut = "Job 4: Output final results completed in " + String.format("%.4f", jobDuration) + "secs.\n";
      System.out.print(writeOut);
       
    } else {
      writeOut = "Job 4: Something went horribly wrong...\n";
      System.err.println(writeOut);
    }
    
    writeToOutput.write(writeOut);
    
    
    // End Program
    endTime = System.nanoTime();   
    duration = (double) ((endTime - startTime)/ NANO2SEC);
    
    writeOut = "\nAnalysis complete in " + String.format("%.4f", duration) + "secs.\n";
    System.out.println();
    System.out.println(writeOut);
    
    writeToOutput.write(writeOut);
    writeToOutput.close();
    
  }
  
}
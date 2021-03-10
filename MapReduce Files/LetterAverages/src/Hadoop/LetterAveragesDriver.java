package Hadoop;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class LetterAveragesDriver extends Configured implements Tool{
	public static void main(String [] args) throws Exception {
		if(args.length != 3) {
			System.err.println("Usage: LetterAverages <input path> <output path>");
			System.exit(-1);
		}
		
		int exitCode = ToolRunner.run(new LetterAveragesDriver(), args);  
		System.exit(exitCode);
		
	}
	
	public int run(String[] args) throws Exception {
		
	
		
	
		Configuration conf1 = new Configuration();
		Job TotalLetters = Job.getInstance(conf1);  
		TotalLetters.setJarByClass(LetterAveragesDriver.class);
		TotalLetters.setJobName("Total Letters");
	    
	  
		FileInputFormat.setInputDirRecursive(TotalLetters, true);
		FileInputFormat.addInputPath(TotalLetters, new Path(args[1]));
		FileOutputFormat.setOutputPath(TotalLetters, new Path(args[2]+"/TotalLetters"));
		
		TotalLetters.setMapperClass(TotalMapper.class);
		TotalLetters.setCombinerClass(TotalReducer.class);
		TotalLetters.setReducerClass(TotalReducer.class);
	    
		TotalLetters.setMapOutputKeyClass(TextPair.class);
		TotalLetters.setMapOutputValueClass(IntWritable.class);
		
		ControlledJob controlledTotalLetters = new ControlledJob(conf1);
	    controlledTotalLetters.setJob(TotalLetters);
	   
	    
	 
	    Configuration conf2 = new Configuration();
		Job LetterFrequency = Job.getInstance(conf2);  
		LetterFrequency.setJarByClass(LetterAveragesDriver.class);
		LetterFrequency.setJobName("Letter Frequencies");
	    
	    
		FileInputFormat.setInputDirRecursive(LetterFrequency, true);
		FileInputFormat.addInputPath(LetterFrequency, new Path(args[1]));
		FileOutputFormat.setOutputPath(LetterFrequency, new Path(args[2]+"/LetterFrequency"));
		
		LetterFrequency.setMapperClass(FreqMapper.class);
		LetterFrequency.setCombinerClass(FreqReducer.class);
		LetterFrequency.setReducerClass(FreqReducer.class);
	    
		LetterFrequency.setMapOutputKeyClass(TextPair.class);
		LetterFrequency.setMapOutputValueClass(IntWritable.class);
		
		ControlledJob controlledLetterFrequency = new ControlledJob(conf2);
	    controlledLetterFrequency.setJob(LetterFrequency);
	   
	    
	    
	
	    Configuration conf3 = new Configuration();
		Job AverageOccurences = Job.getInstance(conf3);  
		AverageOccurences.setJarByClass(LetterAveragesDriver.class);
		AverageOccurences.setJobName("Average Occurrences");
	    
	   
		FileInputFormat.setInputDirRecursive(AverageOccurences, true);
		FileInputFormat.addInputPath(AverageOccurences, new Path(args[2]+"/TotalLetters"));
		FileInputFormat.addInputPath(AverageOccurences, new Path(args[2]+"/LetterFrequency"));
		FileOutputFormat.setOutputPath(AverageOccurences, new Path(args[2]+"/AverageOccurences"));
		
		AverageOccurences.setMapperClass(AverageMapper.class);
		AverageOccurences.setReducerClass(AverageReducer.class);
	    
		AverageOccurences.setMapOutputKeyClass(Text.class);
		AverageOccurences.setMapOutputValueClass(TextPair.class);
		
		ControlledJob controlledAverageOccurences = new ControlledJob(conf3);
	    controlledAverageOccurences.setJob(AverageOccurences);
	    
	    
	    controlledAverageOccurences.addDependingJob(controlledTotalLetters); //Average Occurrences job cannot start until Total Letters and Letter Frequency have finished
	    controlledAverageOccurences.addDependingJob(controlledLetterFrequency);
	    
	    
		JobControl jobCtrl = new JobControl("jobcontrol"); //create job control to direct the workflow of each job
	    jobCtrl.addJob(controlledAverageOccurences);
	    jobCtrl.addJob(controlledLetterFrequency);
	    jobCtrl.addJob(controlledTotalLetters);
	    
	    
	    Thread jobControlThread = new Thread(jobCtrl);
	    jobControlThread.start();
		
	    while (!jobCtrl.allFinished()) {
	    	System.out.println("Executing jobs, still running...");
	    	Thread.sleep(5000);
	    }
		   System.out.println("Finished Executing, check the Hadoop filesystem!");
		   jobCtrl.stop();
	       return (AverageOccurences.waitForCompletion(true) ? 0 : 1);
	}

}

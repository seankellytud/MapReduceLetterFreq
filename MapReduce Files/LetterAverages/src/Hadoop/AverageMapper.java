package Hadoop;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AverageMapper extends Mapper<LongWritable, Text , Text, TextPair> {
    
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		//split each entry from the two sets of inputs into an array, 
		String output [] = value.toString().split("\\s");
		//write  array index 0 along with index 1 and 2 in a textpair which will be used for accessing the total count and the frequency of each letter
		context.write(new Text(output[0]), new TextPair(output[1], output[2]));
	
	}

}

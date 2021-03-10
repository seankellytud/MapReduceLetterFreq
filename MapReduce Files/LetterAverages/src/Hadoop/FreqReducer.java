package Hadoop;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;


public class FreqReducer extends Reducer<TextPair, IntWritable, TextPair, IntWritable> {
	//adds all the 1s together that came from the mapper class to produce the frequency of each letter.
	public void reduce(TextPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
		int count = 0;
		
		for(IntWritable value: values) {
			
			count += value.get();
		}
		
		context.write(key, new IntWritable(count));
	}

}

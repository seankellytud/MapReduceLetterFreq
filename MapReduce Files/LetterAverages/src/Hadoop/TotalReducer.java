package Hadoop;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class TotalReducer extends Reducer<TextPair, IntWritable, TextPair, IntWritable> {
	
	public void reduce(TextPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int languageLetterCount = 0;
		for(IntWritable value: values) {
			// adds up all the 1s passed by mapper to produce total count of letters
			languageLetterCount += value.get();
		}
		context.write(key, new IntWritable(languageLetterCount));
	}

}

package Hadoop;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.MarkableIterator;
import java.text.DecimalFormat;
import org.apache.hadoop.io.Text;


public class AverageReducer extends Reducer<Text, TextPair, Text, TextPair> {
	
	public void reduce(Text key, Iterable<TextPair> textPair, Context context) throws IOException, InterruptedException {
		/*empty value for the total number located at the second item of text pair in total output 
		 * to be iterated over to get total counts for each language
		 * which will then be divided into the frequency of each letter*/
		int count = 0; 
		
		MarkableIterator<TextPair> markI = new MarkableIterator<TextPair>(textPair.iterator());
		markI.mark();
		while (markI.hasNext()) //inputs are looped through and if first element is equals sign, the second element is the count
		{
			TextPair textP = markI.next();
			if(textP.getFirst().toString().equals("=")) { //if there is an equals sign it means the second element of the text pair is the total
				count = Integer.parseInt(textP.getSecond().toString()); 
			}
		}
		markI.reset(); /*reset iterator and loop through them again to find the letter frequency
		inputs are looped through and if first element is not equals sign, the second element is the frequency*/
		while(markI.hasNext()) 
		{
			TextPair textP = markI.next();
			if(!textP.getFirst().toString().equals("=")) { //if there is no equals sign it means the second element of the text pair is the frequency
				int freq = Integer.parseInt(textP.getSecond().toString()); //frequency of each is set to the number which is located at the second TextPair position
				double avg = (double)freq / count ; //get average by dividing the two values
				DecimalFormat df = new DecimalFormat("#.####"); 
				Double roundedAvg = Double.valueOf(df.format(avg)); //format to 4 decimal places

				context.write(key, new TextPair(textP.getFirst().toString(), String.valueOf(roundedAvg))); 
				//output the key (which is the language), then a TextPair consisting of the letter and average occurrence which is rounded to 4 decimal places
		}
	}

}
	}

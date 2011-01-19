package hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * Mapper, converts text 'user movie grade' to pair ('movie', 'user grade')
 * @author maciek
 *
 */
public class FriendsFinderMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, IntWritable, Text> {

	private final static IntWritable num = new IntWritable();
	private final static Text text = new Text();
	
	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<IntWritable, Text> output, Reporter reporter)
			throws IOException {
		
		String line = value.toString();
		StringTokenizer itr = new StringTokenizer(line);
		List<Integer> data = new ArrayList<Integer>();
		
		try {
			while (itr.hasMoreTokens()) {
				int val = Integer.parseInt(itr.nextToken());
				data.add(val);
			}
			if (data.size() != 3)
				throw new NumberFormatException("Invalid data size.");
		} catch (NumberFormatException ex) {
			System.out.println("Invalid input, expected (user, movie, grade) triple.");
			ex.printStackTrace();
			return;
		}
		int user = data.get(0);
		int movie = data.get(1);
		int grade = data.get(2);
		String ret = user + " " + grade;
		text.set(ret);
		num.set(movie);
		
		output.collect(num, text);
	}
}

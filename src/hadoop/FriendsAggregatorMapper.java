package hadoop;

import java.io.IOException;
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
public class FriendsAggregatorMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, Text> {

	private final static Text outKey = new Text();
	private final static Text outValue = new Text();
	
	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		
		String line = value.toString();
		
		try {
			String[] t = line.split("\t");
			outKey.set(t[0]);
			outValue.set(t[1]);
		}
		catch (IndexOutOfBoundsException ex) {
			System.out.println("Invalid input format. '" + line + "'");
			ex.printStackTrace();
			return;
		}
		output.collect(outKey, outValue);
	}
}

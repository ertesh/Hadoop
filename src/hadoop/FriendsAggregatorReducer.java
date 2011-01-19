package hadoop;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class FriendsAggregatorReducer extends MapReduceBase implements
		Reducer<Text, Text, Text, Text> {

	private final static Text outValue = new Text();
	
	
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

		Diff sum = new Diff(0, 0);
		while (values.hasNext()) {
			Diff value = new Diff(values.next().toString());
			sum.add(value);
		}
		outValue.set(sum.toPercentileString());
		output.collect(key, outValue);
	}
}
package hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class CanopyCentersMapReduce extends MapReduceBase implements
		Mapper<Text, Text, LongWritable, Text>,
		Reducer<LongWritable, Text, LongWritable, Text> {

	private final static LongWritable outKey = new LongWritable();
	private final static Text outValue = new Text();

	private final List<UserGrades> centers = new ArrayList<UserGrades>();

	private boolean isNewCenter(UserGrades candidate) {
		for (UserGrades center : centers) {
			if (Metrics.sameStrongCanopy(candidate, center)) {
				return false;
			}
		}
		return true;
	}

	private int addCenter(UserGrades list) {
		centers.add(list);
		return centers.size();
	}

	@Override
	public void map(Text key, Text value,
			OutputCollector<LongWritable, Text> output, Reporter reporter)
			throws IOException {

		String line = value.toString();
		StringTokenizer itr = new StringTokenizer(line);
		List<Integer> data = new ArrayList<Integer>();

		try {
			while (itr.hasMoreTokens()) {
				int val = Integer.parseInt(itr.nextToken());
				data.add(val);
			}
			if (data.size() != 2)
				throw new NumberFormatException("Invalid data size.");
		} catch (NumberFormatException ex) {
			ex.printStackTrace();
			return;
		}
		int user = data.get(0);
		int grade = data.get(1);

		int movie = Integer.parseInt(key.toString());
		String ret = user + " " + grade;
		outKey.set(movie);
		outValue.set(ret);

		output.collect(outKey, outValue);
	}

	@Override
	public void reduce(LongWritable key, Iterator<Text> values,
			OutputCollector<LongWritable, Text> output, Reporter reporter)
			throws IOException {
		UserGrades list = new UserGrades();
		while (values.hasNext()) {
			int user, grade;
			try {
				Text value = values.next();
				String[] s = value.toString().split(" ");
				user = Integer.parseInt(s[0]);
				grade = Integer.parseInt(s[1]);
				list.add(user, grade);
			} catch (NumberFormatException ex) {
				continue;
			}
		}
		if (isNewCenter(list)) {
			int counter = addCenter(list);
			outKey.set(counter);
			outValue.set(list.toString());
			output.collect(outKey, outValue);
		}
	}

}

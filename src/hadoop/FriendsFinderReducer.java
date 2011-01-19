package hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class FriendsFinderReducer extends MapReduceBase implements
		Reducer<IntWritable, Text, Text, Text> {

	private final static Text outKey = new Text();
	private final static Text outValue = new Text();

	
	private List<UserGrade> grades = new ArrayList<UserGrade>();
	
	public void reduce(IntWritable key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

		grades.clear();
		while (values.hasNext()) {
			int user, grade;
			try {
				Text value = values.next();
				String[] s = value.toString().split(" ");
				user = Integer.parseInt(s[0]);
				grade = Integer.parseInt(s[1]);
			} catch (NumberFormatException ex) {
				continue;
			}
			for (UserGrade record: grades) {
				Diff diff = Diff.calculateDiff(grade, record.grade);
				int user1 = Math.min(user, record.user);
				int user2 = Math.max(user, record.user);
				if (user1 == user2) {
					System.out.println("Two grades of the same movie for one user.");
					continue;
				}
				outKey.set(user1 + " " + user2);
				outValue.set(diff.toFractionString());
				output.collect(outKey, outValue);
			}
			grades.add(new UserGrade(user, grade));
		}
	}
}
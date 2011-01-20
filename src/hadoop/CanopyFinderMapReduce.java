package hadoop;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class CanopyFinderMapReduce extends MapReduceBase implements
		Mapper<Text, Text, LongWritable, Text>,
		Reducer<LongWritable, Text, LongWritable, Text> {

	private final static LongWritable outKey = new LongWritable();
	private final static Text outValue = new Text();

	private final List<UserGrades> centers = new ArrayList<UserGrades>();

	@Override
	public void configure(JobConf conf) {
		Path[] cacheFiles;
		try {
			cacheFiles = DistributedCache.getLocalCacheFiles(conf);
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}
	    if (cacheFiles == null) {
	    	//TODO
	    	cacheFiles = FileManager.getFiles(new Path(FileManager.centersPath), conf).toArray(new Path[1]);
	    }	
	    centers.clear();
	    if (cacheFiles.length > 0) {
	    	for (Path cachePath : cacheFiles) {
	            addCenters(cachePath);
	        }
	    }
	}

	private void addCenters(Path cachePath) {
		FileReader fReader;
		try {
			fReader = new FileReader(cachePath.toUri().getPath());
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		}
		BufferedReader bReader = new BufferedReader(fReader);
		String line;
		try {
			while ((line = bReader.readLine()) != null) {
				UserGrades center = new UserGrades();
				String[] tab = line.split("\\s+");
				int id = Integer.parseInt(tab[0]);
				center.setId(id);
				for (int i = 1; i < tab.length; i++) {
					String[] pom = tab[i].split(":");
					int user = Integer.parseInt(pom[0]);
					int grade = Integer.parseInt(pom[1]);
					center.add(user, grade);
				}
				centers.add(center);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
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
		StringBuilder builder = new StringBuilder();
		for (UserGrades center: centers) {
			if (Metrics.sameWeakCanopy(list, center)) {
				builder.append(center.getId());
				builder.append(" ");
			}
		}
		outValue.set(builder.toString());
		output.collect(key, outValue);
	}

}

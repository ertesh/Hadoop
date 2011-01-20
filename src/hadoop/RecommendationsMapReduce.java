package hadoop;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

public class RecommendationsMapReduce extends MapReduceBase implements
		Mapper<Text, Text, LongWritable, Text>,
		Reducer<LongWritable, Text, LongWritable, Text> {

	private final static LongWritable outKey = new LongWritable();
	private final static Text outValue = new Text();

	private Map<Integer, Integer> clusterMap = new HashMap<Integer, Integer>();
	private Map<Integer, List<Integer>> clusters = new HashMap<Integer, List<Integer>>();

	@Override
	public void configure(JobConf conf) {
		Path[] cacheFiles;
		try {
			cacheFiles = DistributedCache.getLocalCacheFiles(conf);
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}
		cacheFiles = FileManager.checkFolder(cacheFiles, conf, true);
		clusterMap.clear();
		clusters.clear();
	    if (cacheFiles.length > 0) {
	    	for (Path cachePath : cacheFiles) {
	            addClusters(cachePath);
	        }
	    }
	}

	private void addClusters(Path cachePath) {
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
				String[] tab = line.split("\\s+");
				int movie = Integer.parseInt(tab[0]);
				int id = Integer.parseInt(tab[1]);
				clusterMap.put(movie, id);
				if (clusters.containsKey(id)) {
					clusters.get(id).add(movie);
				} else {
					List<Integer> cluster = new ArrayList<Integer>();
					cluster.add(movie);
					clusters.put(id, cluster);
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}
	
	private List<Integer> getSimilarMovies(int movie) {
		int clusterId = clusterMap.get(movie);
		List<Integer> similar = clusters.get(clusterId);
		return similar;
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
		
		String ret = movie + " " + grade;
		outKey.set(user);
		outValue.set(ret);

		output.collect(outKey, outValue);
	}

	@Override
	public void reduce(LongWritable key, Iterator<Text> values,
			OutputCollector<LongWritable, Text> output, Reporter reporter)
			throws IOException {
		
		
		Set<Integer> seenMovies = new HashSet<Integer>();
		List<Integer> favourite = new ArrayList<Integer>();
		while (values.hasNext()) {
			int movie, grade;
			try {
				Text value = values.next();
				String[] s = value.toString().split(" ");
				movie = Integer.parseInt(s[0]);
				grade = Integer.parseInt(s[1]);
				seenMovies.add(movie);
				if (grade >= 8) {
					favourite.add(movie);
				}
				
			} catch (NumberFormatException ex) {
				continue;
			}
		}
		
		List<Integer> recommendations = new ArrayList<Integer>();
		
		for (int movie: favourite) {
			List<Integer> similar = getSimilarMovies(movie);
			for (int proposition: similar) {
				if (!seenMovies.contains(proposition))
					recommendations.add(proposition);
			}
		}
		
		for (Integer proposition: recommendations) {
			outValue.set(proposition + " " + 10);
			output.collect(key, outValue);
		}
	}
}

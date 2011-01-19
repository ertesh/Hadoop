package hadoop;

import java.io.File;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;

import api.FileManager;

public class FriendsFinder {

	public void run() {
		FileManager.deleteDir(new File(FileManager.tmpPath));
		FileManager.deleteDir(new File(FileManager.outPath));
		// FriendFinder
		runMapReduce(FileManager.inPath, FileManager.tmpPath, IntWritable.class, Text.class,
				FriendsFinderMapper.class, FriendsFinderReducer.class);
		
		// FriendAggregator
		runMapReduce(FileManager.tmpPath, FileManager.outPath, Text.class, Text.class,
				FriendsAggregatorMapper.class, FriendsAggregatorReducer.class);
	}

	@SuppressWarnings({ "deprecation", "unchecked" })
	public void runMapReduce(String inPath, String outPath, Class<?> keyClass,
			Class<?> valueClass, Class<? extends Mapper> mapper,
			Class<? extends Reducer> reducer) {
		
		JobClient client = new JobClient();
		JobConf conf = new JobConf();

		// specify output types
		conf.setOutputKeyClass(keyClass);
		conf.setOutputValueClass(valueClass);

		// specify input and output DIRECTORIES (not files)
		conf.setInputPath(new Path(inPath));
		conf.setOutputPath(new Path(outPath));

		// specify a mapper
		conf.setMapperClass(mapper);

		// specify a reducer
		conf.setReducerClass(reducer);

		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

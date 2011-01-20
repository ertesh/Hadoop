package hadoop;

import java.io.File;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;

public class HadoopManager {

	public void run() {
		FileManager.deleteDir(new File(FileManager.outPath));
		FileManager.deleteDir(new File(FileManager.tmpPath));

		// find canopy centers
		runMapReduce(FileManager.inPath, FileManager.centersPath + "0", null,
				LongWritable.class, Text.class, CanopyCentersMapReduce.class,
				CanopyCentersMapReduce.class);

		// find canopies
		runMapReduce(FileManager.inPath, FileManager.canopiesPath,
				FileManager.centersPath + "0", LongWritable.class, Text.class,
				CanopyFinderMapReduce.class, CanopyFinderMapReduce.class);

		final int maxLoops = 3;
		for (int i = 1; i <= maxLoops; i++) {
			// run k-means clustering iteration
			runMapReduce(FileManager.inPath, FileManager.clustersPath + i,
					FileManager.centersPath + (i - 1), LongWritable.class,
					Text.class, KMeansMapReduce.class, KMeansMapReduce.class);

			// choose new k centers
			runMapReduce(FileManager.inPath, FileManager.centersPath + i,
					FileManager.clustersPath + i, LongWritable.class,
					Text.class, NewCentersMapReduce.class,
					NewCentersMapReduce.class);

		}

		// get recommendations
		runMapReduce(FileManager.inPath, FileManager.outPath,
				FileManager.clustersPath + maxLoops, LongWritable.class,
				Text.class, RecommendationsMapReduce.class,
				RecommendationsMapReduce.class);
	}

	@SuppressWarnings("unchecked")
	public void runMapReduce(String inPath, String outPath, String cachePath,
			Class<?> keyClass, Class<?> valueClass,
			Class<? extends Mapper> mapper, Class<? extends Reducer> reducer) {

		JobClient client = new JobClient();
		JobConf conf = new JobConf();

		// specify output types
		conf.setOutputKeyClass(keyClass);
		conf.setOutputValueClass(valueClass);

		// specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(conf, new Path(inPath));
		FileOutputFormat.setOutputPath(conf, new Path(outPath));

		// set cache dir
		if (cachePath != null) {
			for (Path path : FileManager.getFiles(new Path(cachePath), conf))
				DistributedCache.addCacheFile(path.toUri(), conf);
		}

		// set key-value input format
		conf.setInputFormat(KeyValueTextInputFormat.class);

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

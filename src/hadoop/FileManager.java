package hadoop;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

public class FileManager {
	public static final String inPath = "input";
	public static final String outPath = "output";
	public static final String tmpPath = "tmp";
	public static final String centersPath = tmpPath + "/" + "centers";
	public static final String canopiesPath = tmpPath + "/" + "canopies";
	public static final String clustersPath = tmpPath + "/" + "clusters";

	// Deletes all files and subdirectories under dir.
	// Returns true if all deletions were successful.
	// If a deletion fails, the method stops attempting to delete and returns
	// false.
	public static boolean deleteDir(File dir) {
		if (!dir.exists())
			return false;
		if (dir.isDirectory()) {
			String[] children = dir.list();
			for (int i = 0; i < children.length; i++) {
				boolean success = deleteDir(new File(dir, children[i]));
				if (!success) {
					return false;
				}
			}
		}

		// The directory is now empty so delete it
		return dir.delete();
	}

	public static List<Path> getFiles(Path path, Configuration conf) {
		List<Path> list = new ArrayList<Path>();
		try {
			FileStatus[] fsa = FileSystem.get(conf).listStatus(path);
			for (FileStatus fs : fsa) {
				if (!fs.isDir())
					list.add(fs.getPath());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return list;
	}

	public static Path[] checkFolder(Path[] cacheFiles, JobConf conf, boolean b) {
		if (cacheFiles != null)
			return cacheFiles;

		String path = centersPath + "0";
		if (b)
			path = clustersPath + "1";
		return getFiles(new Path(path), conf).toArray(new Path[1]);

	}

	public static void saveGrades(List<String> grades) {
		String filename = inPath + "/" + "file01";
		FileWriter fWriter;
		try {
			fWriter = new FileWriter(filename);
			BufferedWriter bWriter = new BufferedWriter(fWriter);
			for (String grade : grades) {
				bWriter.write(grade + "\n");
			}
			bWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static List<String> readRecommendations() {
		
		List<String> ret = new ArrayList<String>();

		String filename = outPath + "/" + "part-00000";
		FileReader fReader;
		try {
			fReader = new FileReader(filename);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return ret;
		}
		BufferedReader bReader = new BufferedReader(fReader);
		String line;
		try {
			while ((line = bReader.readLine()) != null) {
				ret.add(line);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return ret;
		}
		return ret;
	}
}

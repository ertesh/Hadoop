package api;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FileManager {
	public static final String inPath = "input";
	public static final String tmpPath = "tmp";
	public static final String outPath = "output";

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

	public static void saveGrades(Map<User, List<Grade>> grades) {
		String filename = inPath + "/" + "file01";
		FileWriter fWriter;
		try {
			fWriter = new FileWriter(filename);
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}
		BufferedWriter bWriter = new BufferedWriter(fWriter);
		for (Map.Entry<User, List<Grade>> entry: grades.entrySet()) {
			for (Grade grade: entry.getValue()) {
				try {	
					bWriter.write(grade.code());
					bWriter.write("\n");
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		try {
			bWriter.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static Map<User, List<Grade>> readRecommendations() {
		Map<User, List<Grade>> ret = new HashMap<User, List<Grade>>();
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
				String[] tab = line.split("\\s+");
				int userId = Integer.parseInt(tab[0]);
				int movieId = Integer.parseInt(tab[1]);
				int grade = Integer.parseInt(tab[2]);
				User user = User.getUser(userId);
				Movie movie = Movie.getMovie(movieId);
				Grade recommendation = new Grade(user, movie, grade);
				if (!ret.containsKey(user)) {
					List<Grade> list = new ArrayList<Grade>();
					list.add(recommendation);
					ret.put(user, list);
				} else {
					ret.get(user).add(recommendation);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
			return ret;
		}
		return ret;
	}
}

package api;

import java.util.HashMap;
import java.util.Map;

public class Movie {
	private int id;
	private String name;
	
	private static Map<Integer, Movie> movieMap = new HashMap<Integer, Movie>();
	
	private Movie(int movieId, String movieName) {
		id = movieId;
		name = movieName;
	}

	public static Movie newMovie(int movieId, String movieName) {
		Movie ret = new Movie(movieId, movieName);
		movieMap.put(movieId, ret);
		return ret;
	}
	
	public static Movie getMovie(int movieId) {
		Movie ret = movieMap.get(movieId);
		if (ret == null) {
			throw new ArrayIndexOutOfBoundsException();
		}
		return ret;
	}
	
	public String toString() {
		return name;
	}

	public int getId() {
		return id;
	}	
}

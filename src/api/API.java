package api;

import hadoop.FriendsFinder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class API {
	
	private List<User> users;
	private List<Movie> movies;
	private Map<User, List<Grade>> grades;
	private Map<User, List<Grade>> recommendations;
	
	public API() {
		users = new ArrayList<User>();
		movies = new ArrayList<Movie>();
		grades = new HashMap<User, List<Grade>>();
		recommendations = new HashMap<User, List<Grade>>();
	}
	
	public void addUser(User user) {
		users.add(user);
	}
	
	public void addMovie(Movie movie) {
		movies.add(movie);
	}
	
	public void addGrade(User user, Movie movie, int g) {
		Grade grade = new Grade(user, movie, g);
		if (grades.containsKey(user)) {
			grades.get(user).add(grade);
		} else {
			List<Grade> list = new ArrayList<Grade>();
			list.add(grade);
			grades.put(user, list);
		}
		
	}
	
	public List<User> getUsers() {
		return users;
	}
	
	public List<Movie> getMovies() {
		return movies;
	}
	
	public List<Grade> getUserGrades(User user) {
		return grades.get(user);
	}
	
	public List<Grade> getUserRecommendations(User user) {
		return recommendations.get(user);
	}
	
	public void updateRecommendations() {
		FileManager.saveGrades(grades);
		FriendsFinder finder = new FriendsFinder();
		finder.run();
		recommendations.clear();
		recommendations = FileManager.readRecommendations();
	}
}

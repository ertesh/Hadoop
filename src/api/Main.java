package api;

import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

public class Main {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		API api = new API();
		insertData(api);
		api.updateRecommendations();
		for (User user: api.getUsers()) {
			List<Grade> list = api.getUserRecommendations(user);
			if (list == null) {
				continue;
			}
			for (Grade grade: list) {
				System.out.println(grade);
			}
		}
	}
	
	public static void insertData(API api) {
		final int movieCount = 50;
		final int userCount = 20;
		
		
		for (int i = 1; i <= movieCount; i++) {
			api.addMovie(Movie.newMovie(i, "Movie " + i));
		}
		Random random = new Random();
		for (int i = 1; i <= userCount; i++) {
			User user = User.newUser(i, "User " + i);
			api.addUser(user);
			
			int gradesNo = random.nextInt(movieCount / 2);
			Set<Integer> s = new HashSet<Integer>();
			while (s.size() < gradesNo) {
				int movieId = 1 + random.nextInt(movieCount);
				if (s.contains(movieId)) {
					continue;
				}
				s.add(movieId);
				Movie movie = Movie.getMovie(movieId);
				int grade = 1 + random.nextInt(10);
				api.addGrade(user, movie, grade);
			}
		}

	}

}

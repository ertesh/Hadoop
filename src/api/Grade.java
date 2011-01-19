package api;

public class Grade {
	public Grade(User _user, Movie _movie, int g) {
		grade = g;
		user = _user;
		movie = _movie;
		if (movie == null) {
			System.out.println("err");
		}
	}

	int grade;
	Movie movie;
	User user;
	
	public String code() {
		if (user == null || movie == null) {
			System.out.println("err");
		}
		return user.getId() + " " + movie.getId() + " " + grade;
	}
	
	public String toString() {
		return user.toString() + " : " + movie.toString() + " : " + grade;
	}
}

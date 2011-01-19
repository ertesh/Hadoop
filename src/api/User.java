package api;

import java.util.HashMap;
import java.util.Map;

public class User {
	private int id;
	private String name;
	
	private static Map<Integer, User> userMap = new HashMap<Integer, User>();
	
	private User(int userId, String userName) {
		id = userId;
		name = userName;
	}

	public static User newUser(int userId, String userName) {
		User ret = new User(userId, userName);
		userMap.put(userId, ret);
		return ret;
	}
	
	public static User getUser(int userId) {
		return userMap.get(userId);
	}
	
	public String toString() {
		return name;
	}

	public int getId() {
		return id;
	}	
}

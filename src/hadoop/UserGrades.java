package hadoop;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class UserGrades {

	private Map<Integer, Integer> map = new HashMap<Integer, Integer>();
	private Set<Integer> set = new HashSet<Integer>();
	private int id;
	
	public UserGrades() {
		id = -1;
	}
	
	public void add(int user, int grade) {
		map.put(user, grade);
		set.add(user);
	}

	public Set<Integer> getUserSet() {
		return set;
	}
	
	public Map<Integer, Integer> getMap() {
		return map;
	}
	
	public String toString() {
		StringBuilder s = new StringBuilder();
		for (Map.Entry<Integer, Integer> pair: map.entrySet()) {
			s.append(pair.getKey() + ":" + pair.getValue() + " ");
		}
		return s.toString();
	}
	
	public void setId(int _id) {
		id = _id;
	}

	public int getId() {
		return id;
	}
}

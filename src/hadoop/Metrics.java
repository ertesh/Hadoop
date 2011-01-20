package hadoop;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Metrics {
	public static final int strongLimit = 20;
	public static final int weakLimit = 15;
	private static final double INF = 10e9;
	
	public static boolean sameStrongCanopy(UserGrades candidate,
			UserGrades center) {
		Set<Integer> s1 = new HashSet<Integer>(candidate.getUserSet());
		Set<Integer> s2 = center.getUserSet();
		s1.retainAll(s2);
		return (s1.size() > strongLimit);
	}
	
	public static boolean sameWeakCanopy(UserGrades candidate,
			UserGrades center) {
		Set<Integer> s1 = new HashSet<Integer>(candidate.getUserSet());
		Set<Integer> s2 = center.getUserSet();
		s1.retainAll(s2);
		return (s1.size() > weakLimit);
	}
	
	public static double getDistance(UserGrades candidate, UserGrades center) {
		double ret = 0.0;
		int counter = 0;
		Map<Integer, Integer> map1 = candidate.getMap();
		Map<Integer, Integer> map2 = candidate.getMap();
		for (Map.Entry<Integer, Integer> entry: map1.entrySet()) {
			int user = entry.getKey();
			int grade = entry.getValue();
			if (map2.containsKey(user)) {
				int grade2 = map2.get(user);
				ret += (grade - grade2) * (grade - grade2);
				counter++;
			}
		}
		if (counter < weakLimit) {
			return INF;
		}
		ret = Math.sqrt(ret) / counter;
		return ret;
	}

}

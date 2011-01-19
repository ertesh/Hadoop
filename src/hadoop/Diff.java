
package hadoop;

public class Diff {
	protected int points;
	protected int maxPoints;
	
	public Diff(int a, int b) {
		points = a;
		maxPoints = b;
	}
	
	public Diff(String string) {
		String[] t = string.split(" ");
		points = Integer.parseInt(t[0]);
		maxPoints = Integer.parseInt(t[1]);
	}

	public static Diff calculateDiff(int grade1, int grade2) {
		return new Diff(Math.abs(grade1 - grade2), 10);
	}
	
	public String toFractionString() {
		return points + " " + maxPoints;
	}

	public void add(Diff value) {
		points += value.points;
		maxPoints += value.maxPoints;
	}

	public String toPercentileString() {
		Integer percentile = 100 * (maxPoints - points) / maxPoints;
		return percentile.toString();
	}
}

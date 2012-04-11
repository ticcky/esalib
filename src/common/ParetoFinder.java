package common;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Finds the pareto-optimal points from a set of point. Point's dimensions are Numbers.
 * @author zilka
 * @param <T> 
 */
public class ParetoFinder<T> {
	List<ParetoPoint> points;
	List<T> objs;

	public ParetoFinder() {
		points = new ArrayList<ParetoPoint>();
		objs = new ArrayList<T>();
	}

	public void addPoint(T obj, Number ... values) {
		ParetoPoint p = new ParetoPoint();
		for(Number n : values)
			p.values.add(n);
		objs.add(obj);
		points.add(p);
	}

	public List<T> getOptimal() {
		List<T> res = new ArrayList<T>();
		int i = 0;
		for(ParetoPoint p1 : points) {
			//System.out.println("deciding: " + p1);
			boolean opt = true;
			for(ParetoPoint p2: points) {
				if(p1.compareTo(p2) == -1) {
					opt = false;
					//System.out.println(" - this is better: " + p2);
					break;
				}
			}
			if(opt) {
				res.add(objs.get(i));
				System.out.println("adding " + p1);
			}
			i++;
		}
		return res;

	}

	class ParetoPoint implements Comparable {
		List<Number> values = new ArrayList<Number>();

		public int compareTo(Object o) {
			ParetoPoint p = (ParetoPoint) o;
			Iterator<Number> i = values.iterator();
			boolean someWorse = false;
			boolean someBetter = false;
			for(Number n : p.values) {
				double v1 = i.next().doubleValue();
				double v2 = n.doubleValue();
				if(v2 < v1) {
					someBetter = true;
				} else if (v2 > v1) {
					someWorse = true;
				}
			}
			if(someBetter && someWorse)
				return 0; // p1 == p2
			else if(someBetter && !someWorse)
				return -1; // p1 worseThan p2
			else if(!someBetter && someWorse)
				return 1; // p1 betterThan p2
			else
				return 0;
		}

		@Override
		public String toString() {
			String res = "";
			for(Number n : values)
				res += n + " ";
			return res;
		}



	}
}

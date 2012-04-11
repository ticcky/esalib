package gnuplot;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;

/**
 * Stores graphing data.
 * @author zilka
 */
public class GNUPlotData {

	Hashtable<Number, Number> data = new Hashtable<Number, Number>();

	public Hashtable<Number, Number> getData() {
		return data;
	}



	public void addData(Number x, Number y) {
		data.put(x, y);
	}

	public void addData(GNUPlotData d, double divideBy) {
		for(Number x : d.data.keySet()) {
			Number val = new Double(d.data.get(x).doubleValue() / divideBy);
			if(data.containsKey(x)) {
				val = new Double(val.doubleValue() + data.get(x).doubleValue());
			}
			data.put(x, val);
		}
	}

	@Override
	public String toString() {
		String result = "";
		StringBuilder sb = new StringBuilder();

		// Sort hashtable.
		List<Number> v = new ArrayList(data.keySet());
		Collections.sort((List) v);

		for (Number x : v) {
			sb.append(result);
			sb.append(x);
			sb.append(" ");
			sb.append(data.get(x));
			sb.append("\n");
			//result = result + x + " " + data.get(x) + "\n";
		}

		sb.append("e\n");

		return sb.toString();
	}
}

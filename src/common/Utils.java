package common;

import com.thoughtworks.xstream.XStream;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.text.Normalizer;
import java.text.Normalizer.Form;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.io.IOUtils;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * A set of useful utilities.
 * @author zilka
 */
public class Utils {

	public static String extractTextChildren(Node parentNode) {
		NodeList childNodes = parentNode.getChildNodes();
		String result = new String();
		for (int i = 0; i < childNodes.getLength(); i++) {
			Node node = childNodes.item(i);
			if (node.getNodeType() == Node.TEXT_NODE) {
				result += node.getNodeValue();
			}
		}
		return result;
	}
	static int progressCntr;
	static long lastProgressReport = 0;

	public static void progressReporter(String str, int cnt) {
		progressCntr += cnt;
		if (System.currentTimeMillis() - lastProgressReport > 1000) {
			System.out.println(str + ": " + progressCntr
				+ "/1 second");
			lastProgressReport = System.currentTimeMillis();
			progressCntr = 0;
		}

	}

	public static List<Integer> extractList(String fileName) throws FileNotFoundException, IOException {
		List<Integer> res = new ArrayList<Integer>();
		BufferedReader br = new BufferedReader(new FileReader(fileName));
		String ln;
		while ((ln = br.readLine()) != null) {
			res.add(Integer.parseInt(ln));
		}
		return res;
	}

	public static double gauss(double x, double mean, double stdDeviation) {

		return Math.pow(Math.exp(-(((x - mean) * (x - mean)) / ((2
			* stdDeviation * stdDeviation)))), 1 / (stdDeviation * Math.sqrt(2
			* Math.PI)));

	}

	public static String readBinaryStream(InputStream contentStream) throws IOException {
		StringWriter writer = new StringWriter();
		IOUtils.copy(contentStream, writer, "UTF8");
		return writer.toString();
	}

	public static int unsignedToBytes(byte b) {
		return b & 0xFF;
	}

	public static String removeDiacriticalMarks(String string) {
		return Normalizer.normalize(string, Form.NFD).replaceAll("\\p{InCombiningDiacriticalMarks}+", "");
	}

	public static int wordCount(String s, Set<String> stopwordSet) {
		if (stopwordSet == null) {
			stopwordSet = new HashSet<String>();
		}

		String[] parts = s.split(" ");
		int cntr = 0;
		for (String p : parts) {
			if (!stopwordSet.contains(p)) {
				cntr++;
			}
		}

		return cntr;
	}

	public static Set<String> loadStopWords(String f) throws FileNotFoundException, IOException {
		Set<String> stopWords = new HashSet<String>();
		BufferedReader br = new BufferedReader(new FileReader(f));
		String line;
		while ((line = br.readLine()) != null) {
			stopWords.add(line);
			stopWords.add(stem(line));
		}
		return stopWords;
	}

	public static void save(Object c, String fn) throws IOException {
		XStream xs = new XStream();
		xs.toXML(c, new FileWriter(fn));
	}

	public static Object load(String fn) throws IOException {
		XStream xs = new XStream();
		return xs.fromXML(new FileReader(fn));
	}

	public static String stem(String word) {
		word = word.replaceAll("\\W", " ");
		word = word.replaceAll("  ", " ");

		String res = "";
		String[] parts = word.split(" ");
		for(String p : parts) {
			org.tartarus.snowball.ext.PorterStemmer ps = new org.tartarus.snowball.ext.PorterStemmer();
			ps.setCurrent(p);
			ps.stem();

			res += ps.getCurrent().toLowerCase() + " ";
		}

		return res.trim();
	}

	public static String unstopwordize(String stemmed, Set<String> stopwords) {
		String[] parts = stemmed.split(" ");
		String res = "";
		for(String p : parts) {
			if(!stopwords.contains(p)) {
				res += p + " ";
			}
		}
		return res.trim();

	}
}

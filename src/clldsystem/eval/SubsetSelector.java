package clldsystem.eval;

//import com.sun.org.apache.bcel.internal.generic.NEW;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import clldsystem.data.WikiArticleParser;
import org.xml.sax.SAXException;

/**
 * Processes the Wikipedia XML dump and selects only pages that contain a given
 * keyword.
 * @author zilka
 */
public class SubsetSelector {

	String file;

	public SubsetSelector(String file) {
		this.file = file;
	}

	Set<Integer> getPagesWith(String str) {
		Set<Integer> res = new LinkedHashSet<Integer>();
		try {
			SAXParserFactory factory = SAXParserFactory.newInstance();
			SAXParser saxParser;
			factory.setNamespaceAware(false);
			factory.setValidating(false);
			InputStream is;
			FileInputStream fis = new FileInputStream(this.file);
			is = fis;
			saxParser = factory.newSAXParser();

			SSWikiArticleParser handler = new SSWikiArticleParser(res, str, fis);
			saxParser.parse(is, handler);
			is.close();
			return res;
		} catch (IOException ex) {

			Logger.getLogger(SubsetSelector.class.getName()).log(Level.SEVERE, null, ex);
			return res;
		} catch (ParserConfigurationException ex) {
			Logger.getLogger(SubsetSelector.class.getName()).log(Level.SEVERE, null, ex);
			return res;
		} catch (Exception ex) {
			Logger.getLogger(SubsetSelector.class.getName()).log(Level.SEVERE, null, ex);
			return res;
		}

	}

	private class SSWikiArticleParser extends WikiArticleParser {

		Set<Integer> res;
		String str;
		long timer;
		int cntr = 0;
		FileInputStream fis;

		public SSWikiArticleParser(Set<Integer> res, String str, FileInputStream fis) {
			this.res = res;
			this.str = str;
			this.timer = System.currentTimeMillis();
			this.fis = fis;
		}

		@Override
		protected void emitPage(Integer id, String title, String content) {
			cntr += 1;
			if (System.currentTimeMillis() - this.timer >= 1000) {
				System.out.print("Processed " + cntr + " pages per second");
				try {
					System.out.println(" [" + (((float) fis.getChannel().position()) / fis.getChannel().size() * 100) + "%]");
				} catch (IOException ex) {
					Logger.getLogger(SubsetSelector.class.getName()).log(Level.SEVERE, null, ex);
				}
				cntr = 0;
				this.timer = System.currentTimeMillis();
			}
		}

		@Override
		protected void finish() {
		}

		@Override
		public void characters(char[] ch, int start, int length) throws SAXException {
			super.characters(ch, start, length);

			String str = new String(ch, start, length);
			if (str.matches(this.str)) {
				res.add(Integer.parseInt(this.aid));
			}
		}

		@Override
		public void nextBody(String str) {
		}
	}

	public static void main(String[] args) throws FileNotFoundException, IOException {
		SubsetSelector ss = new SubsetSelector(args[0]);
		FileOutputStream fos = new FileOutputStream(args[1]);
		for (Integer i : ss.getPagesWith(args[2])) {
			fos.write((i.toString() + "\n").getBytes());
		}


	}
}

/**
 * 
 * Terminology Extraction Tool
 * 
 * Goes through Wikipedia dump (xml file that can be obtained from download.wikimedia.org/enwiki/)
 * and uses the cross-language links to extract terminology from the pages.
 * 
 * Author: Lukas Zilka (l.zilka@open.ac.uk)
 * Year: 2011
 */

package clldsystem.tools;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Hashtable;
import java.util.Scanner;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.*;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class TerminologyExtractor {
	public static void main(String[] args) {
		// if the user hasn't supplied any parameters, tell him how to use this
		if (args.length < 2) {
			System.out
					.println("Usage: TerminologyExtractor <wiki dump xml> <output dict xml>");
			return;
		}

		// instantiate
		TerminologyExtractor te = new TerminologyExtractor();

		// parse
		SAXParserFactory factory = SAXParserFactory.newInstance();
		SAXParser saxParser;

		// tell the XML dump parser in which languages are we interested;
		// the parser will extract only links starting with the language
		// identifiers
		XMLWikiDumpParser handler = te.new XMLWikiDumpParser();
		handler.initLang("ja");
		handler.initLang("ko");
		handler.initLang("zh");
		handler.setOutputDictFile(args[1]);

		// parse the XML dump & extract vocabulary
		try {
			FileInputStream fis = new FileInputStream(args[0]);
			org.xml.sax.InputSource src = new InputSource(fis);
			src.setEncoding("UTF-8");
			saxParser = factory.newSAXParser();
			saxParser.parse(src, handler);
		} catch (Exception e) {
			e.printStackTrace();
			System.out
					.println("Sorry, something went wrong when opening or reading the input file. Please see the above stack trace for further details.");
			return;
		}

		handler.saveDict();

	}

	/**
	 * XML parser that extracts vocabulary from the <page> elements.
	 * 
	 * @author lz357
	 * 
	 */
	private class XMLWikiDumpParser extends DefaultHandler {
		int counter = 0;
		// current page title
		public String atitle = "";

		// built dictionary dict[lang][word] = <the word's translation>
		public Hashtable<String, Hashtable<String, String>> dict = new Hashtable<String, Hashtable<String, String>>();
		String outputDictFile;

		String buff = "";
		Pattern p = Pattern.compile("\\[\\[([a-z]{2}):(.*)\\]\\]");

		// to keep track of where in xml we are
		Stack<String> s = new Stack<String>();
		String curr;

		// add a dictionary for another language
		public void initLang(String lang) {
			dict.put(lang, new Hashtable<String, String>());
		}

		public void setOutputDictFile(String string) {
			outputDictFile = string;
		}

		public void saveDict() {
			System.out.println("Saving dict...");

			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db;
			try {
				db = dbf.newDocumentBuilder();
			} catch (ParserConfigurationException e1) {
				e1.printStackTrace();
				return;
			}
			Document doc = db.newDocument();

			Element root = doc.createElement("vocabulary");

			doc.appendChild(root);
						
			// write the extracted vocabulary to a file
			try {
				Element e;
				OutputStream os = new FileOutputStream(outputDictFile);				

				for (String lang : dict.keySet()) {
					Hashtable<String, String> h = dict.get(lang);
					for (String k : h.keySet()) {
						e = doc.createElement("word");
						e.setAttribute("lang", lang);
						e.setAttribute("name", k);
						e.setTextContent(h.get(k));						
						root.appendChild(e);
					}
				}				

				TransformerFactory transformerFactory = TransformerFactory.newInstance();
				Transformer transformer = transformerFactory.newTransformer();
				DOMSource source = new DOMSource(doc);
				StreamResult result = new StreamResult(os);
				transformer.transform(source, result);
				os.close();
			} catch (Exception e) {
				e.printStackTrace();
				System.out
						.println("Sorry, something went wrong when trying to write the output file. Please see the above stack trace for further details.");
				return;
			}
		}

		// push elements onto stack, so that we know what text are we currently
		// processing
		@Override
		public void startElement(String uri, String localName, String qName,
				Attributes attributes) throws SAXException {
			// record our whereabouts
			curr = qName.toLowerCase();
			s.push(curr);

			// reset aux variables
			buff = "";
			if (curr.equals("title")) {
				atitle = "";
			}
		}

		// pop elements from the stack (=emerge)
		@Override
		public void endElement(String uri, String localName, String qName)
				throws SAXException {

			// if the page element just ended, we need to search the content
			// for possible entries for our dictionary; such entries are found
			// usually at the end of the content (= wikipedia page) and have the
			// following format: [[<lang>:<page name>]] (e.g. [[cs:Ptakopysk]])
			if (curr == "text") {
				// let the user know we are alive
				System.out.println("Processing: " + atitle);

				// for each line
				Scanner scanner = new Scanner(buff);
				String ln;

				while (scanner.hasNextLine()) {
					ln = scanner.nextLine();

					// if the line has the desired format
					if (ln.matches("\\[\\[[a-z]{2}:.*\\]\\]")) {
						// extract the lang and the page name
						Matcher m = p.matcher(ln);
						if (m.find()) {
							String lang = m.group(1);
							String name = m.group(2);

							// try to add it to a dictionary
							if (dict.containsKey(lang)) {
								dict.get(lang).put(atitle, name);
							}
						}
					}
				}
				scanner.close();
				counter++;
				if (counter % 1 == 0) {
					saveDict();
					System.out.println("** Count: " + counter);
				}
			}

			// update our whereabouts
			curr = s.pop();
		}

		// process text in the xml file
		// mainly just accumulate the text in an aux variable until other
		// procedures
		// pick them up
		@Override
		public void characters(char ch[], int start, int length)
				throws SAXException {
			String str = new String(ch, start, length);
			if (curr.equals("title")) {
				atitle = atitle + str.trim();
			} else if (curr.equals("text")) {
				buff = buff + str;
			}
		}

		// DTD ugly hack.. when the xml parser asks for an external entity,
		// instead of properly acquiring the resource from the internet,
		// shove it an empty xml string
		@Override
		public InputSource resolveEntity(String publicId, String systemId)
				throws IOException, SAXException {
			// TODO unpig this!
			return new InputSource(new ByteArrayInputStream(
					"<?xml version='1.0' encoding='UTF-8'?>".getBytes()));
		}
	};

}

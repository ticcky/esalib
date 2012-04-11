package clldsystem.data;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Stack;



import org.xml.sax.*;
import org.xml.sax.helpers.*;

/**
 * Parses an article stored in a XML file. Functipns emitPage and finish should 
 * be overloaded to implement the desired functionality. This is just an abstract crust
 * which does not do anything with the parsed data.
 * @author zilka
 */
public abstract class WikiArticleParser extends DefaultHandler {

	public String atitle = "";
	public String aid = "";
	public String atext = "";
	boolean article = false;
	boolean body = false;
	Stack<String> s = new Stack<String>();
	String curr;

	protected abstract void emitPage(Integer id, String title, String content);

	protected abstract void finish();

	@Override
	public void startElement(String uri, String localName,
		String qName, Attributes attributes) throws SAXException {
		if (qName.equalsIgnoreCase("page")) {
			article = true;
		} else if (qName.equalsIgnoreCase("text")) {
			body = true;
		}

		curr = qName.toLowerCase();
		s.push(curr);
	}

	@Override
	public void endElement(String uri, String localName, String qName)
		throws SAXException {
		curr = s.pop();
		if (curr.equals("page")) {
			emitPage(Integer.parseInt(aid), atitle, atext);
			atitle = "";
			atext = "";
		}

	}

	public void setId(String id) {
		aid = id;
	}

	public void nextBody(String str) {
		atext = atext + str;
	}

	public void setTitle(String str) {
		atitle = atitle + str;
	}

	@Override
	public void characters(char ch[], int start, int length)
		throws SAXException {
		String str = new String(ch, start, length);
		if (curr.equals("title")) {
			setTitle(str.trim());
		}
		if (body == true) {
			nextBody(str);
		}
		if (curr.equals("id") && !s.contains("revision")) {
			if (str.trim().length() > 0) {
				setId(str.trim());
			}
		}
	}

	@Override
	public InputSource resolveEntity(String publicId, String systemId)
		throws IOException, SAXException {
		// TODO unpig this! This pigness is needed because sometimes the
		//   schema for the xml does not exist and yet we want to parse 
		//   such document. Basically, this resolves the schema to be 
		//   a blank file.
		return new InputSource(
			new ByteArrayInputStream("<?xml version='1.0' encoding='UTF-8'?>".getBytes()));
	}

	@Override
	public void endDocument() throws SAXException {
		super.endDocument();
		finish();
	}
};

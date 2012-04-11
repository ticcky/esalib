package common.wiki;

import de.tudarmstadt.ukp.wikipedia.parser.Paragraph;
import de.tudarmstadt.ukp.wikipedia.parser.ParsedPage;
import de.tudarmstadt.ukp.wikipedia.parser.Section;
import de.tudarmstadt.ukp.wikipedia.parser.mediawiki.MediaWikiParser;
import de.tudarmstadt.ukp.wikipedia.parser.mediawiki.MediaWikiParserFactory;

/**
 * Cleans wikipedia article from the mark-up and returns plaintext.
 * @author zilka
 */
public class WikipediaContentCleaner {
	public static String cleanContent(String content) {
		MediaWikiParserFactory pf = new MediaWikiParserFactory();
		MediaWikiParser parser = pf.createParser();
		ParsedPage pp = parser.parse(content);

		String result = "";
		String par;

		for (Section s : pp.getSections()) {
			result += s.getTitle() + "\n";
			for (Paragraph p : s.getParagraphs()) {
				par = p.getText();
				if (par.startsWith("TEMPLATE")) {
					continue;
				}
				if (par.matches("[^:]+:[^\\ ]+")) {
					continue;
				}
				result += par + "\n\n";
			}
		}

		return result;
	}

}

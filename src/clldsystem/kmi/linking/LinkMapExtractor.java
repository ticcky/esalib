package clldsystem.kmi.linking;

import common.wiki.Wikipedia;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import common.db.DB;
import common.db.DBConfig;
import common.Utils;
import de.tudarmstadt.ukp.wikipedia.parser.Link;
import de.tudarmstadt.ukp.wikipedia.parser.ParsedPage;
import de.tudarmstadt.ukp.wikipedia.parser.Section;
import de.tudarmstadt.ukp.wikipedia.parser.mediawiki.MediaWikiParser;
import de.tudarmstadt.ukp.wikipedia.parser.mediawiki.MediaWikiParserFactory;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * Extracts links out of Wikipedia pages. Similar to LinkingTruth builder, but this
 * is a bit neater.
 * @author zilka
 */
public class LinkMapExtractor {

	DB db;
	Wikipedia w;

	public void setDb(DB db) throws SQLException {
		this.db = db;
		w = new Wikipedia();
		w.setDb(db);
	}

	public static class ExtractedLink {

		public String linkText;
		public String linkTarget;
		public Long sourcePageId;
	}

	public Multimap<String, ExtractedLink> getLinkMap(Long pageId) throws SQLException, IOException {
		String content = w.getContent(pageId);

		Multimap<String, ExtractedLink> linkMap = HashMultimap.create();
		MediaWikiParserFactory pf = new MediaWikiParserFactory();
		MediaWikiParser parser = pf.createParser();
		ParsedPage pp = parser.parse(content);
		for (Section section : pp.getSections()) {
			for (Link link : section.getLinks(Link.type.INTERNAL)) {
				if (link.getTarget().contains(":")) {
					continue;
				}
				String stemmed = Utils.stem(link.getText());
				//stemmed = Utils.unstopwordize(stemmed, stopwords);
				//if(linkMap.containsKey(stemmed) && !link.getTarget().equals(linkMap.get(stemmed))) {
				//System.out.println("Already contains: " + stemmed + " -> " + link.getTarget());
				//System.out.println("Originally: " + stemmed + " -> " + linkMap.get(stemmed));
				//}

				String target = link.getTarget();
				target = target.split("#")[0];
				ExtractedLink el = new ExtractedLink();
				el.linkTarget = target;
				el.linkText = stemmed;
				el.sourcePageId = pageId;

				linkMap.put(stemmed, el);

				// now stem the destination
				String stemmedDest = Utils.stem(target);
				stemmedDest = stemmedDest.replace("_", " ");

				ExtractedLink el2 = new ExtractedLink();
				el2.linkTarget = target;
				el2.linkText = stemmedDest;
				el2.sourcePageId = pageId;
				linkMap.put(stemmedDest, el2);

				ExtractedLink el3 = new ExtractedLink();
				el3.linkTarget = target.replaceAll("[_\\ ]*\\([^\\)]*\\)", "");
				el3.linkText = stemmed;
				el3.sourcePageId = pageId;
				// now if there is a (...) in the dest., try to remove it
				linkMap.put(stemmed, el3);
			}
		}
		return linkMap;

	}

	public Multimap<String, ExtractedLink> getLinkMap(List<Long> similar) throws FileNotFoundException, IOException, SQLException {
		Multimap<String, ExtractedLink> linkMap = HashMultimap.create();

		// load the articles
		PreparedStatement psArticle = db.getConnection().prepareStatement(
			"SELECT p.page_id, t.old_text AS content "
			+ "FROM page p "
			+ "LEFT JOIN revision r ON r.rev_page = p.page_id "
			+ "LEFT JOIN text t on r.rev_text_id = t.old_id "
			+ "WHERE p.page_id = ?");

		// for each similar article
		for (Long i : similar) {
			//if(LinkMapExtractor.forbiddenTopics.contains(i))
			//	continue;
			// get the doc content
			try {
				linkMap.putAll(getLinkMap(i));
			} catch (NullPointerException ex) {
				System.out.println("Error harvesting links from page #"
					+ i);
			}
		}

		return linkMap;
	}

	public static void main(String[] args) throws IOException, FileNotFoundException, SQLException, ClassNotFoundException {
		String similFile = args[0];
		String outLinkMapFile = args[1];
		String dbString = args[2];

		// init
		DBConfig dbcWiki = new DBConfig();
		dbcWiki.setConnectionFromDrupalUrl(dbString); //"mysql://root:root@lwkm012/wikidb_en08");
		DB wikiDb = new DB(dbcWiki);

		List<Long> simil = (List<Long>) Utils.load(similFile);
		LinkMapExtractor lme = new LinkMapExtractor();
		lme.setDb(wikiDb);

		Multimap<String, ExtractedLink> res = lme.getLinkMap(simil);
		Utils.save(res, outLinkMapFile);

	}
}

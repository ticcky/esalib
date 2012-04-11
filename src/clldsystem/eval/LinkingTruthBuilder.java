package clldsystem.eval;

import common.db.DB;
import common.db.DBConfig;
import common.config.AppConfig;
import de.tudarmstadt.ukp.wikipedia.parser.Link;
import de.tudarmstadt.ukp.wikipedia.parser.ParsedPage;
import de.tudarmstadt.ukp.wikipedia.parser.Section;
import de.tudarmstadt.ukp.wikipedia.parser.mediawiki.MediaWikiParser;
import de.tudarmstadt.ukp.wikipedia.parser.mediawiki.MediaWikiParserFactory;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.io.IOUtils;

/**
 * Extracts the linking ground truth out of the given set of wikipedia pages.
 * @author zilka
 */
public class LinkingTruthBuilder {

	DB db;
	PreparedStatement articlePs;
	PreparedStatement conceptPs;

	public LinkingTruthBuilder(DB db, String articleByConceptId, String articleByConceptName) throws SQLException {
		this.db = db;
		articlePs = db.getConnection().prepareStatement(articleByConceptId); //"SELECT * FROM article WHERE concept_id = ? AND lang = ?");
		conceptPs = db.getConnection().prepareStatement(articleByConceptName); //"SELECT * FROM article WHERE title LIKE ? AND lang = ?");
	}

	/**
	 * Extracts the ground truth for a particular page.
	 * @param lang
	 * @param conceptId
	 * @return 
	 */
	public List<Integer> getTruth(String lang, int conceptId) {
		// get a ParsedPage object
		MediaWikiParserFactory pf = new MediaWikiParserFactory();
		MediaWikiParser parser = pf.createParser();

		ArrayList<Integer> res = new ArrayList<Integer>();
		try {
			articlePs.setInt(1, conceptId);
			articlePs.setString(2, lang);

			ResultSet aRes = articlePs.executeQuery();
			aRes.next();

			StringWriter writer = new StringWriter();
			InputStream contentStream = aRes.getBinaryStream("content");
			IOUtils.copy(contentStream, writer, "UTF8");
			String content = writer.toString();

			List<String> links = getLinks(content);

			for (String s : links) {
				conceptPs.setString(1, s);
				conceptPs.setString(2, lang);
				ResultSet cRes = conceptPs.executeQuery();
				if (cRes.next()) {
					res.add(cRes.getInt("concept_id"));
				}

			}

		} catch (IOException ex) {
			Logger.getLogger(LinkingTruthBuilder.class.getName()).log(Level.SEVERE, null, ex);
		} catch (SQLException ex) {
			Logger.getLogger(LinkingTruthBuilder.class.getName()).log(Level.SEVERE, null, ex);
		}

		return res;
	}

	public List<String> getLinks(String content) {
		List<String> res = new ArrayList<String>();

		MediaWikiParserFactory pf = new MediaWikiParserFactory();
		MediaWikiParser parser = pf.createParser();
		ParsedPage pp = parser.parse(content);

		//get the internal links of each section
		for (Section section : pp.getSections()) {
			for (Link link : section.getLinks(Link.type.INTERNAL)) {
				res.add(link.getTarget());
			}
		}
		return res;

	}

	public static void main(String[] args) throws SQLException, ClassNotFoundException {
		//String evalDir = "/home/zilka/devel/kmi/cll/";
		String evalDir = AppConfig.getInstance().getString("LinkingTruthBuilder.evalDir");

		DBConfig dbc = new DBConfig();
		dbc.setConnectionFromDrupalUrl(AppConfig.getInstance().getString("LinkingTruthBuilder.db"));
		String dbStyle = AppConfig.getInstance().getString("LinkingTruthBuilder.dbStyle");
		DB db = new DB(dbc);

		String srcLang = AppConfig.getInstance().getString("LinkingTruthBuilder.srcLang");
		String dstLang = AppConfig.getInstance().getString("LinkingTruthBuilder.dstLang");

		String articleByConceptId = AppConfig.getInstance().getString("LinkingTruthBuilder.query.articleByConceptId");
		String articleByConceptName = AppConfig.getInstance().getString("LinkingTruthBuilder.query.articleByConceptName");
		String articleIter = AppConfig.getInstance().getString("LinkingTruthBuilder.query.articleIter");


		// get articles from the database
		try {
			LinkingTruthBuilder ltb = new LinkingTruthBuilder(db, articleByConceptId, articleByConceptName);
			int currArticle = 0;
			PreparedStatement articlePs;
			articlePs = db.getConnection().prepareStatement(articleIter); //"SELECT * FROM article WHERE lang = ? AND id > ? LIMIT 1");

			// for each article, extract the ground truth
			articlePs.setString(1, srcLang);
			articlePs.setInt(2, currArticle);
			ResultSet articles;
			articles = articlePs.executeQuery();
			do {
				while (articles.next()) {
					int aId = articles.getInt("concept_id");
					System.out.println("Processing article id " + aId);
					StringWriter writer = new StringWriter();
					InputStream contentStream = articles.getBinaryStream("content");
					IOUtils.copy(contentStream, writer, "UTF8");
					String content = writer.toString();

					List<String> lst = ltb.getLinks(content);

					FileOutputStream fos = new FileOutputStream(new File(new File(evalDir, "truth"), new Integer(aId).toString()));
					for (String s : lst) {
						fos.write((s + "\n").getBytes());
					}
					fos.close();

					articlePs.setInt(2, articles.getInt("id"));
				} 
				articles = articlePs.executeQuery();
				if(articles.isAfterLast())
					break;
			} while (true);
		} catch (IOException ex) {
			Logger.getLogger(LinkingTruthBuilder.class.getName()).log(Level.SEVERE, null, ex);
		} catch (SQLException ex) {
			Logger.getLogger(LinkingTruthBuilder.class.getName()).log(Level.SEVERE, null, ex);
		}
	}
}

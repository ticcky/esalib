package clldsystem.kmi.linkers;

import clldsystem.esa.ESAAnalyzer;
import common.db.DB;
import common.ProgressReporter;
import common.wiki.WikipediaContentCleaner;
import java.io.IOException;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import clldsystem.model.Link;
import clldsystem.model.LinkSet;

/**
 * Provides foundation for linkers. Does the stuff that is common for all of them.
 * Expects that linkText will be implemented.
 * @author zilka
 */
public abstract class Linker {

	DB db;
	DB conceptDb;
	DB destDb;
	DB esaDb;
	String resTable;
	String articleIter;
	ESAAnalyzer searcher;
	String srcLang;
	String srcLangStopWordsFile;
	String srcLangStemmer;
	String dstLang;
	int maxLinksPerPage = 1000;
	int specificArticle = -1;

	public void setSpecificArticle(int specificArticle) {
		this.specificArticle = specificArticle;
	}

	public void setMaxLinksPerLinkedPage(int maxLinksPerPage) {
		this.maxLinksPerPage = maxLinksPerPage;
	}

	public void linkArticles() {
		ProgressReporter pr;
		ProgressReporter prWhole = new ProgressReporter("Altogether");
		prWhole.start();
		System.out.println(">> Linking using "
			+ this.getClass().getName());
		System.out.println(">> Dest table: " + getResTableName());

		// prepare the result table
		PreparedStatement psResult = null;
		try {
			psResult = destDb.getConnection().prepareStatement("INSERT INTO "
				+ getResTableName() + " VALUES (?, ?, ?)");
			destDb.executeUpdate("CREATE TABLE IF NOT EXISTS "
				+ getResTableName()
				+ " (page_id INTEGER NOT NULL, links_to INTEGER NOT NULL, score FLOAT, key ndx_page_id(page_id ASC), key ndx_links_to(links_to ASC), key ndx_score(score ASC))");
		} catch (SQLException ex) {
			Logger.getLogger(Linker.class.getName()).log(Level.SEVERE, null, ex);
		}

		// get articles from the database
		try {
			int currArticle = 0;
			int cntr = 0;

			if (specificArticle != -1) {
				currArticle = specificArticle;
			} else {
				// find out the starting article number
				try {
					ResultSet startRes = destDb.executeSelect("SELECT max(page_id) AS page_id FROM "
						+ getResTableName());
					startRes.next();
					currArticle = startRes.getInt("page_id");
					System.out.println(">> Starting from article #"
						+ currArticle);

					startRes = destDb.executeSelect("SELECT count(DISTINCT page_id) AS done FROM "
						+ getResTableName());
					startRes.next();
					cntr = startRes.getInt("done");
				} catch (Exception e) {
					System.out.println(">> Starting from scratch");
				}

				// find out the total number of articles + already done count
			}

			// iterate over all articles in the source database
			PreparedStatement articlePs = db.getConnection().prepareStatement(articleIter); //SELECT * FROM article WHERE lang = ? AND id > ? AND concept_id = 26903"); //299098");
			articlePs.setInt(1, currArticle);


			ResultSet articles;
			articles = articlePs.executeQuery();
			while (articles.next()) {
				cntr++;

				// load the article information
				int pageId = articles.getInt("page_id");
				String pageTitle = articles.getString("page_title");
				InputStream contentStream = articles.getBinaryStream("content");
				String content;
				try {
					content = DB.readBinaryStream(contentStream);
				} catch (IOException e) {
					System.out.println("Error while loading text of article #"
						+ pageId);
					continue;
				}

				content = WikipediaContentCleaner.cleanContent(content);

				// run the linking
				pr = new ProgressReporter(">>> linking of the article");
				pr.start();
				System.out.println("\n>>> Processing page #"
					+ cntr + " (page_id:" + pageId + ")");
				LinkSet ls = linkText(pageTitle, content, pageId);
				ls.removeDuplicates();
				Collections.sort(ls, new Comparator<Link>() {

					public int compare(Link o1, Link o2) {
						return o1.score.compareTo(o2.score)
							* (-1);
					}
				});
				pr.finish();

				// insert the result to the database
				pr = new ProgressReporter(">> store the results");
				pr.start();
				psResult.setInt(1, pageId);

				int linkCounter = 0;
				Iterator<Link> li = ls.iterator();
				while (li.hasNext()) {
					Link l = li.next();
					//System.out.println("INSERT INTO " + resTable + " VALUES(" + pageId + ", " + l.destId + ")");

					psResult.setLong(2, l.destId);
					psResult.setDouble(3, l.score);
					psResult.addBatch();
					linkCounter += 1;
					if (linkCounter == maxLinksPerPage) {
						break;
					}
				}
				psResult.executeBatch();
				pr.finish();
				System.out.println(">> # of extracted links: "
					+ linkCounter);

				// iterate
				if (articles.isLast()) {
					currArticle = articles.getInt("page_id");
					articlePs.setInt(1, currArticle);
					articles = articlePs.executeQuery();
				}

				// if we only want to process one article, end here
				if (specificArticle != -1) {
					break;
				}
			}
		} catch (SQLException ex) {
			Logger.getLogger(Linker.class.getName()).log(Level.SEVERE, null, ex);
		}
		prWhole.finish();
	}

	abstract LinkSet linkText(String pageTitle, String content, int pageId);

	public Linker(DB db, DB conceptDb, DB esaDb, DB destDb, String resTable, String articleIter, String srcLang, String srcLangStopWordsFile, String srcLangStemmer, String dstLang) {
		this.db = db;
		this.conceptDb = conceptDb;
		this.esaDb = esaDb;
		this.destDb = destDb;
		this.resTable = resTable;
		this.articleIter = articleIter;

		this.srcLang = srcLang;
		this.srcLangStopWordsFile = srcLangStopWordsFile;
		this.srcLangStemmer = srcLangStemmer;
		this.dstLang = dstLang;

		try {
			searcher = new ESAAnalyzer(esaDb, srcLang, srcLangStopWordsFile, srcLangStemmer);
		} catch (ClassNotFoundException ex) {
			Logger.getLogger(Linker.class.getName()).log(Level.SEVERE, null, ex);
		} catch (SQLException ex) {
			Logger.getLogger(Linker.class.getName()).log(Level.SEVERE, null, ex);
		} catch (IOException ex) {
			Logger.getLogger(Linker.class.getName()).log(Level.SEVERE, null, ex);
		}
	}

	protected int getConcept(int i) {
		try {
			//PreparedStatement srcConceptPs = db.getConnection().prepareStatement("SELECT ll.ll_from AS concept_id FROM wikidb_cs.page p LEFT JOIN wikidb_cs.langlinks ll ON p.page_title = ll_title WHERE p.page_id = ?");
			PreparedStatement srcConceptPs = db.getConnection().prepareStatement("SELECT concept_id FROM concept_mapping WHERE page_id = ?");
			srcConceptPs.setInt(1, i);
			ResultSet res = srcConceptPs.executeQuery();
			if (res.next()) {
				return res.getInt("concept_id");
			}
		} catch (SQLException ex) {
			Logger.getLogger(Linker.class.getName()).log(Level.SEVERE, null, ex);
		}
		return -1;
	}

	protected String[] splitText(String text) {
		return new String[]{text};
	}

	protected String getResTableName() {
		return resTable;
	}
}

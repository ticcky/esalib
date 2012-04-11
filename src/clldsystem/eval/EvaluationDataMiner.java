package clldsystem.eval;

import common.db.DB;
import common.db.DBConfig;
import common.wiki.WikipediaTitleResolver;
import common.config.AppConfig;
import de.tudarmstadt.ukp.wikipedia.parser.Link;
import de.tudarmstadt.ukp.wikipedia.parser.ParsedPage;
import de.tudarmstadt.ukp.wikipedia.parser.Section;
import de.tudarmstadt.ukp.wikipedia.parser.mediawiki.MediaWikiParser;
import de.tudarmstadt.ukp.wikipedia.parser.mediawiki.MediaWikiParserFactory;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Builds a set of pages for evaluation. This is based on having a few pages
 * as a core, which go to this set. Pages that are directly linked to from 
 * those core pages also go to this set.
 * @author zilka
 */
public class EvaluationDataMiner {
	private final String articleQuery;
	private final int start;
	private final DB db;
	private final DB dbCl;
	private final DB destDb;
	private final String selectionQuery;
	private final String selectionTable;
	private final String selectionClosureTable;
	private final String truthTable;
	private PreparedStatement ps;
	List<Integer> forgottenDocs;
	List<Integer> evalDocs;
	WikipediaTitleResolver titleResolver;
	String truthTag = "";
	boolean dryRun = false;

	public EvaluationDataMiner(DB db, DB dbCl, DB destDb, String selectionQuery, String articleQuery, String selectionTable, String selectionClosureTable, String truthTable, int start) throws FileNotFoundException, SQLException {
		this.db = db;
		this.dbCl = dbCl;
		this.destDb = destDb;
		this.selectionTable = selectionTable;
		this.selectionClosureTable = selectionClosureTable;
		this.selectionQuery = selectionQuery;
		this.articleQuery = articleQuery;
		this.truthTable = truthTable;
		this.start = start;

		this.titleResolver = new WikipediaTitleResolver(db);
	}

	public void build() {

		System.out.println("> building " + truthTable);
		PreparedStatement insTruth = null;
		try {
			// first select the core collection
			if (!dryRun) {
				db.executeUpdate("DROP TABLE IF EXISTS "
					+ selectionTable);
				db.executeUpdate("CREATE TABLE "
					+ selectionTable + " AS "
					+ selectionQuery);
				db.executeUpdate("ALTER TABLE " + selectionTable
					+ " ADD INDEX ndx_id (page_id ASC)");


				//destDb.executeUpdate("DROP TABLE IF EXISTS " + truthTable);
				destDb.executeUpdate("CREATE TABLE IF NOT EXISTS "
					+ truthTable
					+ " (page_id INTEGER NOT NULL, links_to INTEGER NOT NULL, tag varchar(255) null)");
				try {
					destDb.executeUpdate("ALTER TABLE "
						+ truthTable
						+ " ADD INDEX ndx_id (page_id ASC)");
				} catch (SQLException e) {
				}
				try {
					destDb.executeUpdate("ALTER TABLE "
						+ truthTable
						+ " ADD INDEX ndx_links_to (links_to ASC)");
				} catch (SQLException e) {
				}
			}

			// go through the links of the core pages and put their targets to the resulting set
			buildClosure();

		} catch (SQLException ex) {
			Logger.getLogger(EvaluationDataMiner.class.getName()).log(Level.SEVERE, null, ex);
		}

	}

	/**
	 * Gets a set of links from a wikipedia markup content.
	 * @param content
	 * @return 
	 */
	Set<Long> getLinkSet(String content) {
		Set<Long> res = new HashSet<Long>();

		MediaWikiParserFactory pf = new MediaWikiParserFactory();
		MediaWikiParser parser = pf.createParser();
		ParsedPage pp = parser.parse(content);

		//get the internal links of each section
		long linkDest;
		int cntr = 0;
		int bad = 0;
		for (Section section : pp.getSections()) {
			for (Link link : section.getLinks(Link.type.INTERNAL)) {
				String target = link.getTarget();
				if (target.matches("^.{2,12}:.*")
					|| target.contains("#")) {
					continue;
				}
				linkDest = -1;
				try {
					linkDest = titleResolver.getConceptIdFromTitle(target);
				} catch (SQLException ex) {
					Logger.getLogger(EvaluationDataMiner.class.getName()).log(Level.SEVERE, null, ex);
				} catch (IOException ex) {
					Logger.getLogger(EvaluationDataMiner.class.getName()).log(Level.SEVERE, null, ex);
				}
				cntr++;
				if (linkDest != -1) {
					res.add(linkDest);
				} else {
					bad++;
					System.out.print(link.getTarget() + ", ");
				}
			}
		}

		System.out.print("> successfully resolved ");
		System.out.println((cntr - bad) + "/" + cntr + " links ("
			+ ((float) (cntr
			- bad)) / cntr * 100 + " %)");
		return res;
	}

	private List<Integer> getPageIds(String string) {
		ResultSet pages = db.executeSelect("SELECT page_id FROM "
			+ string);
		List<Integer> res = new ArrayList<Integer>();
		try {
			while (pages.next()) {
				res.add(pages.getInt("page_id"));
			}
		} catch (SQLException ex) {
			Logger.getLogger(EvaluationDataMiner.class.getName()).log(Level.SEVERE, null, ex);
		}

		return res;


	}

	/**
	 * Go through the core pages and put all their link targets to the resulting set.
	 */
	private void buildClosure() {
		try {
			PreparedStatement psInsert = destDb.getConnection().prepareStatement("INSERT INTO "
				+ truthTable
				+ " SET page_id = ?, links_to = ?, tag = ?");
			PreparedStatement ps = db.getConnection().prepareStatement("SELECT DISTINCT(page_id), page_title, old_text AS content, page_len FROM "
				+ selectionTable
				+ " INNER JOIN revision ON page_id = rev_page INNER JOIN text ON rev_text_id = old_id WHERE page_id > ? ORDER BY page_id LIMIT 100");
			int pageCntr = 0;
			ps.setInt(1, pageCntr);
			ResultSet res = ps.executeQuery();
			while (res.next()) {
				System.out.println(">> "
					+ res.getString("page_title") + " ("
					+ res.getString("page_len") + " bytes)");
				String content = DB.readBinaryStream(res.getBinaryStream("content"));
				psInsert.setInt(1, res.getInt("page_id"));
				psInsert.setString(3, truthTag);
				Set<Long> links = getLinkSet(content);
				for (long l : links) {
					psInsert.setLong(2, l);
					psInsert.addBatch();
				}
				if (!dryRun) {
					psInsert.executeBatch();
				}

				if (res.isLast()) {
					ps.setInt(1, res.getInt("page_id"));
					res = ps.executeQuery();
				}
			}
			if (!dryRun) {
				db.executeUpdate("DROP TABLE IF EXISTS "
					+ selectionClosureTable);
				db.executeUpdate("CREATE TABLE "
					+ selectionClosureTable
					+ " AS SELECT DISTINCT p.* FROM "
					+ destDb.getConnection().getCatalog()
					+ "."
					+ truthTable
					+ " c LEFT JOIN page p ON c.links_to = p.page_id");
				db.executeUpdate("ALTER TABLE "
					+ selectionClosureTable
					+ " ADD INDEX ndx_id (page_id ASC)");
			}

			//dbCl.executeUpdate("SELECT pc.* FROM " + db.getConnection().getCatalog() + ".page_l p LEFT JOIN " + db.getConnection().getCatalog() + ".concept_mapping cm ON p.page_id = cm.page_id INNER JOIN page pc ON cm.concept_id = pc.page_id");
		} catch (IOException ex) {
			Logger.getLogger(EvaluationDataMiner.class.getName()).log(Level.SEVERE, null, ex);
		} catch (SQLException ex) {
			Logger.getLogger(EvaluationDataMiner.class.getName()).log(Level.SEVERE, null, ex);
		}

	}

	private void setTruthTag(String truthTag) {
		this.truthTag = truthTag;
	}
	
	public static void main(String[] args) throws FileNotFoundException, SQLException, ClassNotFoundException {
		String configId = AppConfig.getInstance().getString("EvaluationDataMinerConfigId");

		String articleQuery = AppConfig.getInstance().getString("EvaluationDataMiner"
			+ configId + ".query.article");
		String selectionQuery = AppConfig.getInstance().getString("EvaluationDataMiner"
			+ configId + ".query.selection");
		String truthTable = AppConfig.getInstance().getString("EvaluationDataMiner"
			+ configId + ".truthTable");
		String truthTag = AppConfig.getInstance().getString("EvaluationDataMiner"
			+ configId + ".truthTag");
		String selectionTable = AppConfig.getInstance().getString("EvaluationDataMiner"
			+ configId + ".selectionTable");
		String selectionClosureTable = AppConfig.getInstance().getString("EvaluationDataMiner"
			+ configId + ".selectionClosureTable");

		DBConfig dbc = new DBConfig();
		dbc.setConnectionFromDrupalUrl(AppConfig.getInstance().getString("EvaluationDataMiner"
			+ configId + ".db"));

		//DBConfig dbcCl = new DBConfig();
		//dbcCl.setConnectionFromDrupalUrl(AppConfig.getInstance().getString("EvaluationDataMiner" + configId + ".clDb"));

		DBConfig dbcDest = new DBConfig();
		dbcDest.setConnectionFromDrupalUrl(AppConfig.getInstance().getString("EvaluationDataMiner"
			+ configId + ".destDb"));

		DB destDb = new DB(dbcDest);
		DB db = new DB(dbc);
		DB dbCl = null; //new DB(dbcCl);

		int start = 0;

		EvaluationDataMiner edb = new EvaluationDataMiner(db, dbCl, destDb, selectionQuery, articleQuery, selectionTable, selectionClosureTable, truthTable, start);
		edb.setTruthTag(truthTag);
		edb.build();

	}
}

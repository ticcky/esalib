package clldsystem.tools;

import common.db.DB;
import common.db.DBConfig;
import common.config.AppConfig;
import java.sql.SQLException;

/**
 * Prepares the MediaWiki database for further use.
 * @author zilka
 */
public class PrepareWikiDb {

	public static void main(String[] args) throws SQLException, ClassNotFoundException {
		AppConfig cfg = AppConfig.getInstance();
		cfg.setSection("PrepareWikiDb");

		String connStr = cfg.getSString("db");
		String disambigStr = cfg.getSString("disambigStr");
		String hnDisambigStr = cfg.getSString("hnDisambigStr");
		String lang = cfg.getSString("lang");

		DBConfig dbc = new DBConfig();
		dbc.setConnectionFromDrupalUrl(connStr);

		DB db;
		db = new DB(dbc);
		int stage = 0;
		if(args.length > 0)
			stage = Integer.parseInt(args[0]);

		switch (stage) {
			case 1:
				try {
					db.executeUpdate("ALTER IGNORE TABLE page DROP INDEX index2");
				} catch (SQLException e) {
				}



				try {
					db.executeUpdate("ALTER IGNORE TABLE text DROP INDEX index2");
				} catch (SQLException e) {
				}



				try {
					db.executeUpdate("ALTER IGNORE TABLE revision DROP INDEX index2");
				} catch (SQLException e) {
				}



				try {
					db.executeUpdate("ALTER IGNORE TABLE revision DROP INDEX index1");
				} catch (SQLException e) {
				}



			case 2:
				try {
					db.executeUpdate("DROP TABLE page_concepts");
				} catch (SQLException e) {
				}



				try {
					db.executeUpdate("DROP TABLE page_disambig");
				} catch (SQLException e) {
				}



				try {
					db.executeUpdate("DROP TABLE concept_mapping");
				} catch (SQLException e) {
				}



				try {
					db.executeUpdate("DROP TABLE redirect_mapping");
				} catch (SQLException e) {
				}

			case 3:
				// add indexes
				db.executeUpdate(
					"ALTER IGNORE TABLE page ADD INDEX index1 (page_id ASC)");
				db.executeUpdate(
					"ALTER IGNORE TABLE page ADD INDEX index2 (page_title ASC)");
				db.executeUpdate(
					"ALTER IGNORE TABLE text ADD INDEX index2 (old_id ASC)");
				db.executeUpdate(
					"ALTER TABLE redirect ADD INDEX index3 (rd_title ASC) , ADD INDEX index4 (rd_from ASC);");
				db.executeUpdate(
					"ALTER IGNORE TABLE revision ADD INDEX index1 (rev_text_id ASC), ADD INDEX index2 (`rev_page` ASC);");

			case 4:
				// select only pages that are in default namespace and not redirects
				db.executeUpdate(
					"CREATE TABLE page_concepts AS SELECT * FROM page WHERE page_namespace = 0 AND page_is_redirect = 0");

			case 5:
				// find disambiguation pages
				db.executeUpdate(
					"CREATE TABLE page_disambig AS SELECT rev_page FROM revision r LEFT JOIN text t ON r.rev_text_id = t.old_id WHERE t.old_text LIKE \"%{{" + disambigStr + "%\" OR t.old_text LIKE \"%{{" + hnDisambigStr + "%\"");
				db.executeUpdate(
					"ALTER TABLE page_concepts ADD INDEX ndx_page_id (page_id ASC)");
				db.executeUpdate(
					"ALTER TABLE page_disambig ADD INDEX ndx_rev_page (rev_page ASC)");

			case 6:
				// delete disambiguation pages
				db.executeUpdate(
					"DELETE page_concepts FROM page_concepts INNER JOIN page_disambig pd ON page_id = pd.rev_page");




			case 7:
				try {
					db.executeUpdate("DROP TABLE IF EXISTS concept_mapping");
					db.executeUpdate("CREATE TABLE concept_mapping AS SELECT langlinks.ll_from as concept_id, page.page_id as page_id FROM page LEFT JOIN langlinks ON langlinks.ll_title = page.page_title WHERE ll_lang = '" + lang + "' AND ll_title != ''");
					db.executeUpdate("ALTER TABLE concept_mapping ADD INDEX ndx_page_id (page_id ASC), ADD INDEX ndx_concept_id (concept_id ASC)");
				} catch (Exception e) {
					System.out.println("> concept_mapping cannot be created: " + e);
				}
			System.out.println("> concept_mapping has been created");

			case 8:
				try {
					db.executeUpdate("DROP TABLE IF EXISTS redirect_mapping");
					db.executeUpdate("CREATE TABLE redirect_mapping AS SELECT redirect.rd_from as page_id, page.page_id as dest_page_id FROM redirect LEFT JOIN page ON replace(redirect.rd_title, '_', ' ') = page.page_title");
					db.executeUpdate("ALTER TABLE redirect_mapping ADD INDEX ndx_page_id (page_id ASC), ADD INDEX ndx_dest_page_Id (dest_page_id ASC)");
				} catch (Exception e) {
					System.out.println("> redirect_mapping cannot be created: " + e);
				}
			System.out.println("> redirect_mapping has been created");

			case 9:
				// delete all redirect pages from the page redirect
				db.executeUpdate("DELETE page_concepts FROM page_concepts INNER JOIN redirect_mapping rm ON page_concepts.page_id = rm.page_id");

				db.executeUpdate("ALTER TABLE revision ADD INDEX ndx_rev_page (rev_page ASC)");
		}

	}
}

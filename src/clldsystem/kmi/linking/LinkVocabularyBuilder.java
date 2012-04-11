package clldsystem.kmi.linking;

import au.com.bytecode.opencsv.CSVReader;
import common.db.DB;
import common.db.DBConfig;
import common.ProgressReporter;
import common.config.AppConfig;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Inserts information about links in the whole wikipedia, extracted by wikipedia-miner
 * into the anchor-summary file.
 * to database.
 * @author zilka
 */
public class LinkVocabularyBuilder {

	static String buildQuery(String common, List<String> items) {
		StringBuilder sb = new StringBuilder();
		sb.append(common);
		int cntr = 0;
		for (String s : items) {
			sb.append("(");
			sb.append(s);
			sb.append(")");
			if (!(cntr == items.size() - 1)) {
				sb.append(",");
			}
			cntr++;
		}
		return sb.toString();
	}

	public static String cleanse(String s) {
		return s.replaceAll("\\W", " ");
	}

	public static void main(String[] args) throws SQLException, FileNotFoundException, IOException, Exception {
		AppConfig cfg = AppConfig.getInstance();
		cfg.setSection("LinkVocabularyBuilder");

		DBConfig dbc = new DBConfig();
		dbc.setConnectionFromDrupalUrl(cfg.getSString("db"));

		DB db = new DB(dbc);

		String inputFile = cfg.getSString("inputFile");

		db.executeUpdate("DROP TABLE IF EXISTS lv_anchors");
		db.executeUpdate("DROP TABLE IF EXISTS lv_freqs");
		db.executeUpdate("CREATE TABLE lv_anchors (id INTEGER NOT NULL, anchor VARCHAR(500))");
		db.executeUpdate("CREATE TABLE lv_freqs (anchor_id INTEGER NOT NULL, page_id INTEGER NOT NULL, freq INTEGER NOT NULL)");


		Map<String, Integer> idMap = new HashMap<String, Integer>();
		String insQuery = "INSERT INTO lv_freqs (anchor_id, page_id, freq) VALUES ";
		List<String> insBuffer = new ArrayList<String>();

		String aInsQuery = "INSERT INTO lv_anchors (id, anchor) VALUES ";
		List<String> aInsBuffer = new ArrayList<String>();


		CSVReader reader = new CSVReader(new FileReader(inputFile), ',', '\"');

		int labelCntr = 1;
		int lineCntr = 0;

		ProgressReporter pr = new ProgressReporter("Link-vocabulary building");
		pr.start();

		String[] nextLine;
		while ((nextLine = reader.readNext()) != null) {
			lineCntr++;
			String[] parts = nextLine[1].split(";");
			int anchorId;
			String anchor = cleanse(nextLine[0]);

			if (idMap.containsKey(anchor)) {
				anchorId = idMap.get(anchor);
			} else {
				idMap.put(anchor, labelCntr);
				aInsBuffer.add(labelCntr + ", '" + anchor
					+ "'");
				anchorId = labelCntr;
				labelCntr++;
			}

			for (String part : parts) {
				insBuffer.add(anchorId + ","
					+ part.replace(":", ","));
				try {
					String x[] = part.split(":");
					for (String xx : x) {
						Integer.parseInt(xx);
					}
				} catch (Exception e) {
					System.out.println("line Number:"
						+ lineCntr);
					throw e;
				}
			}

			if (anchorId % 100000 == 0) {
				String q = "";
				if (insBuffer.size() > 0) {
					try {
						q = buildQuery(insQuery, insBuffer);
						db.executeUpdate(q);
						insBuffer.clear();
					} catch (Exception e) {
						System.out.println(q);
						System.exit(0);
					}
				}
				if (aInsBuffer.size() > 0) {
					try {
						q = buildQuery(aInsQuery, aInsBuffer);
						db.executeUpdate(q);
						aInsBuffer.clear();
					} catch (Exception e) {
						System.out.println(q);
						System.exit(0);
					}
				}
			}
			pr.report();
		}
		String q;
		q = buildQuery(insQuery, insBuffer);
		db.executeUpdate(q);
		q = buildQuery(aInsQuery, aInsBuffer);
		db.executeUpdate(q);
		pr.finish();

	}
}

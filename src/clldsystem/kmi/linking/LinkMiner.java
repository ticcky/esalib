package clldsystem.kmi.linking;

import common.db.DB;
import common.db.DBConfig;
import common.db.DBDatasetIterator;
import common.ProgressReporter;
import common.config.AppConfig;
import de.tudarmstadt.ukp.wikipedia.parser.Link;
import de.tudarmstadt.ukp.wikipedia.parser.ParsedPage;
import de.tudarmstadt.ukp.wikipedia.parser.mediawiki.MediaWikiParser;
import de.tudarmstadt.ukp.wikipedia.parser.mediawiki.MediaWikiParserFactory;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Extracts links from the given set of articles.
 * @author zilka
 */
public class LinkMiner {

	MediaWikiParser parser;

	public LinkMiner() {
		MediaWikiParserFactory pf = new MediaWikiParserFactory();
		parser = pf.createParser();
	}

	public String stem(String str) {
		return str;
	}

	public void countLinks(List<String> input, Map<String, Integer> cntMap) {
		for (String l : input) {
			l = stem(l);

			int cntr = 0;
			if (cntMap.containsKey(l)) {
				cntr = cntMap.get(l);
			}
			cntr++;
			cntMap.put(l, cntr);
		}

	}

	public List<String> mine(String content) {
		List<String> result = new ArrayList<String>();

		// parse the content by the wikipedia parser
		ParsedPage pp = parser.parse(content);

		// return all links on the page
		for (Link l : pp.getLinks()) {
			result.add(l.getText());
		}

		return result;
	}

	public void saveMap(Map<String, Integer> cntMap, String outFile) throws IOException {
		FileWriter fw = new FileWriter(outFile);
		for (String key : cntMap.keySet()) {
			fw.write(key + ";" + cntMap.get(key) + "\n");
		}
	}

	public static void main(String[] args) throws SQLException, IOException, ClassNotFoundException {
		AppConfig cfg = AppConfig.getInstance();
		cfg.setSection("LinkMiner");

		DBConfig dbc = new DBConfig();
		dbc.setConnectionFromDrupalUrl(cfg.getSString("db"));
		DB db = new DB(dbc);

		String articleIter = cfg.getSString("query.articleIter");

		DBDatasetIterator iter = new DBDatasetIterator(db, articleIter);
		iter.setIdField("page_id");

		ProgressReporter pr = new ProgressReporter("Link Miner");
		pr.start();

		Map<String, Integer> cntMap = new HashMap<String, Integer>();

		LinkMiner lm = new LinkMiner();

		int i = 0;
		while (iter.next()) {
			int id = iter.getRes().getInt("page_id");
			String title = iter.getRes().getString("page_title");
			String content = DB.readBinaryStream(iter.getRes().getBinaryStream("content"));

			//System.out.println(">> Mining: " + title + " (" + id + ")");

			List<String> newLinks = lm.mine(content);
			lm.countLinks(newLinks, cntMap);

			if (i % 1 == 0) {
				lm.saveMap(cntMap, "/xdisk/tmp/map.txt");
			}

			pr.report();
			i++;
		}
		pr.finish();
	}
}

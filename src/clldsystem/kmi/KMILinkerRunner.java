package clldsystem.kmi;

import clldsystem.kmi.linkers.CLLinker;
import common.db.DB;
import common.db.DBConfig;
import common.config.AppConfig;
import java.sql.SQLException;
import java.util.List;

/**
 * Launches linking based only on similarity computation (not NTCIR).
 * @author zilka
 */
public class KMILinkerRunner {

	public static void main(String[] args) throws SQLException, ClassNotFoundException {
		// load configuration
		AppConfig cfg = AppConfig.getInstance();
		cfg.setSection("LinkerRunner");

		DBConfig dbc = new DBConfig();
		dbc.setConnectionFromDrupalUrl(cfg.getSString("db"));

		DBConfig dbcConcept = new DBConfig();
		dbcConcept.setConnectionFromDrupalUrl(cfg.getSString("conceptDb"));

		DBConfig dbcEsa = new DBConfig();
		dbcEsa.setConnectionFromDrupalUrl(cfg.getSString("esaDb"));

		DBConfig dbcEsaIndex = new DBConfig();
		dbcEsaIndex.setConnectionFromDrupalUrl(cfg.getSString("esaIndexDb"));

		DBConfig dbcEsaIndexCache = new DBConfig();
		dbcEsaIndexCache.setConnectionFromDrupalUrl(cfg.getSString("esaIndexCacheDb"));

		DBConfig dbcDest = new DBConfig();
		dbcDest.setConnectionFromDrupalUrl(cfg.getSString("destDb"));

		DB db = new DB(dbc);
		DB dbConcept = new DB(dbcConcept);
		DB dbEsa = new DB(dbcEsa);
		DB dbEsaIndex = new DB(dbcEsaIndex);
		DB dbEsaIndexCache = new DB(dbcEsaIndexCache);
		DB dbDest = new DB(dbcDest);

		String srcLang = cfg.getSString("srcLang");
		String srcLangStopWordsFile = cfg.getSString("srcLangStopwordsFile");
		String srcLangStemmer = cfg.getSString("srcLangStemmerClass");
		String dstLang = cfg.getSString("dstLang");

		String articleIter = cfg.getSString("articleIter");

		String esaIndexTable = cfg.getSString("esaIndexTable");
		String esaIndexPath = cfg.getSString("esaIndexPath");
		Integer maxLinksPerLinkedPage = cfg.getSInt("maxLinksPerLinkedPage");

	
		//List<String> cllMethods = cfg.getSList("cllMethods");
		List<String> cllMethods = cfg.getSList("cllMethods.method[@id]");
		int nExperiments = cllMethods.size();

		// 1 - mono_vector
		// 2 - cl_n_vector
		// 3 - cl_n_par_vector
		// 4 - cl_similar
		// 5 - cl_best_esa_sum
		// 6 - cl_similar_similar

		boolean forParagraphs = false;
		String tag = "fast";

		if (args.length >= 2) {
			if (args[1].equals("true")) {
				forParagraphs = true;
			}
		}

		// go through all required linkings and run them
		CLLinker cll;
		for(int i = 0; i < nExperiments; i++ ) { // String method : cllMethods) {
			int n = Integer.parseInt(cllMethods.get(i));
			String cfgPath = "cllMethods.method(" + i + ").";

			System.out.println("> Running linking #" + n + " (" + (i + 1) + "/" + nExperiments + ")");
			//switch (testCase) {
			switch (n) {
				default:
				/*
				// mono_vector
				case 1:
					MonoLingualLinker mll = new MonoLingualLinker(db, dbConcept, dbEsa, dbDest, "1", articleIter, srcLang, srcLangStopWordsFile, srcLangStemmer, dstLang);
					mll.linkArticles();
					break;
				// cl_n_vector
				case 2:
					cll = new CLLinker(db, dbConcept, dbEsa, dbDest, dbEsaIndex, "_textcleaned", articleIter, srcLang, srcLangStopWordsFile, srcLangStemmer, dstLang, esaIndexTable);
					cll.setN(5);
					cll.setnDimensions(500);
					cll.setSplitBeforeSrcEsa(forParagraphs);
					cll.setSplitBeforeDstEsa(false);
					cll.linkArticles();
					break;
				// cl_n_par_vector
				case 3:
					cll = new CLLinker(db, dbConcept, dbEsa, dbDest, dbEsaIndex, "_textcleaned", articleIter, srcLang, srcLangStopWordsFile, srcLangStemmer, dstLang, esaIndexTable);
					cll.setN(5);
					cll.setnDimensions(500);
					cll.setSplitBeforeSrcEsa(forParagraphs);
					cll.setSplitBeforeDstEsa(true);
					cll.linkArticles();
					break;
				 */
				// cl_similar
				case 4:
					cll = new CLLinker(db, dbConcept, dbEsa, dbDest, dbEsaIndex, dbEsaIndexCache, "" + tag, articleIter, srcLang, srcLangStopWordsFile, srcLangStemmer, dstLang, esaIndexTable);
					cll.setLinkExtractionMethod(CLLinker.SIMILAR_DOCUMENTS);
					cll.setN(cfg.getSInt(cfgPath + "N"));
					cll.setSplitBeforeSrcEsa(forParagraphs);
					cll.setSplitBeforeDstEsa(false);
					cll.setEsaIndexPath(esaIndexPath);
					cll.setMaxLinksPerLinkedPage(maxLinksPerLinkedPage);
					cll.linkArticles();
					break;
				// cl_best_esa_sum
				case 5:
					cll = new CLLinker(db, dbConcept, dbEsa, dbDest, dbEsaIndex, dbEsaIndexCache, "" + tag, articleIter, srcLang, srcLangStopWordsFile, srcLangStemmer, dstLang, esaIndexTable);
					cll.setLinkExtractionMethod(CLLinker.ESA_VECTOR_SUM);
					cll.setN(cfg.getSInt(cfgPath + "N"));
					cll.setnDimensions(cfg.getSInt(cfgPath + "nDimensions"));
					cll.setSplitBeforeSrcEsa(forParagraphs);
					cll.setEsaIndexPath(esaIndexPath);
					cll.setMaxLinksPerLinkedPage(maxLinksPerLinkedPage);
					cll.linkArticles();
					break;
				// cl_similar_similar
				case 6:
					cll = new CLLinker(db, dbConcept, dbEsa, dbDest, dbEsaIndex, dbEsaIndexCache, "" + tag, articleIter, srcLang, srcLangStopWordsFile, srcLangStemmer, dstLang, esaIndexTable);
					cll.setLinkExtractionMethod(CLLinker.SIMILAR_SIMILAR);
					cll.setN(cfg.getSInt(cfgPath + "N"));
					cll.setN2(cfg.getSInt(cfgPath + "N2"));
					cll.setSplitBeforeSrcEsa(forParagraphs);
					cll.setEsaIndexPath(esaIndexPath);
					cll.setMaxLinksPerLinkedPage(maxLinksPerLinkedPage);
					cll.linkArticles();
					break;

				// cl_wikipedia_links
				case 7:
					cll = new CLLinker(db, dbConcept, dbEsa, dbDest, dbEsaIndex, dbEsaIndexCache, "" + tag, articleIter, srcLang, srcLangStopWordsFile, srcLangStemmer, dstLang, esaIndexTable);
					cll.setLinkExtractionMethod(CLLinker.USE_WIKIPEDIA_LINKS);
					cll.setMaxLinksPerLinkedPage(maxLinksPerLinkedPage);
					//cll.setMaxLinksPerLinkedPage(cfg.getSInt(cfgPath + "maxLinksPerLinkedPage"));
					cll.setN(cfg.getSInt(cfgPath + "N"));
					cll.setSplitBeforeSrcEsa(forParagraphs);
					cll.setEsaIndexPath(esaIndexPath);
					//cll.setSpecificArticle(21 - 1); // -1 because of the > operator
					cll.linkArticles();
					break;
			}
		}
	}
}

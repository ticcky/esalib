/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package clldsystem.esa;

import clldsystem.kmi.linking.ConceptMapper;
import common.db.DB;
import common.db.DBConfig;
import common.HeapSort;
import common.ProgressReporter;
import common._;
import common.config.AppConfig;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import clldsystem.model.LinkSet;

/**
 * Attempts to retrieve cross-lingually sematically simialr documents from the 
 * ESA vector index.
 */
public class ESAGoogle {

	DB db;
	DB esaDb;
	DB indexDb;
	DB indexCacheDb;
	String esaIndexPath;
	String lang;
	String stopWords;
	String stemmerClass;
	HashMap<Integer, List<Integer>> conceptMapping;
	ConceptMapper cm;

	public void init() throws SQLException {
		cm = new ConceptMapper();
		cm.loadFromDb(db);
	}

	public void setDb(DB db) {
		this.db = db;
	}

	public void setEsaDb(DB esaDb) {
		this.esaDb = esaDb;
	}

	public void setIndexDb(DB indexDb) {
		this.indexDb = indexDb;
	}

	public void setEsaIndexPath(String esaIndexPath) {
		this.esaIndexPath = esaIndexPath;
	}

	public void setStemmerClass(String stemmerClass) {
		this.stemmerClass = stemmerClass;
	}

	public void setStopWords(String stopWords) {
		this.stopWords = stopWords;
	}

	public void setLang(String lang) {
		this.lang = lang;
	}

	public void setIndexCacheDb(DB indexCacheDb) {
		this.indexCacheDb = indexCacheDb;
	}

	private List<Integer> mapConcept(int i) {
		List<Integer> res = conceptMapping.get(i);
		return res;
	}

	public void search(String s) throws ClassNotFoundException {
		try {
			ESAAnalyzer searcher = new ESAAnalyzer(esaDb, lang, stopWords, stemmerClass);
			ProgressReporter pr = new ProgressReporter(">>> extract source vectors");
			pr.start();

			// find ESA concepts for each part
			IConceptVector cv = searcher.getConceptVector(s);
			pr.finish();

			pr = new ProgressReporter(">>> map dimensions & prepare search vector");
			pr.start();
			// prepare the concept vector for searching (= select top N dimensions)
			ByteArrayOutputStream baos = new ByteArrayOutputStream(50000);
			DataOutputStream tdos = new DataOutputStream(baos);
			try {
				// set the number of dimensions for searching
				int ccount = 150;

				// write the vector length
				tdos.writeInt(ccount);

				// build the vector, and skip over the concepts
				// that cannot be CL mapped ( = concepts that don't
				// have equivalent page in the other language's wiki
				Collection<Long> mappedConcepts;
				int cntr = ccount;
				int cntrSkipped = 0;
				int cCntr = 0;
				int cCount = 0;
				int writtenConcepts = 0;
				//List<Integer> conceptsMapped = mapConcepts(conceptList);

				int[] ids = new int[cv.size()];
				double[] vals = new double[cv.size()];
				IConceptIterator it = cv.orderedIterator();
				while (it.next()) {
					ids[cCount] = it.getId();
					vals[cCount] = it.getValue();
					cCount++;
				}
				HeapSort.heapSort(vals, ids);

				//while (cCntr < cv.size() - ccount) {
				//	it.next();
				//	cCntr++;
				//}
				cntr = 0;
				while (cCntr < ccount && cCntr < cCount) {
					mappedConcepts = cm.getPageIds(ids[ids.length - cCntr - 1]); // mapConcept(ids[ids.length - cCntr - 1]);

					// if the mapping exists, write
					if (mappedConcepts != null && mappedConcepts.size()
						> 0) {
						for (Long mappedConcept : mappedConcepts) {
							tdos.writeInt(mappedConcept.intValue());
							tdos.writeFloat((float) vals[vals.length - cCntr - 1]);
							writtenConcepts++;
						}
					} else {
						cntrSkipped++;
					}
					cCntr++;
				}

				System.out.println(">>> # of CL concepts "
					+ writtenConcepts + " (" + cntrSkipped
					+ " skipped)");

				// if there was not enough CL concepts, fill the rest of the vector with nulls
				while (writtenConcepts < ccount) {
					tdos.writeInt(0);
					tdos.writeFloat((float) 0.0);
					writtenConcepts++;
				}
			} catch (IOException ex) {
				Logger.getLogger(ESAGoogle.class.getName()).log(Level.SEVERE, null, ex);
			}
			pr.finish();

			// print the search vector

			pr = new ProgressReporter(">>> search the esa index");
			pr.start();

			ResultSet resDocs;
			ESAIndexSearcher eis = new ESAIndexSearcher(indexDb, esaIndexPath); //, "index_all2", "index_index_all2");
			_<Integer> similarityRank = new _<Integer>(-1);
			IConceptVector res = eis.cSimpleSearch(baos.toByteArray(), 1000);
			IConceptIterator it = res.orderedIterator();
			pr.finish();

			// clean-up
			try {
				baos.close();
				tdos.close();
			} catch (IOException ex) {
				Logger.getLogger(ESAGoogle.class.getName()).log(Level.SEVERE, null, ex);
			}

			LinkSet resLinks = new LinkSet();

			while (it.next()) {
				System.out.println("["
					+ it.getId() + "] "
					+ it.getValue() + " "
					+ getTitle(it.getId()));
			}


		} catch (IOException ex) {
			Logger.getLogger(ESAGoogle.class.getName()).log(Level.SEVERE, null, ex);
		} catch (SQLException ex) {
			Logger.getLogger(ESAGoogle.class.getName()).log(Level.SEVERE, null, ex);
		}

	}

	String getTitle(Integer id) throws SQLException {
		ResultSet res = db.executeSelect("SELECT * FROM page WHERE page_id = "
			+ id);
		if (res.next()) {
			return res.getString("page_title");
		}
		return null;
	}

	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		AppConfig cfg = AppConfig.getInstance();
		cfg.setSection("ESAGoogle");

		// database of the destination wiki
		DB db = new DB("mysql://root:root@lwkm012/wikidb_zh");

		// database with the esa vector index
		DB indexDb = new DB("mysql://lz:lz@localhost:3309/esa_index");
		// path to the esa vector index
		String esaIndexPath = "/data/clld/index_zh/";

		// database with the source language esa
		DB esaDb = new DB("mysql://root:root@localhost/clw");

		// stemming class for the source language
		String stemmerClass = "org.tartarus.snowball.ext.EnglishStemmer";
		// stopwords file for the source language
		String stopWords = "/home/zilka/devel/kmi/clld/CLLD/src/res/stopwords.en.txt";
		// source language identifier
		String lang = "en";

		// run the search
		ESAGoogle eg = new ESAGoogle();
		eg.setDb(db);
		eg.setEsaDb(esaDb);
		eg.setIndexDb(indexDb);
		eg.setStemmerClass(stemmerClass);
		eg.setStopWords(stopWords);
		eg.setLang(lang);
		eg.setEsaIndexPath(esaIndexPath);
		eg.init();
		eg.search("australia");
		eg.search("Australia, officially the Commonwealth of Australia, is a country in the Southern Hemisphere comprising the mainland of the Australian continent, the island of Tasmania and numerous smaller islands in the Indian and Pacific Oceans.[N 4] Neighbouring countries include Indonesia, East Timor and Papua New Guinea to the north, the Solomon Islands, Vanuatu and New Caledonia to the northeast and New Zealand to the southeast.");

	}
}

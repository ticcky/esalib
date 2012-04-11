package clldsystem.esa;

import common.db.DB;
import common.db.DBConfig;
import common.wiki.WikipediaContentCleaner;
import common.config.AppConfig;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.LockObtainFailedException;

/**
 * Inserts ESA vectors of documents into database.
 * @author zilka
 */
public class ESAIndexer {
	// source documents db
	DB wikiDb;
	// esa db
	DB esaDb;
	// destination db
	DB destDb;

	// starting document id
	int start = 0;
	// ending document id (-1 == infinity)
	int end = -1;

	// number of dimensions of ESA vector that are kept
	int esaVectorSize = 1000;

	public void setStart(int start) {
		this.start = start;
	}

	public void setEnd(int end) {
		this.end = end;
	}

	/**
	 * Initialize the indexer.
	 * @param wikiDb source document collection (MediaWiki database)
	 * @param esaDb esa background database
	 * @param destDb destination database
	 */
	public ESAIndexer(DB wikiDb, DB esaDb, DB destDb) {
		this.wikiDb = wikiDb;
		this.esaDb = esaDb;
		this.destDb = destDb;
	}

	public void index(String lang, String stemmerClass, String stopWordsFile, String articleIter, String destTableName) {
		System.out.println("start: " + start + ", end: " + end);
		try {
			//db.executeUpdate("DROP TABLE IF EXISTS esa_vector");
			//db.executeUpdate("CREATE TABLE `esa_vector` (  `id` int(11) NOT NULL AUTO_INCREMENT,  `concept_id` int(11) DEFAULT NULL,  `paragraph_number` int(11) DEFAULT NULL,  `concept` int(11) DEFAULT NULL,  `value` float DEFAULT NULL,  `lang` varchar(45) DEFAULT NULL,  PRIMARY KEY (`id`)) ENGINE=MyISAM AUTO_INCREMENT=4901 DEFAULT CHARSET=utf8");
			//db.executeUpdate("DROP TABLE IF EXISTS esa_vector_index");
			destDb.executeUpdate("CREATE TABLE IF NOT EXISTS " + destTableName + " (  `id` int(11) NOT NULL AUTO_INCREMENT,  `doc_id` int(11) DEFAULT NULL,  `doc_section_id` int(11) DEFAULT NULL,  `vector` mediumblob,  `lang` varchar(10) DEFAULT NULL,  PRIMARY KEY (`id`), KEY ndx_doc_id (doc_id ASC)) ENGINE=MyISAM DEFAULT CHARSET=utf8");



			//PreparedStatement sel = db.getConnection().prepareStatement("SELECT * FROM article WHERE lang = ? AND id > ? LIMIT 100");
			PreparedStatement sel = wikiDb.getConnection().prepareStatement(articleIter);
			//PreparedStatement sel = db.getConnection().prepareStatement("SELECT * FROM article WHERE lang = ? AND concept_id=26903");
			PreparedStatement insVec = destDb.getConnection().prepareStatement("INSERT INTO " + destTableName + " SET doc_id = ?, vector = ?, lang = ?");
			//sel.setString(1, lang);
			sel.setInt(1, start); // 25677); don't forget to comment out the deletion above
			ResultSet res = sel.executeQuery();
			int cntr = 0;
			ESAAnalyzer searcher;
			try {
				searcher = new ESAAnalyzer(esaDb, lang, stopWordsFile, stemmerClass);
				//searcher = new CLWESASearcher(db, lang, "/home/zilka/devel/kmi/util/snowball-czech/snowball/czech_stopwords/czech.stop", "common.CzechStemmer");
			} catch (ClassNotFoundException ex) {
				Logger.getLogger(ESAIndexer.class.getName()).log(Level.SEVERE, null, ex);
				return;
			} catch (IOException ex) {
				Logger.getLogger(ESAIndexer.class.getName()).log(Level.SEVERE, null, ex);
				return;
			}
			float totTime = (float) 0.0;
			int parCntr = 0;
			while (res.next()) {
				try {
					//if ((res.getInt("id") > 25290)) {
					if (true) {
						//System.out.println("article id: " + res.getInt("id"));
						String content = DB.readBinaryStream(res.getBinaryStream("content"));
						String[] pars = new String[]{ content }; // content.split("\n\n");
						for (String p : pars) {
							long start = System.currentTimeMillis();
							try {
								if (content == null) {
									content = "";
								} else {
									content = WikipediaContentCleaner.cleanContent(content);
								}
							} catch (Exception e) {
								System.out.println("Skipping:" + res.getInt("page_id") + ": " + e);
							}
							IConceptVector cvBase = searcher.getConceptVector(content);
							IConceptVector cv = searcher.getNormalVector(cvBase, esaVectorSize);
							if (cv == null) {
								System.out.println("Skipping: " + res.getInt("page_id"));
								break;
							}
							long middle = System.currentTimeMillis();
							IConceptIterator iter = cv.iterator();

							ByteArrayOutputStream baos = new ByteArrayOutputStream(50000);
							DataOutputStream tdos = new DataOutputStream(baos);
							tdos.writeInt(cv.count());

							while (iter.next()) {
								tdos.writeInt(iter.getId());
								tdos.writeFloat((float) iter.getValue());
								/*insPar.setInt(1, res.getInt("concept_id"));
								insPar.setInt(2, parCntr);
								insPar.setInt(3, iter.getId());
								insPar.setDouble(4, iter.getValue());
								insPar.setString(5, res.getString("lang"));
								insPar.addBatch();
								cntr += 1;
								if (cntr % 1000 == 0) {
								insPar.executeBatch();
								}*/
							}
							insVec.setInt(1, res.getInt("page_id"));
							insVec.setBytes(2, baos.toByteArray());
							insVec.setString(3, lang);
							//insVec.executeUpdate();
							insVec.addBatch();

							tdos.close();
							baos.close();

							long end = System.currentTimeMillis();

							totTime += (float) (end - start) / 1000;

							parCntr += 1;
						}
						int aId = res.getInt("id");
						if (this.end > 0 && aId > this.end) {
							System.out.println("  -- " + res.getInt("id") + " avg doc " + (totTime / parCntr));
							System.out.println("  -- finish -- ");
							insVec.executeBatch();
							break;
						}
					}
				} catch (IOException ex) {
					Logger.getLogger(ESAIndexer.class.getName()).log(Level.SEVERE, null, ex);
				}

				if (res.isLast()) {
					System.out.println("  -- " + res.getInt("id") + " avg doc " + (totTime / parCntr));
					long start = System.currentTimeMillis();
					insVec.executeBatch();
					long end = System.currentTimeMillis();
					System.out.println("  -- inserting the batch " + ((float) (end - start)) / 1000);

					int aId = res.getInt("id");
					res.close();
					sel.setInt(1, aId);
					res = sel.executeQuery();
				}

			}
			insVec.executeBatch();
		} catch (SQLException ex) {
			Logger.getLogger(ESAIndexer.class.getName()).log(Level.SEVERE, null, ex);
		}
	}

	public void index2(String lang) throws SQLException, CorruptIndexException, LockObtainFailedException, IOException, ClassNotFoundException {
		int docCnt = 0;
		// index the input articles
		ArticleIndexer ai = new ArticleIndexer("/tmp/testndx", "/home/zilka/devel/kmi/util/wikiprep-esa/esa-lucene/src/config/stopwords.txt", "org.tartarus.snowball.ext.EnglishStemmer", null);
		PreparedStatement sel = wikiDb.getConnection().prepareStatement("SELECT * FROM article WHERE lang = ?");
		sel.setString(1, lang);
		ResultSet res = sel.executeQuery();
		while (res.next()) {
			try {
				System.out.println("Indexing article..");
				String content = new String(res.getBytes("content"), "UTF-8");
				String[] pars = content.split("\n\n");
				for (String p : pars) {
					ai.addArticle(new Integer(docCnt).toString(), res.getString("title"), content);
					docCnt++;
				}
			} catch (UnsupportedEncodingException ex) {
				Logger.getLogger(ESAIndexer.class.getName()).log(Level.SEVERE, null, ex);
			}

		}
		ai.finish();

		// create the inverted index
		PreparedStatement insPar = wikiDb.getConnection().prepareStatement("INSERT INTO esa_vector SET concept_id = ?, paragraph_number = ?, concept = ?, value = ?, lang = ?");
		ESAIndexBuilder.modify(wikiDb, "test", "/tmp/testndx");

		// create ESA vectors for the documents
		HashMap<String, byte[]> myNdx = new HashMap<String, byte[]>();
		HashMap<String, byte[]> esaNdx = new HashMap<String, byte[]>();
		res = wikiDb.executeSelect("SELECT * FROM test_ndx");
		while (res.next()) {
			myNdx.put(new String(res.getBytes("term"), "UTF-8"), res.getBytes("vector"));
		}
		res = wikiDb.executeSelect("SELECT * FROM en_ndx");
		while (res.next()) {
			esaNdx.put(new String(res.getBytes("term"), "UTF-8"), res.getBytes("vector"));
		}

		ByteArrayInputStream mbais;
		DataInputStream mdis;
		int mplen;
		int mdoc;
		ByteArrayInputStream bais;
		DataInputStream dis;
		int plen;
		int concept;
		float score;
		float xscore;
		ESAVector esaVector = new ESAVector(docCnt);

		/*for (String esaTerm : esaNdx.keySet()) {
		byte[] myDocs = myNdx.get(esaTerm);
		if(myDocs == null)
		continue;

		mbais = new ByteArrayInputStream(myDocs);
		mdis = new DataInputStream(mbais);
		mplen = mdis.readInt();
		for (int k = 0; k < mplen; k++) {
		mdoc = mdis.readInt();
		score = mdis.readFloat();

		bais = new ByteArrayInputStream(esaNdx.get(esaTerm));
		dis = new DataInputStream(bais);
		plen = dis.readInt();
		for (int kk = 0; kk < plen; kk++) {
		concept = dis.readInt();
		score = dis.readFloat();

		esaVector.add(mdoc, concept, score);
		}
		bais.close();
		dis.close();
		}

		mbais.close();
		mdis.close();
		}*/
		for (String myTerm : myNdx.keySet()) {
			byte[] esaDocs = esaNdx.get(myTerm);
			if (esaDocs == null) {
				continue;
			}

			mbais = new ByteArrayInputStream(esaDocs);
			mdis = new DataInputStream(mbais);
			mplen = mdis.readInt();
			for (int k = 0; k < mplen; k++) {
				mdoc = mdis.readInt();
				score = mdis.readFloat();

				bais = new ByteArrayInputStream(myNdx.get(myTerm));
				dis = new DataInputStream(bais);
				plen = dis.readInt();
				for (int kk = 0; kk < plen; kk++) {
					concept = dis.readInt();
					xscore = dis.readFloat();

					esaVector.add2(concept, mdoc, score);
				}
				bais.close();
				dis.close();
			}

			mbais.close();
			mdis.close();
		}

		System.out.println(esaVector.vectors2.size());
		for (Integer i : esaVector.vectors.keySet()) {
			HashMap<Integer, Float> v = esaVector.vectors.get(i);
			for (Integer c : v.keySet()) {
				//System.out.print(v.get(c) + ", ");
				System.out.print(c + ", ");
			}
			System.out.println();
		}


	}

	private void setEsaVectorSize(int esaVectorSize) {
		this.esaVectorSize = esaVectorSize;
	}

	class ESAVector {

		HashMap<Integer, float[]> vectors2 = new HashMap<Integer, float[]>();
		HashMap<Integer, HashMap<Integer, Float>> vectors = new HashMap<Integer, HashMap<Integer, Float>>();
		int maxDocs;

		public ESAVector(int maxDocs) {
			this.maxDocs = maxDocs;
		}

		public void add(int doc, int concept, float score) {
			HashMap<Integer, Float> vector = vectors.get(doc);
			if (vector == null) {
				vector = new HashMap<Integer, Float>();
				vectors.put(doc, vector);
			}
			Float val = vector.get(concept);
			if (val == null) {
				val = new Float(0.0);
			}
			val += score;
			vector.put(concept, val);
		}

		public void add2(int doc, int concept, float score) {
			float[] vector = vectors2.get(concept);
			if (vector == null) {
				vector = new float[maxDocs];
				vectors2.put(doc, vector);
			}
			vector[doc] += score;
		}
	}

	public static void main(String[] args) throws Exception {
		String articleIter = AppConfig.getInstance().getString("ESAIndexer.query.articleIter");

		DBConfig dbc = new DBConfig();
		dbc.setConnectionFromDrupalUrl(AppConfig.getInstance().getString("ESAIndexer.articleDb"));

		DBConfig dbcEsa = new DBConfig();
		dbcEsa.setConnectionFromDrupalUrl(AppConfig.getInstance().getString("ESAIndexer.esaDb"));

		DBConfig dbcDest = new DBConfig();
		dbcDest.setConnectionFromDrupalUrl(AppConfig.getInstance().getString("ESAIndexer.destDb"));
		String destTableName = AppConfig.getInstance().getString("ESAIndexer.destTableName");

		String lang = AppConfig.getInstance().getString("ESAIndexer.lang");
		int esaVectorSize = AppConfig.getInstance().getInt("ESAIndexer.esaVectorSize");

		DB db = new DB(dbc);
		DB dbEsa = new DB(dbcEsa);
		DB dbDest = new DB(dbcDest);

		ESAIndexer ei = new ESAIndexer(db, dbEsa, dbDest);
		ei.setEsaVectorSize(esaVectorSize);
		String stopWords = AppConfig.getInstance().getString("ESAIndexer.stopWordsFile");
		String stemmerClass = AppConfig.getInstance().getString("ESAIndexer.stemmerClass");
		if (args.length > 0) {
			ei.setStart(Integer.parseInt(args[0]));
		}
		if (args.length > 1) {
			ei.setEnd(Integer.parseInt(args[1]));
		}
		ei.index(lang, stemmerClass, stopWords, articleIter, destTableName);

	}
}

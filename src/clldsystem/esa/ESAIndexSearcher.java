package clldsystem.esa;

import clldsystem.esa.ESAAnalyzer;
import clldsystem.kmi.linking.ConceptMapper;
import common.db.DB;
import common.Utils;
import common._;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Performs search in ESA Vector Index for documents having vectors similar to the
 * vector given as an input. There are several implementation, the best one is 
 * searching via the cSimpleSearch method.
 * @author zilka
 */
public class ESAIndexSearcher {

	DB indexDb;
	DB cacheDb;
	String indexTable;
	String indexIndexTable;
	String indexPath;

	public void setCacheDb(DB cacheDb) {
		this.cacheDb = cacheDb;
	}
	
	/**
	 * Initializes the searcher in case the cSimpleSearch method will be used.
	 * @param indexDb database with the vector index
	 * @param esaIndexPath path to the vector index on the database machine
	 */
	public ESAIndexSearcher(DB indexDb, String esaIndexPath) {
		this.indexDb = indexDb;
		this.indexPath = esaIndexPath;
	}


	/**
	 * Initializes the searcher for other searching methods.
	 * @param indexDb database with the vector index
	 * @param esaIndexPath path to the vector index on the database machine
	 * @param table table with the vector index
	 * @param tableIndex name of the table with the index of the table with the vectors
	 */
	public ESAIndexSearcher(DB indexDb, String esaIndexPath, String table, String tableIndex) {
		this.indexDb = indexDb;
		this.indexTable = table;
		this.indexIndexTable = tableIndex;
		this.indexPath = esaIndexPath;
	}

	/**
	 * Example invocation.
	 * @param args
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 * @throws IOException 
	 */
	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
		String stopWords = "/home/zilka/devel/kmi/clld/CLLD/src/res/stopwords.en.txt";
		String stemmerClass = "org.tartarus.snowball.ext.EnglishStemmer";
		String lang = "en";



		DB db = new DB("mysql://root:quaeCi9f@localhost:3308/esa_index");
		DB esaDb = new DB("mysql://root:root@localhost/clw");

		ESAAnalyzer searcher = new ESAAnalyzer(esaDb, lang, stopWords, stemmerClass);
		IConceptVector cv = searcher.getConceptVector(Utils.readBinaryStream(new FileInputStream("/home/zilka/devel/kmi/clld/linux.txt")));
		cv = ESAAnalyzer.trim(cv, 100);


		ESAIndexSearcher eis = new ESAIndexSearcher(db, "/data/index_all2");
		IConceptVector results = eis.cSimpleSearch(ESAAnalyzer.buildVector(cv), 1000);
		// NOTE: for other methods, the ESAIndexSearcher must be initilaized with the 4 parameter constructor
		//eis.dumbSearch(cv, "olympic_games");
	}

	/**
	 * Proxy call for the dumbSearch method.
	 * @param cv
	 * @param searchId
	 * @param limit
	 * @return 
	 */
	public ResultSet dumbSearch(IConceptVector cv, String searchId, int limit) {
		byte[] v = ESAAnalyzer.buildVector(cv, 100);
		return dumbSearch(v, searchId, limit);
	}

	/**
	 * Looks up a cache table name in the cache for the given pair (vector, search id)
	 * @param v
	 * @param searchId
	 * @return 
	 */
	private String lookupCacheTableName(byte[] v, String searchId) {
		try {
			cacheDb.executeUpdate("CREATE TABLE IF NOT EXISTS esa_search_cache (search_vector VARBINARY(3000), table_id VARCHAR(100))");

			PreparedStatement cachePs = cacheDb.getConnection().prepareStatement("SELECT * FROM esa_search_cache WHERE search_vector = ?");
			cachePs.setBytes(1, v);

			String dbName = cacheDb.getConnection().getCatalog();

			// if one already exists, return it
			ResultSet res = cachePs.executeQuery();
			if (res.next()) {
				String tableName = res.getString("table_id");
				res.close();
				return tableName;
				// else generate a name
			} else {
				int cntr = 0;
				while (true) {
					cntr++;
					try {
						cacheDb.getConnection().prepareStatement("SELECT count(*) FROM "
							+ searchId).execute();
					} catch (SQLException e) {
						break;
					}
				}
				if (cntr == 0) {
					return dbName + "." + searchId;
				} else {
					return dbName + "." + searchId + "_"
						+ cntr;
				}
			}
		} catch (SQLException ex) {
			Logger.getLogger(ESAIndexSearcher.class.getName()).log(Level.SEVERE, null, ex);
			return "";
		}

	}

	/**
	 * Associates the given vector with a particular table name in the cache.
	 * @param v
	 * @param tableName
	 * @throws SQLException 
	 */
	private void insertCacheTableName(byte[] v, String tableName) throws SQLException {
		PreparedStatement psInsert = cacheDb.getConnection().prepareStatement("INSERT INTO esa_search_cache SET search_vector = ?, table_id = ?");
		psInsert.setBytes(1, v);
		psInsert.setString(2, tableName);
		psInsert.execute();
	}

	/**
	 * Performs search for similar documents to a document represented by the
	 * vector v. The search is performed by comparing the vector v with each vector
	 * in the database. Consequently, this is slow. For all 3.5M articles in 
	 * English Wikipedia, and an input vector of 100 dimensions, this might take up to 8 minutes.
	 * @param v
	 * @param searchId
	 * @param limit
	 * @return 
	 */
	public ResultSet dumbSearch(byte[] v, String searchId, int limit) {
		try {
			String cacheTableName = lookupCacheTableName(v, searchId);
			PreparedStatement psResults = indexDb.getConnection().prepareStatement("SELECT r.doc_id, r.simil, i.vector FROM "
				+ cacheTableName + " r LEFT JOIN " + indexTable
				+ " i ON i.doc_id = r.doc_id");
			ResultSet res = null;
			try {
				res = psResults.executeQuery();
			} catch (SQLException e) {
			}

			if (res == null) {
				//indexDb.executeUpdate("DROP TABLE IF EXISTS _res_" + searchId);
				PreparedStatement psClEsa = indexDb.getConnection().prepareStatement("CREATE TABLE "
					+ cacheTableName
					+ " AS SELECT i.doc_id, esa_simil(i.vector, ?) AS simil FROM "
					+ indexTable
					+ " i ORDER BY simil DESC LIMIT ?");
				psClEsa.setBytes(1, v);
				psClEsa.setInt(2, limit);
				psClEsa.executeUpdate();

				res = psResults.executeQuery();

				insertCacheTableName(v, cacheTableName);
			}
			return res;
		} catch (SQLException ex) {
			Logger.getLogger(ESAIndexSearcher.class.getName()).log(Level.SEVERE, null, ex);
			return null;
		}

	}

	/**
	 * Searches the ESA vector index using the MySQL interface of our Vector Index.
	 * Therefore this is pretty quick - about 30 seconds for a query to index of
	 * English Wikipedia (3.5M articles) and a query vector of 100 dimensions
	 * @param v
	 * @param limit number of results that we are interested in
	 * @return
	 * @throws IOException
	 * @throws SQLException 
	 */
	public IConceptVector cSimpleSearch(byte[] v, int limit) throws IOException, SQLException {
		// debugging
		System.out.println("Searching for:");
		for (Byte i : v) {
			System.out.print(String.format("%02x", i));
		}
		System.out.println();

		// do the similarity search
		PreparedStatement psClEsa = indexDb.getConnection().prepareStatement("SELECT esa_search(?, '"
			+ indexPath + "', " + limit + ") AS res_vector");
		psClEsa.setBytes(1, v);
		ResultSet clres;
		try {
			clres = psClEsa.executeQuery();
		} catch (SQLException ex) {
			System.err.println(ex);
			return null;
		}

		byte[] resVector;
		IConceptVector cv = null;
		if (clres.next()) {
			// read the result
			resVector = clres.getBytes("res_vector");
			cv = ESAAnalyzer.getVector(new ByteArrayInputStream(resVector));
			return cv;
		}
		return null;

	}

	public static IConceptVector mapVector(IConceptVector src, ConceptMapper cm, int cnt) {
		IConceptVector res = new TroveConceptVector(cnt);
		IConceptIterator it = src.orderedIterator();
		while (it.next() && cnt > 0) {
			Long mappedId = null;
			try {
				if ((mappedId = cm.getPageId(it.getId()))
					!= null) {
					res.add(mappedId.intValue(), it.getValue());
					cnt--;
				}
			} catch (NullPointerException ex) {
				//System.out.println("err, mapping:" + it.getId());
			}
		}

		return res;
	}
	
	/**
	 * Uses the same MySQL interface as cSimpleSearch, but keeps in a database
	 * table information about the rank of retrieved document. This does not 
	 * belong here, but it was experimentally implemented here. Now, it is useless.
	 * @param pageId
	 * @param mappedPageId
	 * @param pageRank
	 * @param v
	 * @param searchId
	 * @param limit
	 * @return 
	 */
	public ResultSet cSearch(int pageId, List<Integer> mappedPageId, _<Integer> pageRank, byte[] v, String searchId, int limit) {
		System.out.println("Searching for:");
		for (Byte i : v) {
			System.out.print(String.format("%02x", i));
			//+ "//:Integer.toHexString(common.Utils.unsignedToBytes(i)) + ", ");
		}
		System.out.println();
		try {
			String cacheTableName = lookupCacheTableName(v, searchId);
			indexDb.executeUpdate("DROP TABLE IF EXISTS "
				+ cacheTableName);
			PreparedStatement psResults = indexDb.getConnection().prepareStatement("SELECT r.*, i.vector FROM "
				+ cacheTableName + " r "
				+ "LEFT JOIN " + indexTable
				+ " i ON r.doc_id = i.doc_id ORDER BY simil DESC");


			ResultSet res = null;
			try {
				res = psResults.executeQuery();
			} catch (SQLException e) {
			}

			if (res == null) {
				PreparedStatement psClEsa = indexDb.getConnection().prepareStatement("SELECT esa_search(?, '"
					+ indexPath + "', " + limit
					+ ") AS res_vector");
				psClEsa.setBytes(1, v);
				ResultSet clres = psClEsa.executeQuery();

				byte[] resVector;
				int similRank = 0;
				IConceptVector cv = null;
				if (clres.next()) {
					resVector = clres.getBytes("res_vector");
					String insStr = "INSERT INTO "
						+ cacheTableName
						+ " VALUES ";
					try {
						cv = ESAAnalyzer.getVector(new ByteArrayInputStream(resVector));
						indexDb.executeUpdate("DROP TABLE IF EXISTS "
							+ cacheTableName);
						indexDb.executeUpdate("CREATE TABLE "
							+ cacheTableName
							+ " (doc_id INTEGER, simil FLOAT, key ndx_doc_id (doc_id ASC), key ndx_simil (simil DESC))");

						//PreparedStatement insRes = indexDb.getConnection().prepareStatement("INSERT INTO _esasearch_tmp VALUES doc_id = ?, simil = ?");

						IConceptIterator i = cv.orderedIterator();
						pageRank.s(cv.size());
						while (i.next()) {
							if (mappedPageId.contains(i.getId())) {
								pageRank.s(similRank);
							}
							if (Double.isNaN(i.getValue())) {
								continue;
							}
							similRank++;
							insStr += "("
								+ i.getId()
								+ ", "
								+ i.getValue()
								+ ") ,";
							//System.out.println(i.getId() + " " + i.getValue());
							//insRes.setInt(1, i.getId());
							//insRes.setDouble(2, i.getValue());
							//insRes.addBatch();
						}
						//insRes.executeBatch();
						insStr = insStr.substring(0, insStr.length()
							- 2);
						if (cv.size() > 0) {
							indexDb.executeUpdate(insStr);
						}
					} catch (IOException ex) {
						Logger.getLogger(ESAIndexSearcher.class.getName()).log(Level.SEVERE, null, ex);
					} catch (SQLException ex) {
						System.out.println(insStr);
						Logger.getLogger(ESAIndexSearcher.class.getName()).log(Level.SEVERE, null, ex);
					}
				}
				insertCacheTableName(v, cacheTableName);

			}

			res = psResults.executeQuery();

			return res;
		} catch (SQLException ex) {
			Logger.getLogger(ESAIndexSearcher.class.getName()).log(Level.SEVERE, null, ex);
			return null;
		}

	}
}

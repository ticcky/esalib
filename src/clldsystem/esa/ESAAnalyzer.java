package clldsystem.esa;

import clldsystem.data.LUCENEWikipediaAnalyzer;
import common.HeapSort;
import common.Utils;
import common.config.AppConfig;
import common.db.DB;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;

import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import gnu.trove.TIntFloatHashMap;
import gnu.trove.TIntIntHashMap;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.Version;

/**
 * Performs search on the index located in database.
 * 
 * @author zilka, Cagatay Calli <ccalli@gmail.com>
 */
public class ESAAnalyzer {

	String lang;
	DB db;
	boolean caching = false; // cache the results?
	boolean loadToMemory = true; // theoretically loading the whole esa index first into the memory might help the overall speed a bit
	boolean debug = false;
	// database connection
	Connection connection;
	PreparedStatement pstmtQuery;
	PreparedStatement pstmtIdfQuery;
	// database queries
	String strMaxConcept = "SELECT COUNT(concept_id) FROM article GROUP BY lang";
	String strInlinks = "SELECT i.target_id, i.inlink FROM inlinks i WHERE i.target_id IN ";
	String strLinks = "SELECT target_id FROM pagelinks WHERE source_id = ?";
	String strTermQuery;
	String strIdfQuery;
	// lucene analyzer for the input query
	Analyzer analyzer;
	// used for the esa projection
	int maxConceptId;
	Integer currMaxConceptNdx = 0;
	int[] ids;
	TIntFloatHashMap values;
	HashMap<String, Integer> freqMap = new HashMap<String, Integer>(30); // word's freq
	HashMap<String, Double> tfidfMap = new HashMap<String, Double>(30); // word's tfidf
	HashMap<String, Float> idfMap = new HashMap<String, Float>(30); // word's idf
	HashMap<String, byte[]> esaNdx = new HashMap<String, byte[]>();
	HashMap<String, Float> esaIdf = new HashMap<String, Float>();
	TIntIntHashMap conceptMap = new TIntIntHashMap(); // mapping of temporary id's to concept id's of wikipedia
	ArrayList<String> termList = new ArrayList<String>(30);
	// esa vector similarity measure
	ConceptVectorSimilarity sim = new ConceptVectorSimilarity(new CosineScorer());
	private Map<String, IConceptVector> esaCache;

	public void setDebug(boolean debug) {
		this.debug = debug;
	}

	public void setAnalyzer(Analyzer analyzer) {
		this.analyzer = analyzer;
	}

	/**
	 * Trim the concept vector, so that it only contains the best n dimensions.
	 * @param v
	 * @param n
	 * @return
	 */
	public static IConceptVector trim(IConceptVector v, int n) {
		IConceptVector res = new TroveConceptVector(n);
		IConceptIterator it = v.orderedIterator();
		while (it.next() && res.count() < n) {
			res.add(it.getId(), it.getValue());
		}
		return res;
	}

	void initDB() throws ClassNotFoundException, SQLException, IOException {
		connection = db.getConnection();

		// load everything to memory
		ResultSet res;
		if (loadToMemory) {
			res = db.executeSelect("SELECT * FROM " + lang + "_ndx");
			while (res.next()) {
				esaNdx.put(new String(res.getBytes("term"), "UTF-8"), res.getBytes("vector"));
			}

			res = db.executeSelect("SELECT * FROM " + lang
				+ "_terms");
			while (res.next()) {
				esaIdf.put(new String(res.getBytes("term"), "UTF-8"), res.getFloat("idf"));
			}
			System.err.println("index loaded to memory");
		}

		strTermQuery = "SELECT t.vector FROM " + lang
			+ "_ndx t WHERE t.term = ?";
		strIdfQuery = "SELECT t.idf FROM " + lang
			+ "_terms t WHERE t.term = ?";

		pstmtQuery = connection.prepareStatement(strTermQuery);
		pstmtQuery.setFetchSize(1);

		pstmtIdfQuery = connection.prepareStatement(strIdfQuery);
		pstmtIdfQuery.setFetchSize(1);


		//ResultSet res = connection.createStatement().executeQuery(strMaxConcept);
		//res.next();
		maxConceptId = 8000000; //res.getInt(1) + 1;
	}

	public void clean() {
		freqMap.clear();
		tfidfMap.clear();
		idfMap.clear();
		termList.clear();
		Arrays.fill(ids, 0);
	}

	public ESAAnalyzer(DB db, String lang, String stopWords, String stemmerClass) throws ClassNotFoundException, SQLException, IOException {
		this(db, lang);
		this.setAnalyzer(new LUCENEWikipediaAnalyzer(stopWords, stemmerClass));

	}

	public ESAAnalyzer(DB db, String lang) throws ClassNotFoundException, SQLException, IOException {
		this.lang = lang;
		this.db = db;
		initDB();

		analyzer = new StandardAnalyzer(Version.LUCENE_30);

		ids = new int[maxConceptId];
		values = new TIntFloatHashMap(100000);

		esaCache = new HashMap<String, IConceptVector>();
	}

	/**
	 * Projects the query into the concept space.
	 * @param query
	 * @return Returns concept vector results exist, otherwise null 
	 * @throws IOException
	 * @throws SQLException
	 */
	public IConceptVector getConceptVector(String query) throws IOException, SQLException {
		if (caching && esaCache.containsKey(query)) {
			return esaCache.get(query);
		}

		String strTerm;
		int numTerms = 0;
		ResultSet rs;
		int doc;
		double score;
		int vint;
		double vdouble;
		double tf;
		double vsum;
		int plen;

		this.clean();
		values.clear();

		// prepare the input query and tokenize it
		query = Utils.removeDiacriticalMarks(query);
		TokenStream ts = analyzer.tokenStream("contents", new StringReader(query));
		ts.reset();
		while (ts.incrementToken()) {
			TermAttribute t = ts.getAttribute(TermAttribute.class);
			strTerm = t.term();
			System.out.println(strTerm.toString());
			if (strTerm.equals(",") || strTerm.equals(".")) {
				continue;
			}
			termList.add(strTerm);
			numTerms++;
		}
		ts.end();
		ts.close();		

		// start the output
		ByteArrayInputStream bais;
		DataInputStream dis;

		// tokenize the query

		// if the index is not in the memory, read from the database the required information
		if (!loadToMemory) {
			// prepare temporary table with all the terms from our document
			db.executeUpdate("DROP TABLE IF EXISTS _esaterms");
			db.executeUpdate("CREATE TABLE _esaterms (term VARCHAR(255))");
			PreparedStatement psTTerms = db.getConnection().prepareStatement("INSERT INTO _esaterms (term) VALUES (?)");
			for (String t : termList) {		
				//psTTerms.setString(1, t);
				psTTerms.setBytes(1, t.getBytes("UTF-8"));
				psTTerms.addBatch();
			}
			psTTerms.executeBatch();
			psTTerms.close();
			
			

			// nov retreive the wiki vectors and idf's of the terms from the database
			ResultSet res;
			esaNdx.clear();
			esaIdf.clear();
			res = db.executeSelect("SELECT ndx.* FROM _esaterms INNER JOIN "
				+ lang + "_ndx ndx ON _esaterms.term = ndx.term");
			while (res.next()) {
				esaNdx.put(new String(res.getBytes("term"), "UTF-8"), res.getBytes("vector"));
			}
			res.close();

			res = db.executeSelect("SELECT terms.* FROM _esaterms INNER JOIN "
				+ lang
				+ "_terms terms ON _esaterms.term = terms.term");
			while (res.next()) {
				esaIdf.put(new String(res.getBytes("term"), "UTF-8"), res.getFloat("idf"));
			}
			res.close();
		}

		// for each term of the query, get it's idf and tf
		for (String t : termList) {
			// record term IDF
			if (!idfMap.containsKey(t)) {
				Float idf = esaIdf.get(t);
				if (idf != null) {
					idfMap.put(t, idf);
				}
			}

			// records term counts for TF
			if (freqMap.containsKey(t)) {
				vint = freqMap.get(t);
				freqMap.put(t, vint + 1);
			} else {
				freqMap.put(t, 1);
			}
		}


		// if we compute the vector of an empty string, the universe implodes.. 
		if (numTerms == 0) {
			return null;
		}

		// calculate TF-IDF vector (normalized)
		vsum = 0;
		for (String tk : idfMap.keySet()) {
			tf = 1.0 + Math.log(freqMap.get(tk));
			vdouble = (idfMap.get(tk) * tf);
			tfidfMap.put(tk, vdouble);
			vsum += vdouble * vdouble;
		}
		vsum = Math.sqrt(vsum);

		// comment this out for canceling query normalization
		for (String tk : idfMap.keySet()) {
			vdouble = tfidfMap.get(tk);
			tfidfMap.put(tk, vdouble / vsum);
		}


		// start the esa scoring
		score = 0;
		for (String tk : termList) {
			// get the wiki vector of the current term
			byte[] rss = esaNdx.get(tk);
			score = 0;

			// if there is no vector for the current term, skip
			if (rss != null) {
				// decode the vector
				bais = new ByteArrayInputStream(rss);
				dis = new DataInputStream(bais);

				/**
				 * 4 bytes: int - length of array
				 * [ 4 byte (doc) - 8 byte (tfidf) ]*
				 */
				plen = dis.readInt();
				for (int k = 0; k < plen; k++) {
					doc = dis.readInt();
					score = dis.readFloat();
					float scoreF = (float) (score
						* tfidfMap.get(tk));
					values.adjustOrPutValue(doc, scoreF, scoreF);
				}

				dis.close();
				bais.close();
			}

		}

		// sort the values
		float[] valuesF = values.getValues();
		ids = values.keys();
		HeapSort.heapSort(valuesF, ids);

		//put them into the vector from the best dimension to the worst one
		IConceptVector newCv = new TroveConceptVector(ids.length);
		for (int i = ids.length - 1; i >= 0 && valuesF[i] > 0; i--) {
			newCv.set(ids[i], valuesF[i] / numTerms);
		}

		// eventually, cache the result
		if (caching) {
			esaCache.put(query, newCv);
		}

		if(newCv.count() > 0)
			return newCv;
		else
			return null;
	}

	/**
	 * Calculate semantic relatedness between two concept vectors
	 * @param doc1
	 * @param doc2
	 * @return returns relatedness if successful, -1 otherwise
	 */
	public double getRelatedness(IConceptVector c1, IConceptVector c2) {
		try {

			if (c1 == null || c2 == null) {
				return -1;	// undefined
			}

			final double rel = sim.calcSimilarity(c1, c2);

			return rel;

		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}

	}

	/**
	 * Calculate semantic relatedness between documents
	 * @param doc1
	 * @param doc2
	 * @return returns relatedness if successful, -1 otherwise
	 */
	public double getRelatedness(String doc1, String doc2) {
		try {
			IConceptVector c1;
			if(!caching)
				esaCache.clear();
			if ((c1 = esaCache.get(doc1)) == null) {
				c1 = getConceptVector(doc1);
				esaCache.put(doc1, c1);
			}
			IConceptVector c2;
			if ((c2 = esaCache.get(doc2)) == null) {
				c2 = getConceptVector(doc2);
				esaCache.put(doc2, c2);
			}
			if(c1 == null || c2 == null)
				return Double.NaN;
			System.err.println("vector 1 dimensions: " + c1.count());
			System.err.println("vector 2 dimensions: " + c2.count());

			return getRelatedness(c1, c2);
		} catch (IOException ex) {
			Logger.getLogger(ESAAnalyzer.class.getName()).log(Level.SEVERE, null, ex);
			return -1;
		} catch (SQLException ex) {
			Logger.getLogger(ESAAnalyzer.class.getName()).log(Level.SEVERE, null, ex);
			return -1;
		}
	}

	private int getConceptIndex(int doc) {
		if (conceptMap.containsKey(doc)) {
			return conceptMap.get(doc);
		} else {
			conceptMap.put(doc, currMaxConceptNdx++);
			return currMaxConceptNdx - 1;
		}
	}

	public static IConceptVector getVector(byte[] input) throws IOException {
		return getVector(new ByteArrayInputStream(input));
	}

	/**
	 * Create the concept vector from an input stream.
	 * @param bs
	 * @return
	 * @throws IOException
	 */
	public static IConceptVector getVector(InputStream bs) throws IOException {
		DataInputStream dis = new DataInputStream(bs);

		int plen = dis.readInt();
		IConceptVector cv = new TroveConceptVector(plen);
		for (int k = 0; k < plen; k++) {
			int doc = dis.readInt();
			float score = dis.readFloat();
			cv.set(doc, score);
		}

		dis.close();

		return cv;
	}

	/** 
	 * Returns trimmed form of concept vector
	 * @param cv
	 * @return
	 */
	public IConceptVector getNormalVector(IConceptVector cv, int limit) {
		IConceptVector cv_normal = new TroveConceptVector(limit);
		IConceptIterator it;

		if (cv == null) {
			return null;
		}

		it = cv.orderedIterator();

		int count = 0;
		while (it.next()) {
			if (count >= limit) {
				break;
			}
			cv_normal.set(it.getId(), it.getValue());
			count++;
		}

		return cv_normal;
	}

	public static byte[] buildVector(IConceptVector cv) {
		return buildVector(cv, -1);
	}

	/**
	 * Build binary vector of the given concept vector. Preserve only cv number of dimensions.
	 * @param cv
	 * @param cnt
	 * @return
	 */
	public static byte[] buildVector(IConceptVector cv, int cnt) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream(50000);
		DataOutputStream tdos = new DataOutputStream(baos);
		try {
			int cCnt = cnt;
			// write the vector length
			if (cnt == -1) {
				cCnt = cv.count();
			}
			tdos.writeInt(cCnt);

			// build the vector, and skip over the concepts
			// that cannot be CL mapped ( = concepts that don't
			// have equivalent page in the other language's wiki
			int mappedConcept;
			int cCntr = 0;
			//List<Integer> conceptsMapped = mapConcepts(conceptList);
			IConceptIterator it = cv.orderedIterator();
			while (it.next() && cCntr < cCnt) {
				tdos.writeInt(it.getId());
				tdos.writeFloat((float) it.getValue());
				cCntr++;
			}

			// fill the rest
			while (cCntr < cCnt) {
				tdos.writeInt(0);
				tdos.writeFloat((float) 0.0);
				cCntr++;
			}

		} catch (IOException ex) {
			Logger.getLogger(ESAAnalyzer.class.getName()).log(Level.SEVERE, null, ex);
		}

		return baos.toByteArray();

	}

	public static void main(String[] args) throws SQLException, ClassNotFoundException, IOException {
		// load config
		AppConfig cfg = AppConfig.getInstance();
		cfg.setSection("ESAAnalyzer");
		
		// create analyzer
		ESAAnalyzer esa = new ESAAnalyzer(new DB(cfg.getSString("db")), cfg.getSString("lang"));
		LUCENEWikipediaAnalyzer wikiAnalyzer = new LUCENEWikipediaAnalyzer(cfg.getSString("stopWordsFile"), cfg.getSString("stemmerClass"));
		wikiAnalyzer.setLang(cfg.getSString("lang"));
		esa.setAnalyzer(wikiAnalyzer);
		

		if(args.length == 0) {	// for interactive mode			
			InputStreamReader converter = new InputStreamReader(System.in);
			BufferedReader in = new BufferedReader(converter);
			String curLine = null;
			String curLine2 = "";
			while(curLine != "") {
				curLine = in.readLine();
				curLine2 = in.readLine();
				long startTime = System.nanoTime();
				System.out.println(esa.getRelatedness(curLine, curLine2));
				System.err.println("time: " + (System.nanoTime() - startTime) / 1000000000.0);
			}
		} else if(args.length == 2) {  // for non-interactive mode
			System.out.println(esa.getRelatedness(args[0], args[1]));
		} else {
			System.out.println("Please specify 0 arguments for interactive use, or exactly 2 strings as arguments for non-interactive use.");
		}
			
		
		// System.out.println(esa.getRelatedness("journei", "cow"));
	}
}
// clean-up index
// DELETE FROM `clw`.`cs_terms` where not(term REGEXP '^[A-Za-z0-9\-]+$');
// DELETE FROM `clw`.`cs_ndx` where not(term REGEXP '^[A-Za-z0-9\-]+$');


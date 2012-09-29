package clldsystem.esa;

import common.HeapSort;
import common.db.DB;
import common.db.DBConfig;
import common.config.AppConfig;
import gnu.trove.TIntFloatHashMap;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.HashMap;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.TermFreqVector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

/**
 * Reads TF and IDF from the index and 
 * writes cosine-normalized TF.IDF values to database.
 * 
 * Normalization is performed as in Gabrilovich et al. (2009)
 * 
 * Usage: IndexModifier <Lucene index location>
 * 
 * @author Cagatay Calli <ccalli@gmail.com>
 *
 */
public class ESAIndexBuilder {
	static Statement stmtLink;
	static PreparedStatement pstmtVector;
	// static String strLoadData = "LOAD DATA LOCAL INFILE 'vector.txt' INTO TABLE idx FIELDS ENCLOSED BY \"'\"";
	static String strVectorQuery;
	static String strTermLoadData;
	private static IndexReader reader = null;
	static int WINDOW_SIZE = 100;
	static float WINDOW_THRES = 0.005f;
	static DecimalFormat df = new DecimalFormat("#.########");
	static String ndxTable;
	static String termsTable;
	static int minTermFreq = 3;

	public static void initDB(DB db, String lang) throws ClassNotFoundException, SQLException, IOException {
		ndxTable = lang + "_ndx";
		termsTable = lang + "_terms";

		strVectorQuery = "INSERT INTO " + ndxTable + " (term,vector) VALUES (?,?)";
		strTermLoadData = "LOAD DATA LOCAL INFILE 'term.txt' INTO TABLE " + termsTable + " FIELDS ENCLOSED BY \"'\"";

		stmtLink = db.getConnection().createStatement();
		stmtLink.setFetchSize(200);

		stmtLink.execute("DROP TABLE IF EXISTS " + ndxTable);
		stmtLink.execute("CREATE TABLE " + ndxTable + " ("
			+ "term VARBINARY(255),"
			+ "vector MEDIUMBLOB "
			+ ")");

		stmtLink.execute("DROP TABLE IF EXISTS " + termsTable);
		stmtLink.execute("CREATE TABLE " + termsTable + " ("
			+ "term VARBINARY(255),"
			+ "idf FLOAT "
			+ ")");


		pstmtVector = db.getConnection().prepareStatement(strVectorQuery);

	}

	public static void setMinTermFreq(int minTermFreq) {
		ESAIndexBuilder.minTermFreq = minTermFreq;
	}

	/**
	 * @param args
	 * @throws IOException 
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws NoSuchAlgorithmException 
	 */
	public static void modify(DB db, String lang, String ndxPath) throws IOException, ClassNotFoundException, SQLException {
		// open the artcile index
		try {
			Directory fsdir = FSDirectory.open(new File(ndxPath));
			reader = IndexReader.open(fsdir, true);
		} catch (Exception ex) {
			System.out.println("Cannot create index..." + ex.getMessage());
			System.exit(-1);
		}

		initDB(db, lang);

		long sTime, eTime;

		sTime = System.currentTimeMillis();

		int maxid = reader.maxDoc();
		TermFreqVector tv;
		String[] terms;
		String term = "";

		Term t;

		int tfreq = 0;
		float idf;
		float tf;
		float tfidf;
		double inlinkBoost;
		double sum;

		int wikiID;

		int hashInt;

		int numDocs = reader.numDocs();

		TermEnum tnum = reader.terms();
		HashMap<String, Integer> freqMap = new HashMap<String, Integer>(500000);
		HashMap<String, Float> idfMap = new HashMap<String, Float>(500000);

		HashMap<String, Float> tfidfMap = new HashMap<String, Float>(5000);

		HashMap<String, Integer> termHash = new HashMap<String, Integer>(500000);


		FileOutputStream fos = new FileOutputStream("vector.txt");
		OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8");

		tnum = reader.terms();

		hashInt = 0;

		// phase 1: get terms from the index and roughly filter them (occurence count, non-ascii chars)
		System.out.println(" ESA [0] computing idf-map");
		int tCntr = 0;
		while (tnum.next()) {

			t = tnum.term();
			term = t.text();
			if((!lang.equals("zh") && !term.matches("^[a-z]*$")) || 
				(lang.equals("zh") && term.matches("^[a-z0-9]*$")) )
				continue;


			tfreq = tnum.docFreq();	// get DF for the term

			// skip rare terms
			if (tfreq < minTermFreq) {
				continue;
			}

			tCntr++;

			// idf = (float)(Math.log(numDocs/(double)(tfreq+1)) + 1.0);
			idf = (float) (Math.log(numDocs / (double) (tfreq)));
			// idf = (float)(Math.log(numDocs/(double)(tfreq)) / Math.log(2));

			idfMap.put(term, idf);
			termHash.put(term, hashInt++);
			freqMap.put(term, tfreq);
		}
		// phase 1 done
		System.out.println(" ESA [0] - term count: " + tCntr);

		// phase 2: build the file with a list, where each row has the following format:
		//   <term id> <term> <doc id> <tfidf>
		int cntr = 0;
		int cntrX = 0;
		long start = System.currentTimeMillis();
		for (int i = 0; i < maxid; i++) {
			try {
				if (!reader.isDeleted(i)) {
					//System.out.println(i);
					wikiID = Integer.valueOf(reader.document(i).getField("id").stringValue());
					inlinkBoost = 1.0; // inlinkMap.get(wikiID);

					tv = reader.getTermFreqVector(i, "contents");
					if (tv == null) {
						continue;
					}
					try {
						terms = tv.getTerms();

						int[] fq = tv.getTermFrequencies();


						sum = 0.0;
						tfidfMap.clear();

						// for all terms of a document
						for (int k = 0; k < terms.length; k++) {
							term = terms[k];
							if (!idfMap.containsKey(term)) {
								continue;
							}

							tf = (float) (1.0 + Math.log(fq[k]));
							// tf = (float) (1.0 + Math.log(fq[k]) / Math.log(2));

							idf = idfMap.get(term);

							tfidf = (float) (tf * idf);
							tfidfMap.put(term, tfidf);

							sum += tfidf * tfidf;

						}


						sum = Math.sqrt(sum);

						// for all terms of a document
						for (int k = 0; k < terms.length; k++) {
							term = terms[k];
							if (!idfMap.containsKey(term)) {
								continue;
							}

							tfidf = (float) (tfidfMap.get(term) / sum * inlinkBoost);


							// System.out.println(i + ": " + term + " " + fq[k] + " " + tfidf);

							// ++++ record to DB (term,doc,tfidf) +++++
							osw.write(termHash.get(term) + "\t" + term + "\t" + wikiID + "\t" + df.format(tfidf) + "\n");

						}
						cntr += 1;
						cntrX += 1;

					} catch (Exception e) {
						e.printStackTrace();
						System.out.println("ERR: " + wikiID + " " + tv);
						continue;
					}

				}
			} catch (Exception e) {
				System.out.println("Exception when processing: " + reader.document(i).getField("id").stringValue());
			}
			if(System.currentTimeMillis() - start > 1000) {
				System.out.println("Processed: " + cntrX + " docs/1 second [" + (((float)i)/maxid) * 100 + " %]");
				start = System.currentTimeMillis();
				cntrX = 0;
			}
		}
		osw.close();
		fos.close();

		// phase 2: done
		System.out.println(" ESA [1] processed " + cntr + " docs");

		// phase 3: sort the produced list according to the term (now they are sorted according to document)
		String[] cmd = {"/bin/sh", "-c", "sort -T . -S 1400M -n -t\\\t -k1 < vector.txt > vsorted.txt"};
		Process p1 = Runtime.getRuntime().exec(cmd);
		try {
			int exitV = p1.waitFor();
			if (exitV != 0) {
				System.out.println(" !exit");
				System.out.println(DB.readBinaryStream(p1.getErrorStream()));
				System.exit(1);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
			System.out.println(DB.readBinaryStream(p1.getErrorStream()));
			System.out.println(" !exit2");
			System.exit(1);
		}

		// delete unsorted doc-score file
		p1 = Runtime.getRuntime().exec("rm vector.txt");
		try {
			int exitV = p1.waitFor();
			if (exitV != 0) {
				System.exit(1);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
			System.exit(1);
		}
		// phase 3: done
		System.out.println(" ESA [2] sorting finished");

		// phase 4: prune the list and insert it into db, effectivelly
		//          creating the ESA background
		FileInputStream fis = new FileInputStream("vsorted.txt");
		InputStreamReader isr = new InputStreamReader(fis, "UTF-8");
		BufferedReader bir = new BufferedReader(isr);

		String line;
		String prevTerm = null;
		int doc;
		float score;
		TIntFloatHashMap hmap = new TIntFloatHashMap(100);

		// for pruning
		int mark, windowMark;
		float first = 0, last = 0, highest = 0;
		float[] window = new float[WINDOW_SIZE];

		while ((line = bir.readLine()) != null) {
			final String[] parts = line.split("\t");
			term = parts[1];

			// prune and write the vector
			if (prevTerm != null && !prevTerm.equals(term)) {
				int[] arrDocs = hmap.keys();
				float[] arrScores = hmap.getValues();

				HeapSort.heapSort(arrScores, arrDocs);

				// prune the vector

				mark = 0;
				windowMark = 0;
				highest = first = last = 0;

				ByteArrayOutputStream baos = new ByteArrayOutputStream(50000);
				DataOutputStream tdos = new DataOutputStream(baos);

				for (int j = arrDocs.length - 1; j >= 0; j--) {
					score = arrScores[j];

					// sliding window

					window[windowMark] = score;

					if (mark == 0) {
						highest = score;
						first = score;
					}

					if (mark < WINDOW_SIZE) {
						tdos.writeInt(arrDocs[j]);
						tdos.writeFloat(score);
					} else if (highest * WINDOW_THRES < (first - last)) {
						tdos.writeInt(arrDocs[j]);
						tdos.writeFloat(score);

						if (windowMark < WINDOW_SIZE - 1) {
							first = window[windowMark + 1];
						} else {
							first = window[0];
						}
					} else {
						// truncate
						break;
					}

					last = score;

					mark++;
					windowMark++;

					windowMark = windowMark % WINDOW_SIZE;

				}

				ByteArrayOutputStream dbvector = new ByteArrayOutputStream();
				DataOutputStream dbdis = new DataOutputStream(dbvector);
				dbdis.writeInt(mark);
				dbdis.flush();
				dbvector.write(baos.toByteArray());
				dbvector.flush();

				dbdis.close();

				// write to DB
				pstmtVector.setString(1, prevTerm);
				pstmtVector.setBytes(2, dbvector.toByteArray());

				pstmtVector.addBatch();

				tdos.close();
				baos.close();

				hmap.clear();
			}
			pstmtVector.executeBatch();

			doc = Integer.valueOf(parts[2]);
			score = Float.valueOf(parts[3]);

			hmap.put(doc, score);

			prevTerm = term;
		}

		bir.close();
		isr.close();
		fis.close();

		// record term IDFs
		FileOutputStream tos = new FileOutputStream("term.txt");
		OutputStreamWriter tsw = new OutputStreamWriter(tos, "UTF-8");

		//stmtLink.execute("CREATE TABLE " + termsTable + " (term text, vector text);");
		for (String tk : idfMap.keySet()) {
			tsw.write("'" + tk.replace("\\", "\\\\").replace("'", "\\'") + "'\t" + idfMap.get(tk) + "\n");
			stmtLink.execute("INSERT INTO " + termsTable + " VALUES ('" + tk.replace("\\", "\\\\").replace("'", "\\'") + "', '" + idfMap.get(tk) + "');");
		}
		osw.close();
		tsw.close();
		//stmtLink.execute(strTermLoadData);
		stmtLink.execute("CREATE INDEX terms_term ON " + termsTable + " (term)");
		stmtLink.execute("CREATE INDEX idx_term ON " + ndxTable + " (term)");

		eTime = System.currentTimeMillis();

		// phase 4: done
		System.out.println(" ESA [3] total time (sec): " + (eTime - sTime) / 1000.0);


		reader.close();

	}

	/**
	 * Commandline invocation of the the ESAIndexBuilder.
	 * @param args
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws SQLException 
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {
		AppConfig cfg = AppConfig.getInstance();
		cfg.setSection("ESAIndexBuilder");

		DBConfig dbc = new DBConfig();
		dbc.setConnectionFromDrupalUrl(cfg.getSString("db"));

		DB db = new DB(dbc);

		ESAIndexBuilder.setMinTermFreq(cfg.getSInt("minTermFreq"));
		ESAIndexBuilder.modify(db, cfg.getSString("lang"), cfg.getSString("esaIndexPath"));
	}
}

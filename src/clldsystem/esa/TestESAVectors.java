package clldsystem.esa;

import clldsystem.esa.ESAAnalyzer;
import common.db.DB;
import common.db.DBConfig;
import common.config.AppConfig;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

import java.io.FileInputStream;

/**
 * Does some computation with ESA vectors of documents. For demonstration and debugging purposes.
 * @author zilka
 */
public class TestESAVectors {

	static Connection connection;
	static Statement stmtQuery;
	static DB db;
	static DB dbEsa;
	static String strTitles = "SELECT page_id AS concept_id, page_title FROM page WHERE page_id IN ";

	public static void initDB() throws ClassNotFoundException, SQLException, IOException {
		DBConfig dbc = new DBConfig();
		dbc.setConnectionFromDrupalUrl(AppConfig.getInstance().getString("TestESAVectors.db"));
		DBConfig dbcEsa = new DBConfig();
		dbcEsa.setConnectionFromDrupalUrl(AppConfig.getInstance().getString("TestESAVectors.dbEsa"));

		db = new DB(dbc);
		dbEsa = new DB(dbcEsa);

		connection = db.getConnection();

		stmtQuery = connection.createStatement();
		stmtQuery.setFetchSize(100);

	}

	/**
	 * @param args
	 * @throws IOException 
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
		initDB();

		// initialize the searcher and analyze two input documents
		ESAAnalyzer searcher = new ESAAnalyzer(dbEsa, 
			AppConfig.getInstance().getString("TestESAVectors.lang"), 
			AppConfig.getInstance().getString("TestESAVectors.stopwordsFile"), 
			AppConfig.getInstance().getString("TestESAVectors.stemmerClass"));

		BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(
			AppConfig.getInstance().getString("TestESAVectors.inputFile")), "UTF-8"));
		BufferedReader in2 = new BufferedReader(new InputStreamReader(new FileInputStream(
			AppConfig.getInstance().getString("TestESAVectors.inputFile2")), "UTF-8"));

		// read the first file
		String text = "";
		String nText;
		while ((nText = in.readLine()) != null) {
			text += nText + " ";
		}
		
		// read the second file
		String text2 = "";
		String nText2;
		while ((nText2 = in2.readLine()) != null) {
			text2 += nText2 + " ";
		}

		// create ESA vector of the first file
		IConceptVector cv = searcher.getConceptVector(text);
		//IConceptVector cv2 = searcher.getConceptVector(text2);
		IConceptVector cv2 = null;
		
		// convert the vector to binary format
		byte[] v = ESAAnalyzer.buildVector(ESAAnalyzer.trim(cv, 100));
		System.out.println("Searching for:");
		for (Byte i : v) {
			System.out.print(String.format("%02x", i));
		}
		
		System.out.println();
		//IConceptVector cv2 = searcher.getConceptVector(AppConfig.getInstance().getString("TestESAVectors.inputString"));
		System.out.println("dimensions: " + cv.size());
		System.out.println("dimensions2: " + cv2.size());
		System.out.println(searcher.getRelatedness(cv, cv2));
		/*
		for (int k = 100; k < 100000; k += 1000) {
			IConceptVector cvExp = new TroveConceptVector(k);
			IConceptIterator it = cv.orderedIterator();

			for (int i = 0; i < k; i++) {
				it.next();
				cvExp.add(it.getId(), it.getValue());
			}
			System.out.printf("similarity (short %d): %.8f\n", k, searcher.getRelatedness(cvExp, cv));
		}

		//IConceptVector cv = searcher.getNormalVector(cvx,50);

		if (cv == null || cv2 == null) {
			System.exit(1);
		}

		System.out.printf("similarity: %.8f\n", searcher.getRelatedness(cv, cv));
		System.exit(0);
		 *
		 */

		IConceptIterator it = cv.orderedIterator();

		HashMap<Integer, Double> vals = new HashMap<Integer, Double>(50);
		HashMap<Integer, String> titles = new HashMap<Integer, String>(50);

		String inPart = "(";

		int count = 0;
		while (it.next() && count < 50) {
			inPart += it.getId() + ",";
			vals.put(it.getId(), it.getValue());
			count++;
		}

		inPart = inPart.substring(0, inPart.length() - 1) + ")";

		ResultSet r = stmtQuery.executeQuery(strTitles + inPart);
		while (r.next()) {
			titles.put(r.getInt(1), new String(r.getBytes(2), "UTF-8"));
		}

		count = 0;
		it.reset();
		while (it.next() && count < 50) {
			int id = it.getId();
			//System.out.println(id + "\t\t" + id + "\t\t" + vals.get(id));
			System.out.println(id + "\t\t" + titles.get(id) + "\t\t"
				+ vals.get(id));
			count++;
		}


	}
}

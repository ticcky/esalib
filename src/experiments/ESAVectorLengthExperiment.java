package experiments;

import common.db.DB;
import common.db.DBConfig;
import common.ProgressReporter;
import common.Utils;
import common.config.AppConfig;
import clldsystem.esa.ESAAnalyzer;
import clldsystem.esa.IConceptIterator;
import clldsystem.esa.IConceptVector;
import clldsystem.esa.TroveConceptVector;
import gnuplot.GNUPlot;
import gnuplot.GNUPlotData;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Generates graph of correlation of results between uncut esa vector and a cut esa vector 
 * at different places (Y axis)
 * @author zilka
 */
public class ESAVectorLengthExperiment {

	public static String cachePath = "/tmp/evle/";

	public static IConceptVector cutVector(IConceptVector v, Integer cut) {
		IConceptVector res = new TroveConceptVector(cut);
		IConceptIterator it = v.orderedIterator();
		it.reset();
		if (cut == -1) {
			cut = v.size();
		}
		int cntr = 0;
		while (it.next() && cntr < cut) {
			res.add(it.getId(), it.getValue());
			cntr++;
		}

		return res;

	}

	public static double getCorrelation(HashMap<Integer, Integer> o1, HashMap<Integer, Integer> o2) {

		double n = 0.0;
		double N = 0; //o1.keySet().size();
		for (Integer i : o1.keySet()) {
			double x1 = o1.get(i) - o2.get(i);
			x1 *= x1;
			n += x1;
			N++;
		}

		return 1 - 6 * n / (N * (N * N - 1));
	}

	public static double getTopCorrelation(HashMap<Integer, Integer> o1, HashMap<Integer, Integer> o2, int top) {

		double n = 0.0;
		double N = 0; //o1.keySet().size();
		for (Integer i : o1.keySet()) {
			if (i > top) {
				continue;
			}
			double x1 = o1.get(i) - o2.get(i);
			x1 *= x1;
			n += x1;
			N++;
		}

		return 1 - 6 * n / (N * (N * N - 1));
	}

	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
		AppConfig cfg = AppConfig.getInstance();
		cfg.setSection("ESAVectorLengthExperiment");

		DBConfig dbc = new DBConfig();
		dbc.setConnectionFromDrupalUrl(cfg.getSString("db"));

		DBConfig dbcEsa = new DBConfig();
		dbcEsa.setConnectionFromDrupalUrl(cfg.getSString("esaDb"));

		DB db = new DB(dbc);
		DB dbEsa = new DB(dbcEsa);

		ESAAnalyzer searcher = new ESAAnalyzer(
			dbEsa, cfg.getSString("lang"), cfg.getSString("stopwordsFile"),
			cfg.getSString("stemmerClass"));

		ProgressReporter pr;

		pr = new ProgressReporter(">> query concept extraction");
		pr.start();

		// get query vector
		String query = cfg.getSString("query");
		query = Utils.readBinaryStream(new FileInputStream(query));
		//query = "linux";
		IConceptVector queryVector = searcher.getConceptVector(query);

		pr.finish();

		pr = new ProgressReporter(">> choose documents");
		pr.start();

		// choose N documents
		// - random
		// - related
		List<IConceptVector> docVectors = new ArrayList<IConceptVector>();
		ResultSet res = db.executeSelect(cfg.getSString("documentListQuery"));
		String content;
		int x = 0;
		IConceptVector newVector;
		while (res.next()) {
			content = DB.readBinaryStream(res.getBinaryStream("content"));
			newVector = searcher.getConceptVector(content);
			/*
			File cacheFile = new File(cachePath, res.getInt("page_id")
				+ ".cache");
			try {

				FileInputStream fis = new FileInputStream(cacheFile);
				newVector = CLWESASearcher.getVector(fis);
			} catch (Exception e) {
				ByteArrayOutputStream bos = new ByteArrayOutputStream();
				FileWriter fw = new FileWriter(cacheFile);
				bos.write(CLWESASearcher.buildVector(newVector));
				fw.write(bos.toString());
				bos.close();
				fw.close();
			}
			 *
			 */
			docVectors.add(newVector);
			x++;
			System.out.println(x);
		}
		pr.finish();

		int maxCut = 0;
		// for different cuts from 50 to ... compute the similarity order
		List<Integer> cuts = new ArrayList<Integer>();
		for (int cut = 50; cut < 500; cut += 50) {
			cuts.add(cut);
			maxCut = Math.max(cut, maxCut);
		}
		for (int cut = 500; cut < 10000; cut += 1000) {
			cuts.add(cut);
			maxCut = Math.max(cut, maxCut);
		}
		cuts.add(-1);

		List<IConceptVector> results = new ArrayList<IConceptVector>();
		for (Integer cut : cuts) {
			pr = new ProgressReporter(">> compute the similarity order for the cut of "
				+ cut);
			pr.start();
			// can cut the query vector there, but now we don't want to
			IConceptVector queryVectorX = cutVector(queryVector, cut);

			// prepare the resulting list (docId, score)
			IConceptVector tmpRes = new TroveConceptVector(docVectors.size());
			results.add(tmpRes);
			int docCntr = 0;

			// for each document, compute the cosine score
			for (IConceptVector docVector : docVectors) {
				IConceptVector cutDocVector = cutVector(docVector, cut);
				double score = searcher.getRelatedness(queryVectorX, cutDocVector);
				System.out.printf(">>> %d: %.16f\n", docCntr, score);
				tmpRes.add(docCntr, score);
				docCntr++;
			}
			pr.finish();
		}

		//record the similarity order
		List<HashMap<Integer, Integer>> simOrder = new ArrayList<HashMap<Integer, Integer>>();
		for (IConceptVector cutRes : results) {
			int cntr = 0;
			IConceptIterator it = cutRes.orderedIterator();
			HashMap<Integer, Integer> tmpRes = new HashMap<Integer, Integer>();
			simOrder.add(tmpRes);
			while (it.next()) {
				tmpRes.put(it.getId(), cntr);
				if (it.getValue() > 0.0) {
					cntr++;
				}
			}
		}

		HashMap<Integer, Integer> referenceOrd = simOrder.get(simOrder.size()
			- 1);

		GNUPlotData d = new GNUPlotData();
		//GNUPlotData d10 = new GNUPlotData();

		int cntr = 0;
		//compute the correlations
		for (HashMap<Integer, Integer> ordRes : simOrder) {
			double score = getCorrelation(ordRes, referenceOrd);
			//double score10 = getTopCorrelation(ordRes, referenceOrd, 10);
			int cut = cuts.get(cntr);
			if (cut == -1) {
				cut = maxCut + 1;
			}
			d.addData(cut, score);
			//d10.addData(cut, score10);
			cntr++;
		}


		GNUPlot gp = new GNUPlot();
		gp.setPlotStyle(GNUPlot.PLOT_LINES);
		gp.plot(d, "/xdisk/devel/kmi/clld/esa_cli_fq.png");
		//gp.plot(d10, "/xdisk/devel/kmi/clld/esa_cli10_fq.png");





	}
}

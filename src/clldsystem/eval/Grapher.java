package clldsystem.eval;

import clldsystem.eval.Grapher.EvalData;
import common.db.DB;
import common.db.DBConfig;
import common.ProgressReporter;
import common.config.AppConfig;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import gnuplot.GNUPlot;
import gnuplot.GNUPlotData;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;

/**
 * Produces loads of PR graphs and a few html pages for results of some method, compared
 * to some ground truth.
 * @author zilka
 */
public class Grapher {

	DB evalDb;
	String plotDir;
	String plotDirParent;
	String tableName;
	String dstDirName;
	String truthTableName;
	Set<Integer> relevantPages;
	String truthTagConstraint;
	String conceptMappingTableName;
	String pagesSrcTableName;
	String pagesDstTableName;
	EvalData sumData;

	public Grapher(DB evalDb, String plotDir, String tableName, String truthTableName, String truthTagConstraint) {
		this.evalDb = evalDb;
		this.tableName = tableName;
		this.truthTableName = truthTableName;
		this.truthTagConstraint = truthTagConstraint;
		this.dstDirName = tableName;
		if (!this.truthTagConstraint.equals("%")) {
			this.dstDirName += "_" + this.truthTagConstraint;
		}
		plotDirParent = plotDir;
		new File(plotDir, dstDirName).mkdir();
		this.plotDir = new File(plotDir, dstDirName).getAbsolutePath();
	}

	public void setConceptMappingTableName(String conceptMappingTableName) {
		this.conceptMappingTableName = conceptMappingTableName;
	}

	public void setPagesSrcTableName(String pagesTableName) {
		this.pagesSrcTableName = pagesTableName;
	}

	public void setPagesDstTableName(String pagesDstTableName) {
		this.pagesDstTableName = pagesDstTableName;
	}

	/**
	 * Computes data for evaluation of one page. Those can be then used for 
	 * plotting the graphs.
	 * @param pageId
	 * @return 
	 */
	EvalData getData(int pageId) {
		try {
			// initialize the result
			EvalData data = new EvalData();

			// get the ground truth for this page
			// TODO: cross database is fixed now, make it adjustable
			ResultSet res;
			PreparedStatement psTruthSize =
				evalDb.getConnection().prepareStatement(
				"SELECT count(*) AS count "
				+ "FROM (SELECT DISTINCT t.* "
				+ "FROM " + truthTableName + " t "
				+ "LEFT JOIN " + conceptMappingTableName
				+ " cm "
				+ "ON t.page_id = cm.concept_id "
				+ "WHERE cm.concept_id = " + pageId + " "
				+ "AND (t.tag LIKE '" + truthTagConstraint
				+ "' "
				+ "OR t.tag IS NULL)"
				+ ") x");

			int truthSize = 0;
			res = psTruthSize.executeQuery();
			if (res.next()) {
				truthSize = res.getInt("count");
				data.truthSize = truthSize;
			} else {
				return null;
			}

			System.out.println("> #" + pageId + ", truth size: "
				+ truthSize);
			if (truthSize == 0) {
				return data;
			}


			ResultSet pageIdRes = evalDb.executeSelect("SELECT page_id "
				+ "FROM " + conceptMappingTableName + " "
				+ "WHERE concept_id = " + pageId);

			if (pageIdRes.next()) {
				data.mappedId = pageIdRes.getInt("page_id");
			} else {
				data.mappedId = -1;
			}

			// for measuring the rank of the correct page in the similarity results
			String similaritySearchTableName = "sr_" + tableName;
			try {
				ResultSet similRankRes = evalDb.executeSelect("SELECT rank FROM sr_"
					+ tableName + " r LEFT JOIN "
					+ conceptMappingTableName
					+ " cm ON cm.page_id = r.doc_id WHERE cm.concept_id = "
					+ pageId);
				if (similRankRes.next()) {
					data.moreSimilarCnt = similRankRes.getInt("rank");
				}
			} catch (Exception e) {
				data.moreSimilarCnt = -2;
			}

			//res = evalDb.executeSelect("SELECT r.page_id, r.page_id = t.page_id as result FROM " + tableName + " r LEFT JOIN wikidb_cs.concept_mapping cm ON cm.concept_id = r.links_to LEFT JOIN link_truth t ON t.page_id = r.page_id AND t.links_to = cm.page_id WHERE r.page_id = " + pageId);

			// the MASTER query of comparing the truth with the results
			String query =
				"SELECT r.page_id, r.links_to, t.page_id IS NOT NULL AS result "
				+ "FROM " + tableName + " r "
				+ "LEFT JOIN " + conceptMappingTableName
				+ " cm "
				+ "ON cm.page_id = r.page_id "
				+ "LEFT JOIN " + truthTableName + " t "
				+ "ON t.page_id = cm.concept_id AND t.links_to = r.links_to "
				// just for the gain
				//+ "LEFT JOIN " + conceptMappingTableName + " cm2 "
				//+ "ON cm2.concept_id = r.links_to "

				+ "WHERE cm.concept_id = " + pageId + " "
				// just for the gain
				//+ "AND cm2.page_id IS NULL "

				+ "AND (t.tag LIKE '" + truthTagConstraint
				+ "' OR t.tag IS NULL) "
				+ "GROUP BY r.page_id, r.links_to "
				//+ "ORDER BY (r.score - 0.5)*(r.score - 0.5) ";
				+ "ORDER BY r.score DESC ";
			res = evalDb.executeSelect(query);

			if (res == null) {
				System.out.println("Error, when trying to get the data from the database. Evaluation table probably doesn't exist.");
				return null;
			}


			// compute the P, R, F ... measures on the result data
			int n = 0;
			int good = 0;
			float P = (float) 0.0;
			float R = (float) 0.0;
			float Fx = (float) 0.0;
			float fxBeta = (float) 1.0;
			float totalFx = (float) 0.0;
			float fxSamplingRate = (float) 0.1;
			while (res.next()) {
				data.empty = false;
				int resVal = res.getInt("result");
				n++;

				if (resVal == 1) {
					good++;
					data.goodResults.add(res.getInt("links_to"));
				} else {
					data.badResults.add(res.getInt("links_to"));
				}
				float xRel = ((float) n) / truthSize;
				P = good / ((float) n);
				R = good / ((float) truthSize);
				Fx = (float) ((1 + fxBeta * fxBeta) * P * R / (fxBeta
					* fxBeta * P + R));
				if (Float.isNaN(Fx)) {
					Fx = (float) 0.0;
				}

				data.P.addData(n, P);
				data.R.addData(n, R);
				data.PR.addData(R, P);
				data.F.addData(n, 2 * (P * R) / (P + R));
				data.counter.addData(n, good);

				if (n == truthSize) {
					data.totalPrec = P;
				}

				if (n <= 10 * truthSize) {
					data.foundLinksIn2T = good;
					try {
						data.P_norm.addData(Math.round(xRel
							* 100), P);
						data.R_norm.addData(Math.round(xRel
							* 100), R);
						data.Fx.addData(xRel, Fx);
						if (n <= truthSize) {
							totalFx += Fx;
						} else {
							totalFx += (-(xRel - 1) * (xRel
								- 1) + 1) * Fx;
						}
						//totalFx += Utils.gauss((((float) n) / truthSize), 0.0, 0.5) * Fx;
					} catch (ArithmeticException e) {
					}
				}
			}
			data.foundLinks = good;
			data.totalRecall = R;

			// finish the plots in case we have less link suggestions than is entries in the truth
			while (n < truthSize * 10) {
				n++;
				float xRel = ((float) n) / truthSize;
				if (n == truthSize) {
					data.totalPrec = P;
				}
				try {
					data.P_norm.addData(Math.round(xRel
						* 100), P);
					data.R_norm.addData(Math.round(xRel
						* 100), R);
					data.Fx.addData(((float) n) / truthSize, Fx);
					if (n <= truthSize) {
						totalFx += Fx;
					} else {
						totalFx += (-(xRel - 1) * (xRel
							- 1) + 1) * Fx;
					}
				} catch (ArithmeticException e) {
				}
			}
			totalFx /= truthSize;

			data.totalFx = totalFx;
			
			return data;
		} catch (SQLException ex) {
			Logger.getLogger(Grapher.class.getName()).log(Level.SEVERE, null, ex);
			return null;
		}
	}

	/**
	 * Plots the evaluation results (computed in getData).
	 * @param d
	 * @param pageId
	 * @throws IOException 
	 */
	void plot(EvalData d, int pageId) throws IOException {
		if (!d.empty) {
			GNUPlot gp = new GNUPlot();
			//gp.setPlotStyle(GNUPlot.PLOT_DOTS);
			gp.setPlotStyle(GNUPlot.PLOT_LINES);

			// P
			gp.setxLabel("# of links");
			gp.setyLabel("Precision");
			gp.resetXRange();
			gp.setYRange((float) 0.0, (float) 1.1);
			gp.plot(d.P, new File(plotDir, pageId + ".P.png").getAbsolutePath());

			// P_norm
			gp.setxLabel("% of truth");
			gp.setyLabel("Precision");
			gp.resetXRange();
			gp.setYRange((float) 0.0, (float) 1.1);
			gp.plot(d.P_norm, new File(plotDir, pageId
				+ ".P_norm.png").getAbsolutePath());

			// R
			gp.setxLabel("# of links");
			gp.setyLabel("Recall");
			gp.resetXRange();
			gp.setYRange((float) 0.0, (float) 1.1);
			gp.plot(d.R, new File(plotDir, pageId + ".R.png").getAbsolutePath());

			// R_norm
			gp.setxLabel("% of truth");
			gp.setyLabel("Recall");
			gp.resetXRange();
			gp.setYRange((float) 0.0, (float) 1.1);
			gp.plot(d.R_norm, new File(plotDir, pageId
				+ ".R_norm.png").getAbsolutePath());

			gp.setxLabel("# of links");
			gp.setyLabel("F1-measure");
			gp.resetXRange();
			gp.setYRange((float) 0.0, (float) 1.1);
			gp.plot(d.F, new File(plotDir, pageId + ".F.png").getAbsolutePath());

			/*gp.setxLabel("# of links");
			gp.setyLabel("Fx-measure");
			gp.resetXRange();
			gp.setYRange((float) 0.0, (float) 1.1);
			gp.plot(d.Fx, new File(plotDir, pageId + ".Fx.png").getAbsolutePath());
			 *
			 */

			gp.setxLabel("Recall");
			gp.setyLabel("Precision");
			gp.setXRange((float) 0.0, (float) 1.1);
			gp.setYRange((float) 0.0, (float) 1.1);
			gp.plot(d.PR, new File(plotDir, pageId + ".PR.png").getAbsolutePath());

			gp.setxLabel("# of links");
			gp.setyLabel("# of correct links");
			gp.resetXRange();
			gp.setYRange((float) 0.0, d.truthSize);
			gp.plot(d.counter, new File(plotDir, pageId
				+ ".counter.png").getAbsolutePath());

		} else {
			System.err.println("Empty result #" + pageId);
		}

	}

	List<Integer> getPages() {
		try {
			List<Integer> pages = new ArrayList<Integer>();
			PreparedStatement psPages = evalDb.getConnection().prepareStatement("SELECT t.page_id FROM "
				+ truthTableName + " t GROUP BY page_id");
			ResultSet res = psPages.executeQuery();
			while (res.next()) {
				pages.add(res.getInt("page_id"));
			}
			return pages;
		} catch (SQLException ex) {
			Logger.getLogger(Grapher.class.getName()).log(Level.SEVERE, null, ex);
			return null;
		}

	}

	/**
	 * Wrapper for the whole graphing process. Iterates over the pages and
	 * draws all graphs.
	 */
	private void graphAll() {
		try {
			List<Integer> pages = getPages();
			sumData = new EvalData();

			Configuration cfg = new Configuration();
			cfg.setDirectoryForTemplateLoading(new File("templates/"));

			Template templ = cfg.getTemplate("graphs.all.html");
			//cfg.setObjectWrapper(ObjectWrapper.BEANS_WRAPPER);

			Map out = new HashMap();
			List<Map> graphs = new ArrayList<Map>();
			out.put("graphs", graphs);
			out.put("dst_lang", "en");
			out.put("src_lang", "es");

			// build the index
			DescriptiveStatistics stats = new DescriptiveStatistics();
			DescriptiveStatistics statsRecall = new DescriptiveStatistics();
			DescriptiveStatistics statsPrec = new DescriptiveStatistics();
			List<Integer> foundSummary = new ArrayList<Integer>();
			//FileOutputStream fos = new FileOutputStream();
			int cntr = 0;
			for (Integer p : pages) {
				Map graph = new HashMap();
				graphs.add(graph);
				cntr++;
				System.out.printf("[%d/%d] %.0f %% ", cntr, pages.size(), ((float) cntr)
					/ pages.size() * 100);
				EvalData d = getData(p);
				sumData.addData(d, pages.size());

				graph.put("page_id", p);
				graph.put("dst_page", p);
				graph.put("src_page", d.mappedId);


				List<String> graphList = new ArrayList<String>();
				graph.put("graphs", graphList);
				graphList.add("P");
				graphList.add("R");
				graphList.add("P_norm");
				graphList.add("R_norm");
				graphList.add("PR");

				graph.put("data", d);
				graph.put("truthSize", d.truthSize);
				graph.put("totalRecall", d.totalRecall);
				graph.put("foundLinks", d.foundLinks);
				graph.put("foundLinksIn2T", d.foundLinksIn2T);
				graph.put("totalFx", d.totalFx);
				graph.put("moreSimilarCnt", d.moreSimilarCnt);


				plot(d, p);
				stats.addValue(d.totalFx);
				statsRecall.addValue(d.totalRecall);
				statsPrec.addValue(d.totalPrec);
				foundSummary.add(d.foundLinksIn2T);
			}
			// prepare PR
			GNUPlotData avgPR = new GNUPlotData();
			GNUPlotData pN = sumData.P_norm;
			GNUPlotData rN = sumData.R_norm;
			Hashtable<Number, Number> pNh = sumData.P_norm.getData();
			Hashtable<Number, Number> rNh = sumData.R_norm.getData();
			for(int i = 0; i < pNh.size(); i++) {
				avgPR.addData(pNh.get(i), rNh.get(i));
			}
			sumData.PR = avgPR;
			plot(sumData, 0);


			double mean = stats.getMean();
			double std = stats.getStandardDeviation();
			out.put("mean_fx", mean);
			out.put("std_fx", std);

			mean = statsRecall.getMean();
			std = statsRecall.getStandardDeviation();
			out.put("mean_recall", mean);
			out.put("std_recall", std);

			mean = statsPrec.getMean();
			std = statsPrec.getStandardDeviation();
			out.put("mean_precision", mean);
			out.put("std_precision", std);
			try {
				templ.process(out, new FileWriter(new File(plotDir, "index.html")));
			} catch (TemplateException ex) {
				Logger.getLogger(Grapher.class.getName()).log(Level.SEVERE, null, ex);
				System.out.println("[E] Couldn't write the index.html");
			}


			System.out.println("F-score of the system: " + mean);
		} catch (IOException ex) {
			Logger.getLogger(Grapher.class.getName()).log(Level.SEVERE, null, ex);
		}

	}

	class EvalData {

		GNUPlotData P;
		GNUPlotData P_norm;
		GNUPlotData R;
		GNUPlotData R_norm;
		GNUPlotData PR;
		GNUPlotData F;
		GNUPlotData counter;
		GNUPlotData Fx;
		float totalFx;
		float totalRecall;
		float totalPrec;
		int truthSize;
		int foundLinks;
		int foundLinksIn2T;
		boolean empty = true;
		List<Integer> badResults;
		List<Integer> goodResults;
		List<Integer> notFoundResults;
		int moreSimilarCnt;
		int mappedId;

		public float getTotalRecall() {
			return totalRecall;
		}

		public EvalData() {
			P = new GNUPlotData();
			P_norm = new GNUPlotData();
			R = new GNUPlotData();
			R_norm = new GNUPlotData();
			PR = new GNUPlotData();
			F = new GNUPlotData();
			Fx = new GNUPlotData();
			counter = new GNUPlotData();

			badResults = new ArrayList<Integer>();
			goodResults = new ArrayList<Integer>();
			notFoundResults = new ArrayList<Integer>();

			moreSimilarCnt = -1;
			mappedId = -1;
		}

		public void addData(EvalData d, double divideBy) {
			P.addData(d.P, divideBy);
			P_norm.addData(d.P_norm, divideBy);
			R.addData(d.R, divideBy);
			R_norm.addData(d.R_norm, divideBy);
			//PR.addData(d.PR, divideBy);
			F.addData(d.F, divideBy);
			//counter.addData(d.counter, divideBy);
			//Fx.addData(d.Fx, divideBy);
			empty = false;
		}
	}

	/**
	 * Generates the overview html page.
	 * @param methods
	 * @param dir 
	 */
	public static void generateOverview(List<String> methods, String dir) {
		try {
			Configuration cfg = new Configuration();
			cfg.setDirectoryForTemplateLoading(new File("templates/"));
			Template templ = cfg.getTemplate("graphs.overview.html");
			//cfg.setObjectWrapper(ObjectWrapper.BEANS_WRAPPER);
			Map out = new HashMap();
			out.put("methods", methods);

			try {
				templ.process(out, new FileWriter(new File(dir, "overview.html")));
			} catch (TemplateException ex) {
				Logger.getLogger(Grapher.class.getName()).log(Level.SEVERE, null, ex);
				System.out.println("[E] Couldn't write the index.html");
			}
		} catch (IOException ex) {
			Logger.getLogger(Grapher.class.getName()).log(Level.SEVERE, null, ex);
		}
	}

	/**
	 * Generates the comparison html page.
	 * @param methods
	 * @param pages
	 * @param graphs
	 * @param dir 
	 */
	public static void generateCompare(List<String> methods, List<Integer> pages, List<String> graphs, String dir) {
		try {
			Configuration cfg = new Configuration();
			cfg.setDirectoryForTemplateLoading(new File("templates/"));
			Template templ = cfg.getTemplate("graphs.compare.html");
			for (String graph : graphs) {
				//cfg.setObjectWrapper(ObjectWrapper.BEANS_WRAPPER);
				Map out = new HashMap();
				out.put("methods", methods);
				out.put("pages", pages);
				out.put("graph", graph);

				try {
					templ.process(out, new FileWriter(new File(dir, "compare."
						+ graph + ".html")));
				} catch (TemplateException ex) {
					Logger.getLogger(Grapher.class.getName()).log(Level.SEVERE, null, ex);
					System.out.println("[E] Couldn't write the index.html");
				}
			}
		} catch (IOException ex) {
			Logger.getLogger(Grapher.class.getName()).log(Level.SEVERE, null, ex);
		}

	}

	/**
	 * Launches the graphing.
	 * @param args
	 * @throws IOException
	 * @throws SQLException
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws IOException, SQLException, ClassNotFoundException {
		AppConfig cfg = AppConfig.getInstance();
		cfg.setSection("Grapher");

		String truthTableName = cfg.getSString("truthTableName");
		String truthTagConstraint = cfg.getSString("truthTagConstraint");
		String plotDir = cfg.getSString("plotDir");

		//String tableNameId = AppConfig.getInstance().getString("Grapher" + configId + ".tableNameId");
		List<String> tableNames = cfg.getSList("tableName");
		//String tableName = AppConfig.getInstance().getString("Grapher" + configId + ".tableName" + tableNameId);
		DBConfig dbcEval = new DBConfig();
		dbcEval.setConnectionFromDrupalUrl(cfg.getSString("evalDb"));
		DB evalDb = new DB(dbcEval);

		String conceptMappingTable = cfg.getSString("conceptMappingTableName");
		String pagesSrcTable = cfg.getSString("pagesSrcTableName");
		String pagesDstTable = cfg.getSString("pagesDstTableName");

		List<String> graphs = new ArrayList<String>();
		graphs.add("P_norm");
		graphs.add("R_norm");
		graphs.add("F");
		graphs.add("PR");

		Grapher g = new Grapher(evalDb, plotDir, "", truthTableName, truthTagConstraint);
		g.setConceptMappingTableName(conceptMappingTable);

		List<Integer> pages = g.getPages();
		Grapher.generateOverview(tableNames, plotDir);
		Grapher.generateCompare(tableNames, pages, graphs, plotDir);
		for (String tableName : tableNames) {
			g = new Grapher(evalDb, plotDir, tableName, truthTableName, truthTagConstraint);
			g.setConceptMappingTableName(conceptMappingTable);
			g.setPagesSrcTableName(pagesSrcTable);
			g.setPagesDstTableName(pagesDstTable);

			ProgressReporter pr = new ProgressReporter("graphed");
			pr.start();
			System.out.println(">>> Graphing: " + tableName);
			g.graphAll();
			pr.finish();
			// for each topic
		}

	}
}

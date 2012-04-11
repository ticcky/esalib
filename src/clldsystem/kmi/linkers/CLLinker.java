package clldsystem.kmi.linkers;

import clldsystem.esa.ConceptVectorSimilarity;
import clldsystem.esa.CosineScorer;
import clldsystem.esa.ESAAnalyzer;
import clldsystem.esa.ESAIndexSearcher;
import clldsystem.esa.IConceptIterator;
import clldsystem.esa.IConceptVector;
import clldsystem.esa.TroveConceptVector;
import com.mysql.jdbc.NotImplemented;
import common.db.DB;
import common.HeapSort;
import common.ProgressReporter;
import common.wiki.WikipediaTitleResolver;
import common._;
import de.tudarmstadt.ukp.wikipedia.parser.Paragraph;
import de.tudarmstadt.ukp.wikipedia.parser.ParsedPage;
import de.tudarmstadt.ukp.wikipedia.parser.Section;
import de.tudarmstadt.ukp.wikipedia.parser.mediawiki.MediaWikiParser;
import de.tudarmstadt.ukp.wikipedia.parser.mediawiki.MediaWikiParserFactory;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import clldsystem.model.Link;
import clldsystem.model.LinkSet;

/**
 * Cross-language links using different methods. Dispatched through the linkText method.
 * @author zilka
 */
public class CLLinker extends Linker {

	public static int ESA_VECTOR_DIMENSIONS = 1;
	public static int SIMILAR_DOCUMENTS = 2;
	public static int ESA_VECTOR_SUM = 3;
	public static int SIMILAR_SIMILAR = 4;
	public static int USE_WIKIPEDIA_LINKS = 5;
	PreparedStatement psEsaVector;
	PreparedStatement psClEsa;
	PreparedStatement psEsa2Stage;
	int N = 3;
	int N2;
	int nDimensions = 100;
	int linkExtractionMethod = CLLinker.ESA_VECTOR_DIMENSIONS;
	boolean splitBeforeSrcEsa = false;
	boolean splitBeforeDstEsa = false;
	DB esaIndexDb;
	DB esaIndexCacheDb;
	HashMap<Integer, List<Integer>> conceptMapping;
	String esaIndexPath;

	public CLLinker(DB db, DB dbConcept, DB esaDb, DB destDb, DB esaIndexDb, DB esaIndexCacheDB, String resTable, String articleIter, String srcLang, String srcLangStopWordsFile, String srcLangStemmer, String dstLang, String esaVectorIndexTable) {
		super(db, dbConcept, esaDb, destDb, resTable, articleIter, srcLang, srcLangStopWordsFile, srcLangStemmer, dstLang);

		this.esaIndexDb = esaIndexDb;
		this.esaIndexCacheDb = esaIndexCacheDB;

		try {
			// prepare the statement for searching the esa vector index for the similar documents
			psEsaVector = esaIndexDb.getConnection().prepareStatement("SELECT doc_id, vector FROM " + esaVectorIndexTable + " WHERE doc_id = ?");
			psClEsa = esaIndexDb.getConnection().prepareStatement("SELECT doc_id, esa_simil(vector, ?) AS simil, vector FROM " + esaVectorIndexTable + " ORDER BY simil DESC LIMIT ?");
			psEsa2Stage = esaIndexDb.getConnection().prepareStatement("SELECT doc_id, esa_simil(vector, ?) AS simil, vector FROM " + esaVectorIndexTable + " ORDER BY simil DESC LIMIT ?");

			conceptMapping = new HashMap<Integer, List<Integer>>();
			ResultSet res = db.executeSelect("SELECT * FROM concept_mapping");
			while (res.next()) {
				List<Integer> curr = conceptMapping.get(res.getInt("page_id"));
				if(curr == null) {
					curr = new ArrayList<Integer>();
					conceptMapping.put(res.getInt("page_id"), curr);
				}

				curr.add(res.getInt("concept_id"));
			}
		} catch (SQLException ex) {
			Logger.getLogger(CLLinker.class.getName()).log(Level.SEVERE, null, ex);
		}
	}

	public void setEsaIndexPath(String esaIndexPath) {
		this.esaIndexPath = esaIndexPath;
	}

	public void setN(int N) {
		this.N = N;
	}

	public int getN() {
		return N;
	}

	public void setnDimensions(int nDimensions) {
		this.nDimensions = nDimensions;
	}

	public void setSplitBeforeDstEsa(boolean splitBeforeDstEsa) {
		this.splitBeforeDstEsa = splitBeforeDstEsa;
	}

	public void setSplitBeforeSrcEsa(boolean splitBeforeSrcEsa) {
		this.splitBeforeSrcEsa = splitBeforeSrcEsa;
	}

	public void setLinkExtractionMethod(int linkExtractionMethod) {
		this.linkExtractionMethod = linkExtractionMethod;
	}

	@Override
	LinkSet linkText(String pageTitle, String content, int pageId) {
		try {
			ProgressReporter pr = new ProgressReporter(">>> extract source vectors");
			pr.start();

			// split text into paragraphs
			String[] pars;
			if (splitBeforeSrcEsa) {
				pars = splitText(content);
			} else {
				pars = new String[]{content};
			}

			// find ESA concepts for each part
			List<IConceptVector> cvs = new ArrayList<IConceptVector>();
			int totalLength = 0;
			for (String par : pars) {
				IConceptVector cv = searcher.getConceptVector(par);
				if (cv != null) {
					cvs.add(cv);
					totalLength += cv.count();
				}
			}

			// sort all esa concepts over all paragraphs by their value
			int cCount = 0;
			int[] ids = new int[totalLength];
			double[] vals = new double[totalLength];
			for (IConceptVector cv : cvs) {
				IConceptIterator it = cv.orderedIterator();
				while (it.next()) {
					ids[cCount] = it.getId();
					vals[cCount] = it.getValue();
					cCount++;
				}
			}
			HeapSort.heapSort(vals, ids);

			/*List<Integer> conceptList = new ArrayList<Integer>();
			for(int i = 0; i < Math.min(cCount, 1000); i++) {
			conceptList.add(ids[i]);
			}*/
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
				List<Integer> mappedConcepts;
				int cntr = ccount;
				int cntrSkipped = 0;
				int cCntr = 0;
				//List<Integer> conceptsMapped = mapConcepts(conceptList);
				for (int i = ids.length - 1; i > Math.max(0, ids.length - 1 - cntr); i--) {
					mappedConcepts = mapConcept(ids[i]);

					// if the mapping exists, write
					if (mappedConcepts != null && mappedConcepts.size() > 0) {
						for(Integer mappedConcept : mappedConcepts) {
							tdos.writeInt(mappedConcept);
							tdos.writeFloat((float) vals[i]);
							cCntr++;
						}
					} else {
						// if not, skip
						cntr++;
						cntrSkipped++;
					}
				}

				System.out.println(">>> # of CL concepts " + cCntr + " (" + cntrSkipped + " skipped)");

				// if there was not enough CL concepts, fill the rest of the vector with nulls
				while (cCntr < ccount) {
					tdos.writeInt(0);
					tdos.writeFloat((float) 0.0);
					cCntr++;
				}
			} catch (IOException ex) {
				Logger.getLogger(CLLinker.class.getName()).log(Level.SEVERE, null, ex);
			}
			pr.finish();

			// print the search vector

			pr = new ProgressReporter(">>> search the esa index");
			pr.start();

			// search for the similar documents in the esa vector index
			//psClEsa.setBytes(1, baos.toByteArray());
			//psClEsa.setInt(2, N);
			ResultSet resDocs;
			ESAIndexSearcher eis = new ESAIndexSearcher(esaIndexDb, esaIndexPath, "index_all2", "index_index_all2");
			eis.setCacheDb(esaIndexCacheDb);
			//resDocs = eis.preSelectedSearch(baos.toByteArray(), "tuesdayxx" + pageId, 1000);
			_<Integer> similarityRank = new _<Integer>(-1);
			List<Integer> mappedPageId = conceptMapping.get(pageId);
			resDocs = eis.cSearch(pageId, mappedPageId, similarityRank, baos.toByteArray(), srcLang + "2" + dstLang + "_tuesday_" + pageId, 1000);


			String similarityRankTableName = "sr_" + getResTableName();
			destDb.executeUpdate("CREATE TABLE IF NOT EXISTS " + similarityRankTableName + " (doc_id INTEGER, rank INTEGER)");
			destDb.executeUpdate("INSERT INTO " + similarityRankTableName + " SET doc_id = " + pageId + ", rank = " + similarityRank.g());


			//resDocs = eis.dumbSearch(baos.toByteArray(), srcLang + "2" + dstLang + "_tuesday_" + pageId, 1000);

			//ResultSet resDocs = psClEsa.executeQuery();
			pr.finish();

			// clean-up
			try {
				baos.close();
				tdos.close();
			} catch (IOException ex) {
				Logger.getLogger(CLLinker.class.getName()).log(Level.SEVERE, null, ex);
			}

			LinkSet resLinks = new LinkSet();

			pr = new ProgressReporter(">>> retrieve the links");
			pr.start();

			// retrieve the similar documents, and set the result links
			if (linkExtractionMethod == CLLinker.ESA_VECTOR_DIMENSIONS) {
				extractLinksFromDimensions(resDocs, resLinks);
			} else if (linkExtractionMethod == CLLinker.SIMILAR_DOCUMENTS) {
				extractLinksSimilar(resDocs, resLinks);
			} else if (linkExtractionMethod == CLLinker.ESA_VECTOR_SUM) {
				extractLinksFromDimensions(resDocs, resLinks);
			} else if (linkExtractionMethod == CLLinker.SIMILAR_SIMILAR) {
				extractLinks2Similar(pageId, resDocs, resLinks);
			} else if (linkExtractionMethod == CLLinker.USE_WIKIPEDIA_LINKS) {
				extractLinksWikipediaLinks(resDocs, resLinks);
			}
			pr.finish();

			resDocs.close();

			return resLinks;
		} catch (IOException ex) {
			Logger.getLogger(CLLinker.class.getName()).log(Level.SEVERE, null, ex);
		} catch (SQLException ex) {
			Logger.getLogger(CLLinker.class.getName()).log(Level.SEVERE, null, ex);
		}
		return null;
	}

	void extractLinksSimilar(ResultSet resDocs, LinkSet resLinks) throws SQLException, IOException {
		int cntr = 0;
		while (resDocs.next() && cntr < N) {
			int docId = resDocs.getInt("doc_id");
			float simil = resDocs.getFloat("simil");
			// InputStream vector = resDocs.getBinaryStream("vector");

			// if there is some similarity
			//if (simil > 0) {
				resLinks.add(new Link(new Long(docId), (double) simil));
				cntr++;

			//}
		}

	}

	void extractLinksFromDimensions(ResultSet resDocs, LinkSet resLinks) throws SQLException, IOException {
		List<IConceptVector> vectors = new ArrayList<IConceptVector>();
		int maxVecSize = 0;

		while (resDocs.next() && vectors.size() < N) {
			int docId = resDocs.getInt("doc_id");
			float simil = resDocs.getFloat("simil");
			InputStream vector = resDocs.getBinaryStream("vector");
			if(vector == null)
				continue;

			// if there is some similarity
			if (simil > 0) {
				if (!splitBeforeDstEsa) {
					//Set<CLWESASearcher.Topic> esaConcepts;
					IConceptVector esaConcepts;
					// get the top concepts of this document from its esa vector
					try {
						esaConcepts = ESAAnalyzer.getVector(vector);
						maxVecSize = Math.max(maxVecSize, esaConcepts.count());
						vectors.add(esaConcepts);
					} catch (IOException ex) {
						Logger.getLogger(CLLinker.class.getName()).log(Level.SEVERE, null, ex);
					}
				} else {
					throw new NotImplemented();
					/*
					// get content of the article and split it
					String dstContent;
					String[] dstPars = new String[] {};

					PreparedStatement psArticle = conceptDb.getConnection().prepareStatement("SELECT old_text AS content FROM page p LEFT JOIN revision r ON rev_page = page_id LEFT JOIN text t ON rev_text_id = old_id WHERE page_id = ? ");
					psArticle.setInt(1, docId);
					ResultSet articleRes = psArticle.executeQuery();
					if(articleRes.next()) {
					dstContent = DB.readBinaryStream(articleRes.getBinaryStream("content"));
					dstPars = splitText(dstContent);
					} else {
					System.err.println("ERROR: the article from the esa index was not found! (" + docId + ")");
					}

					// add the concepts of the paragraphs into the links set
					for(String p : dstPars) {
					IConceptVector dstCV = searcher.getConceptVector(p);
					if(dstCV == null)
					continue;

					int linkCntr = 0;
					IConceptIterator it = dstCV.iterator();
					while(it.next() && linkCntr < nDimensions) {
					resLinks.add(new Link(it.getId(), it.getValue()));
					linkCntr++;
					}
					}
					 *
					 */
				}
			}
		}

		IConceptVector cv = new TroveConceptVector(maxVecSize);
		for (IConceptVector icv : vectors) {
			cv.add(icv);
		}

		IConceptIterator i = cv.orderedIterator();
		int cntr = 0;
		while (i.next() && cntr < nDimensions) {
			resLinks.add(new Link(new Long(i.getId()), i.getValue()));
			cntr++;
		}



	}

	private List<Integer> mapConcept(int i) {
		List<Integer> res = conceptMapping.get(i);
		return res;

		/*
		if (res != null) {
			return res;
		} else {
			return -1;
		}
		try {
		PreparedStatement srcConceptPs = db.getConnection().prepareStatement("SELECT concept_id FROM concept_mapping WHERE page_id = ?");
		srcConceptPs.setInt(1, i);
		ResultSet res = srcConceptPs.executeQuery();
		if (res.next()) {
		return res.getInt("concept_id");
		}
		} catch (SQLException ex) {
		Logger.getLogger(CLLinker.class.getName()).log(Level.SEVERE, null, ex);
		}
		return -1;*/
	}

	private List<Integer> mapConcepts(List<Integer> concepts) {
		List<Integer> result = new ArrayList<Integer>();
		try {
			// fill the concepts into a temporary table in the db
			PreparedStatement insConcept = db.getConnection().prepareStatement("INSERT INTO _mapconcepts SET concept_id = ?");
			db.executeUpdate("CREATE TEMPORARY TABLE _mapconcepts (concept_id INTEGER NOT NULL)");
			for (Integer c : concepts) {
				insConcept.setInt(1, c);
				insConcept.addBatch();
			}
			insConcept.executeBatch();

			PreparedStatement srcConceptPs = db.getConnection().prepareStatement("SELECT cm.concept_id FROM _mapconcepts _m LEFT JOIN concept_mapping cm ON page_id = _m.concept_id");

			ResultSet res = srcConceptPs.executeQuery();
			while (res.next()) {
				result.add(res.getInt("concept_id"));
			}
			return result;
		} catch (SQLException ex) {
			Logger.getLogger(CLLinker.class.getName()).log(Level.SEVERE, null, ex);
			return null;
		}
	}

	protected String[] splitText(String text) {
		MediaWikiParserFactory pf = new MediaWikiParserFactory();
		MediaWikiParser parser = pf.createParser();
		ParsedPage pp = parser.parse(text);
		List<Paragraph> pars = pp.getParagraphs();
		String[] res = new String[pars.size()];
		int resCntr = 0;
		for (Paragraph p : pars) {
			res[resCntr++] = p.getText();
		}
		return res;
	}

	@Override
	protected String getResTableName() {
		String res = "";
		res += srcLang + "2" + dstLang + "_";
		if (linkExtractionMethod == ESA_VECTOR_DIMENSIONS) {
			res += "vector_";
			res += N;
			res += "docs_";
			res += nDimensions;
			res += "dims_";

		} else if (linkExtractionMethod == SIMILAR_DOCUMENTS) {
			res += "simdocs_";
			res += N + "docs_";
		} else if (linkExtractionMethod == ESA_VECTOR_SUM) {
			res += "vsum_";
			res += N;
			res += "docs_";
			res += nDimensions;
			res += "dims_";
		} else if (linkExtractionMethod == SIMILAR_SIMILAR) {
			res += "2simdocs_";
			res += "first" + N + "docs_";
			res += N2 + "docs_";
		} else if (linkExtractionMethod == USE_WIKIPEDIA_LINKS) {
			res += "wikilinks_";
			res += "first" + N + "docs_";
		} else {
			res += "xxx_";
		}
		if (splitBeforeSrcEsa) {
			res += "bsplit_";
		} else {
			res += "bwhole_";
		}
		if (splitBeforeDstEsa) {
			res += "esplit";
		} else {
			res += "ewhole";
		}
		res += resTable;
		return res;
	}

	public void setN2(int i) {
		this.N2 = i;
	}

	private void extractLinks2Similar(int pageId, ResultSet resDocs, LinkSet resLinks) throws SQLException {
		// list of vectors for top N documents
		List<IConceptVector> vectors = new ArrayList<IConceptVector>();
		int cntr = 0;
		int maxVecSize = 0;
		while (resDocs.next() && cntr < N2) {
			try {
				// get the vector from database
				int docId = resDocs.getInt("doc_id");
				float simil = resDocs.getFloat("simil");
				InputStream vector = resDocs.getBinaryStream("vector");

				// save it for suming
				IConceptVector v = ESAAnalyzer.getVector(vector);
				vectors.add(v);
				maxVecSize = Math.max(v.size(), maxVecSize);
				cntr++;
			} catch (IOException ex) {
				Logger.getLogger(CLLinker.class.getName()).log(Level.SEVERE, null, ex);
			}
		}

		// join all vectors into one
		IConceptVector cv = new TroveConceptVector(maxVecSize);
		for (IConceptVector icv : vectors) {
			cv.add(icv);
		}

		// select top N dimensions
		IConceptVector searchVector = new TroveConceptVector(nDimensions * 7 / 10);
		IConceptIterator i = cv.orderedIterator();
		cntr = 0;
		while(i.next() && cntr < nDimensions) {
			searchVector.add(i.getId(), i.getValue());
			cntr++;
		}

		// search for documents similar to this
		byte[] searchVectorBytes = ESAAnalyzer.buildVector(searchVector, 150);
		ESAIndexSearcher eis = new ESAIndexSearcher(esaIndexDb, esaIndexPath, "index_all2", "index_index_all2");
		eis.setCacheDb(esaIndexCacheDb);
		//resDocs = eis.preSelectedSearch(baos.toByteArray(), "tuesdayxx" + pageId, 1000);
		//resDocs = eis.dumbSearch(searchVectorBytes, srcLang + "2" + dstLang + "_tuesday_" + pageId + "_simil2", 1000);
		_<Integer> similarityRank = new _<Integer>(-1);
		List<Integer> mappedPageId = conceptMapping.get(pageId);
		resDocs = eis.cSearch(-1, mappedPageId, similarityRank, searchVectorBytes, srcLang + "2" + dstLang + "_tuesday_" + pageId + "_simil2", 1000);

		// save it
		try {
			extractLinksSimilar(resDocs, resLinks);
		} catch (IOException ex) {
			Logger.getLogger(CLLinker.class.getName()).log(Level.SEVERE, null, ex);
		}

	}

	private void extractLinksWikipediaLinks(ResultSet resDocs, LinkSet resLinks) throws SQLException, IOException {
		int cntr = 0;
		WikipediaTitleResolver titleResolver = new WikipediaTitleResolver(conceptDb);
		Long linkDest;
		while (resDocs.next() && cntr < N) {
			int docId = resDocs.getInt("doc_id");
			float simil = resDocs.getFloat("simil");
			InputStream vector = resDocs.getBinaryStream("vector");
			IConceptVector simArticleVect;
			try {
				simArticleVect = ESAAnalyzer.getVector(vector);
			} catch(Exception e) {
				System.err.print(e);
				continue;
			}

			// get content
			PreparedStatement psArticle = conceptDb.getConnection().prepareStatement("SELECT old_text AS content FROM page p LEFT JOIN revision r ON rev_page = page_id LEFT JOIN text t ON rev_text_id = old_id WHERE page_id = ? ");
			psArticle.setInt(1, docId);
			ResultSet resArticle = psArticle.executeQuery();

			if (resArticle.next()) {
				String content = DB.readBinaryStream(resArticle.getBinaryStream("content"));

				MediaWikiParserFactory pf = new MediaWikiParserFactory();
				MediaWikiParser parser = pf.createParser();
				ParsedPage pp = parser.parse(content);


				for (Section section : pp.getSections()) {
					for (de.tudarmstadt.ukp.wikipedia.parser.Link link : section.getLinks(de.tudarmstadt.ukp.wikipedia.parser.Link.type.INTERNAL)) {
						linkDest = titleResolver.getConceptIdFromTitle(link.getTarget());
						IConceptVector lVect = null;
						double score = -1.0;

						if(linkDest != -1) {
							psEsaVector.setLong(1, linkDest);
							ResultSet vres = psEsaVector.executeQuery();
							if(vres.next()) {
								lVect = ESAAnalyzer.getVector(vres.getBinaryStream("vector"));
							} else {
								lVect = searcher.getConceptVector(link.getText());

								PreparedStatement psRedirect = conceptDb.getConnection().prepareStatement("SELECT * FROM redirect r WHERE rd_from = ?");
								psRedirect.setLong(1, linkDest);
								vres = psRedirect.executeQuery();
								if(vres.next()) {
									linkDest = titleResolver.getConceptIdFromTitle(vres.getString("rd_title"));
									psEsaVector.setLong(1, linkDest);
									vres = psEsaVector.executeQuery();
									if(vres.next())
										lVect = ESAAnalyzer.getVector(vres.getBinaryStream("vector"));
									else
										System.err.println("Couldn't use the pre-computed ESA vector for article #" + linkDest);
								} else {
									System.err.println("Couldn't use the pre-computed ESA vector for article #" + linkDest);
								}
							}
							if(lVect != null && simArticleVect != null) {
								ConceptVectorSimilarity c = new ConceptVectorSimilarity(new CosineScorer());
								score = c.calcSimilarity(lVect, simArticleVect);
							}
							if (Double.isInfinite(score) || Double.isNaN(score)) {
								score = 0.0;
							}

							resLinks.add(new Link(linkDest, score));
						}
					}
				}
			}
			resArticle.close();

			// add the similar page alone
			resLinks.add(new Link(new Long(docId), (double) simil));

			cntr++;
		}
	}
}

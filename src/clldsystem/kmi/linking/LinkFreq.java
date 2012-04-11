package clldsystem.kmi.linking;

import au.com.bytecode.opencsv.CSVReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

/**
 * Provides information about occurrence of the given text in the rest of 
 * Wikipedia as a link. Based on D.Milne's wikipedia-miner data.
 * @author zilka
 */
public class LinkFreq {

	String indexPath;
	IndexSearcher searcher;

	public void setIndexPath(String indexPath) {
		this.indexPath = indexPath;
	}

	public void buildIndex(String linkBg) throws IOException {
		Directory fsdir = FSDirectory.open(new File(indexPath));
		IndexWriter writer = new IndexWriter(fsdir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.UNLIMITED);

		//BufferedReader br = new BufferedReader(new FileReader(linkBg));
		String[] ln;
		CSVReader cr = new CSVReader(new FileReader(linkBg));
		while ((ln = cr.readNext()) != null) {
			org.apache.lucene.document.Document ldoc = new org.apache.lucene.document.Document();

			ldoc.add(new Field("link", ln[0], Field.Store.NO, Field.Index.ANALYZED));
			ldoc.add(new Field("freq", ln[1], Field.Store.YES, Field.Index.ANALYZED));

			writer.addDocument(ldoc);
		}
		writer.commit();
		writer.optimize();
		writer.close();

	}

	public void init() throws CorruptIndexException, IOException {
		Directory fsdir = FSDirectory.open(new File(indexPath));
		searcher = new IndexSearcher(fsdir);
	}

	public Integer getFreq(String queryString) throws IOException {
		if(queryString.length() == 0)
			return 0;
		String escapedQueryString = QueryParser.escape(queryString);
		QueryParser queryParser = new QueryParser(Version.LUCENE_30, "link", new SimpleAnalyzer());
		//Query query = new PrefixQuery(new Term("text", escapedQueryString));
		Query query;
		try {
			query = queryParser.parse(escapedQueryString);
		} catch (ParseException ex) {
			Logger.getLogger(LinkFreq.class.getName()).log(Level.SEVERE, null, ex);
			return null;
		}

		TopScoreDocCollector collector = TopScoreDocCollector.create(1, true);
		searcher.search(query, collector);
		ScoreDoc[] hits = collector.topDocs().scoreDocs;
		for (ScoreDoc h : hits) {
			org.apache.lucene.document.Document doc = searcher.getIndexReader().document(h.doc);
			Field f = doc.getField("freq");
			return Integer.parseInt(f.stringValue());
		}

		return 0;
	}

	public static void main(String[] args) throws IOException {
		LinkFreq lp = new LinkFreq();
		lp.setIndexPath("/tmp/xndx/");
		//lp.init();
		//System.out.println(lp.getFreq("0 6 0st broad gaug locomot"));
		//System.out.println(lp.getFreq("class"));
		//System.out.println(lp.getFreq("zz top"));
		//System.out.println(lp.getFreq("sex chromosom system"));
		//System.out.println(lp.getFreq(""));
		//System.out.println(lp.getFreq(""));
		lp.buildIndex("/xdisk/devel/kmi/clld/data/asum.txt");


	}
}

package clldsystem.esa;

import common.db.DB;
import common.db.DBConfig;
import common.Utils;
import common.config.AppConfig;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import clldsystem.data.LUCENEWikipediaAnalyzer;
import org.apache.commons.io.IOUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.Version;

/**
 * Indexes articles from database, so that they can be used as concepts in ESA. 
 * This is the first step in building an ESA background.
 * @author zilka
 */
public class ArticleFileIndexer {

	private final IndexWriter writer;

	/**
	 * Initialize ArticleIndexer for building Lucene index out of a set of
	 * articles, that is afterwards further used by other tools for 
	 * building the ESA background.
	 * @param indexDir where should the index be created
	 * @param stopWordsFile path to stop-words file
	 * @param stemmer name of the stemmer class
	 * @param lang language of the articles
	 * @throws CorruptIndexException
	 * @throws LockObtainFailedException
	 * @throws IOException 
	 */
	public ArticleFileIndexer(String indexDir, String stopWordsFile, String stemmer, String lang) throws CorruptIndexException, LockObtainFailedException, IOException {
		// initialize LUCENE index
		Directory fsDir = FSDirectory.open(new File(indexDir));

		// tell LUCENE to use our analyzer to analyze the articles
		LUCENEWikipediaAnalyzer lwa = new LUCENEWikipediaAnalyzer(stopWordsFile, stemmer);
		lwa.setLang(lang);
		IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_30, lwa);

		writer = new IndexWriter(fsDir, iwc);

		// tell LUCENE to use our similarity measure (tfidf)
		iwc.setSimilarity(new clldsystem.esa.ESASimilarity());
	}

	/**
	 * Adds an article to the index.
	 * @param id id of the article
	 * @param title title of the article
	 * @param content content of the article
	 * @return 
	 */
	public void addArticle(String id, String title, String content) {
		content = Utils.removeDiacriticalMarks(content);

		Document doc = new Document();
		doc.add(new Field("contents", content, Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.WITH_OFFSETS));
		doc.add(new Field("id", String.valueOf(id), Field.Store.YES, Field.Index.NOT_ANALYZED));
		doc.add(new Field("title", title, Field.Store.YES, Field.Index.NOT_ANALYZED));

		try {
			writer.addDocument(doc);
		} catch (CorruptIndexException ex) {
			Logger.getLogger(ArticleFileIndexer.class.getName()).log(Level.SEVERE, null, ex);
		} catch (IOException ex) {
			Logger.getLogger(ArticleFileIndexer.class.getName()).log(Level.SEVERE, null, ex);
		}
	}

	/**
	 * Finalizes the index writing.
	 * @return 
	 */
	public boolean finish() {
		try {
			writer.commit();
			writer.close();
			return true;
		} catch (CorruptIndexException ex) {
			Logger.getLogger(ArticleFileIndexer.class.getName()).log(Level.SEVERE, null, ex);
			return false;
		} catch (IOException ex) {
			Logger.getLogger(ArticleFileIndexer.class.getName()).log(Level.SEVERE, null, ex);
			return false;
		}
	}

	/**
	 * CLI interface.
	 * @param args id of article from which the indexing should start
	 * @throws CorruptIndexException
	 * @throws LockObtainFailedException
	 * @throws IOException
	 * @throws SQLException
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws CorruptIndexException, LockObtainFailedException, IOException, SQLException, ClassNotFoundException {
		// create the indexer
		ArticleFileIndexer ai = new ArticleFileIndexer(
			AppConfig.getInstance().getString("ArticleIndexer.indexPath"), 
			AppConfig.getInstance().getString("ArticleIndexer.stopwordsFile"), 
			AppConfig.getInstance().getString("ArticleIndexer.stemmerClass"), 
			AppConfig.getInstance().getString("ArticleIndexer.lang")
		);
/*
		// load the article database configuration
		DBConfig dbc = new DBConfig();
		dbc.setConnectionFromDrupalUrl(AppConfig.getInstance().getString("ArticleIndexer.db"));
		DB db = new DB(dbc);

		// load the query for iterating over the articles in the database
		String articleIter = AppConfig.getInstance().getString("ArticleIndexer.query.articleIter");
		PreparedStatement ps = db.getConnection().prepareStatement(articleIter);
		if (args.length > 0) {
			ps.setInt(1, Integer.parseInt(args[0]));
		} else {
			ps.setInt(1, 0);
		}*/

		// initialize some aux. variables
		long time = System.currentTimeMillis();
		long start = time;
		int cntr = 0;
		int cntrX = 0;

		// start iterating over the articles, insert each article into database
		File p = new File("/home/zilka/devel/rr/keyword_crawl/kwesa_train/");
		for(File t : p.listFiles()) {
			if(t.getName().endsWith(".kwout")) {
				StringWriter writer = new StringWriter();
				InputStream contentStream = new FileInputStream(t);
				IOUtils.copy(contentStream, writer);
				String content = writer.toString();
				
				String articleId = t.getName().replace(".kwout", "");
				
				ai.addArticle(articleId, t.getName(), content);
				cntr += 1;
				System.out.println(cntr);
			}
			
		}
		/*return;
		ResultSet rs = ps.executeQuery();
		while (true) {
			int lastId = -1;
			while (rs.next()) {
				try {
					StringWriter writer = new StringWriter();
					InputStream contentStream = rs.getBinaryStream("content");
					IOUtils.copy(contentStream, writer, "UTF8");
					String content = writer.toString();

					ai.addArticle(rs.getString("page_id"), rs.getString("page_title"), content);
					lastId = rs.getInt("page_id");
					cntrX += 1;
					if (System.currentTimeMillis() - time
						> 1000) {
						cntr += cntrX;
						System.out.println("Processed: "
							+ cntrX
							+ "/1 second [total: "
							+ cntr + "]");
						time = System.currentTimeMillis();
						cntrX = 0;
					}
				} catch (Exception e) {
					System.out.println("Exception: " + e);
				}
			}
			if (lastId != -1) {
				ps.setInt(1, lastId);
				rs = ps.executeQuery();
			} else {
				break;
			}
		}
		System.out.println("SUCCESS: processed " + cntr + " pages in "
			+ (System.currentTimeMillis() - start) / 1000
			+ " seconds");
			*/
		ai.finish();
	}
}

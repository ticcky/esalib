package clldsystem.data;

import de.tudarmstadt.ukp.wikipedia.parser.ParsedPage;
import de.tudarmstadt.ukp.wikipedia.parser.mediawiki.MediaWikiParser;
import de.tudarmstadt.ukp.wikipedia.parser.mediawiki.MediaWikiParserFactory;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.io.IOUtils;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.LengthFilter;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.PorterStemFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.wikipedia.analysis.WikipediaTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.cn.smart.SentenceTokenizer;
import org.apache.lucene.analysis.cn.smart.WordTokenFilter;
import org.apache.lucene.analysis.snowball.SnowballFilter;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.util.Version;
import org.tartarus.snowball.SnowballProgram;

/**
 * LUCENE Analyzer subclass that provides tokenization and normalization capabilities
 * for MediaWiki mark-up documents, by combining MediaWiki parser, stop-word filter and stemmer.
 * @author zilka
 */
public class LUCENEWikipediaAnalyzer extends Analyzer {
	// stopword set for filtering tokens
	final Set<?> stopWordSet;
	
	// snowball stemmer class name for stemming tokens
	final String snowballStemmer;
	
	// language of text
	String lang = null;

	public LUCENEWikipediaAnalyzer(String stopWordsFile, String snowballStemmer) throws IOException {
		this.snowballStemmer = snowballStemmer;

		// read stop words
		if (stopWordsFile != null) {
			InputStream is = new FileInputStream(stopWordsFile);
			BufferedReader br = new BufferedReader(new InputStreamReader(is));
			ArrayList<String> stopWords = new ArrayList<String>(500);


			String line;

			while ((line = br.readLine()) != null) {
				line = line.trim();
				if (!line.equals("")) {
					stopWords.add(line.trim());
				}
			}

			br.close();

			final CharArraySet stopSet = new CharArraySet(Version.LUCENE_30, stopWords.size(), false);
			stopSet.addAll(stopWords);
			stopWordSet = CharArraySet.unmodifiableSet(stopSet);
		} else {
			stopWordSet = null;
		}
	}

	public void setLang(String lang) {
		this.lang = lang;
	}

	/**
	 * Produces TokenStream instance for tokenizing input text. 
	 * First, a language is determined, because a special treatment needs to be taken
	 * for Chinese. Then, the individual filters (length, stemming, stopword 
	 * removal) are hooked up and the corresponding TokenStream instance is returned.
	 * 
	 * @param fieldName
	 * @param reader
	 * @return 
	 */
	@Override
	public TokenStream tokenStream(String fieldName, Reader reader) {
		if (lang == null || !lang.equals("zh")) {
			Tokenizer tokenizer = new WikipediaTokenizer(reader);

			TokenStream stream = new StandardFilter(Version.LUCENE_30, tokenizer);
			//cstream = new LengthFilter(true, stream, 3, 100);
			stream = new LowerCaseFilter(Version.LUCENE_30, stream);
			// stopword filter
			if (stopWordSet != null) {
				stream = new StopFilter(Version.LUCENE_30, stream, stopWordSet);
			}
			//if stemmer is defined, add stemming filter
			if (snowballStemmer != null) {
				try {
					Class<SnowballProgram> stemmer = (Class<SnowballProgram>) Class.forName(snowballStemmer);
					stream = new SnowballFilter(stream, stemmer.newInstance());
				} catch (InstantiationException ex) {
					Logger.getLogger(LUCENEWikipediaAnalyzer.class.getName()).log(Level.SEVERE, null, ex);
				} catch (IllegalAccessException ex) {
					Logger.getLogger(LUCENEWikipediaAnalyzer.class.getName()).log(Level.SEVERE, null, ex);
				} catch (ClassNotFoundException ex) {
					Logger.getLogger(LUCENEWikipediaAnalyzer.class.getName()).log(Level.SEVERE, null, ex);
				}
			}
			return stream;
		} else if (lang.equals("zh")) {
			try {
				// For chinese, the input needs to be cleaned, because
				// the SentenceTokenizer does not accept token stream
				// as in case of English/other languages.
				MediaWikiParserFactory pf = new MediaWikiParserFactory();
				MediaWikiParser parser = pf.createParser();

				StringWriter sw = new StringWriter();
				IOUtils.copy(reader, sw);

				ParsedPage p = parser.parse(sw.toString());
				reader = new StringReader(p.getText());
			} catch (IOException ex) {
				Logger.getLogger(LUCENEWikipediaAnalyzer.class.getName()).log(Level.SEVERE, null, ex);
				reader = new StringReader("");
			}

			Tokenizer tokenizer = new SentenceTokenizer(reader);
			TokenStream stream = new WordTokenFilter(tokenizer);
			stream = new PorterStemFilter(stream);
			stream = new StopFilter(Version.LUCENE_30, stream, stopWordSet);

			return stream;

		} else {
			// if it gets here, something's wrong with the language selection IFs
			return null;
		}

	}
}

package kwesa;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import common.db.DB;
import common.Utils;

import clldsystem.data.LUCENEWikipediaAnalyzer;
import clldsystem.esa.ESAAnalyzer;

public class KeywordSuggest {
	public static void main(String[] args) throws Exception {
		// init esa
		ESAAnalyzer esa = new ESAAnalyzer(new DB("mysql://root:root@localhost/kwesa"), "kw");
		esa.setAnalyzer(new LUCENEWikipediaAnalyzer("/home/zilka/devel/rr/keyword_crawl/res/stopwords.en.txt", "org.tartarus.snowball.ext.EnglishStemmer"));
		
		// init keywords
		Set<String> keyWords = new HashSet<String>();
		BufferedReader br = new BufferedReader(new FileReader("/home/zilka/devel/rr/keyword_crawl/kws/keywords_all.sorted.txt"));
		String line;
		while ((line = br.readLine()) != null) {
			keyWords.add(line);
		}		
		
		int cnt = 0;
		int total = keyWords.size();
		
		FileWriter fw = new FileWriter("/tmp/out.txt");
		for(String kw : keyWords) {
			fw.write(esa.getRelatedness("information retrieval", kw) + "; " + kw + "\n");
			cnt += 1;
			System.out.println(cnt / (float)total);
		}
		fw.close();

	}
}

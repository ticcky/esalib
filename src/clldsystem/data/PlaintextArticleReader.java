package clldsystem.data;

import java.util.Scanner;

import clldsystem.model.*;

/**
 * Reads articles stored in plain text file.
 * @author zilka
 */
public class PlaintextArticleReader extends ArticleReader {

	/**
	 * Reads articles stored in plain text file. 
	 * Expected format is: title on the first line, the rest of the document
	 * follows.
	 * @param text
	 * @return 
	 */
	@Override
	public Article read(String text) {
		Scanner scanner = new Scanner(text);
		String NL = System.getProperty("line.separator");
		String title = scanner.nextLine();
		String atext = "";

		try {
			while (scanner.hasNextLine()) {
				atext = atext + scanner.nextLine() + NL;
			}
		} finally {
			scanner.close();
		}

		Article a = new Article();
		a.setTitle(title);
		a.setText(atext);
		return a;
	}
}

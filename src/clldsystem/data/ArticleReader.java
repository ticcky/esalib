package clldsystem.data;

import clldsystem.model.Article;

/** 
 * Builds an Article instance out of plain text.
 * @author zilka
 */
public abstract class ArticleReader {
	/**
	 * Produces an Article instance for the text.
	 * @param text
	 * @return
	 */
	public abstract Article read(String text);
}

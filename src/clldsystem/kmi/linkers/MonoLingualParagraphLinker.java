package clldsystem.kmi.linkers;

import common.db.DB;
import clldsystem.esa.IConceptIterator;
import clldsystem.esa.IConceptVector;
import java.io.IOException;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import clldsystem.model.Link;
import clldsystem.model.LinkSet;

/**
 * Links using the mono-lingual ESA vector of the page. Pretty stupid :)
 * @author zilka
 */
public class MonoLingualParagraphLinker extends Linker {
	int N = 10;
	public MonoLingualParagraphLinker(DB db, DB conceptDb, DB esaDb, DB destDb, String resTable, String articleIter, String srcLang, String srcLangStopWordsFile, String srcLangStemmer, String dstLang) {
		super(db, conceptDb, esaDb, destDb, resTable, articleIter, srcLang, srcLangStopWordsFile, srcLangStemmer, dstLang);
	}

	public void setN(int N) {
		this.N = N;
	}

	public int getN() {
		return N;
	}

	@Override
	LinkSet linkText(String pageTitle, String content, int pageId) {
		try {
			LinkSet res = new LinkSet();
			String[] pars = splitText(content);
			for (String s : pars) {
				IConceptVector v = searcher.getConceptVector(s);
				IConceptIterator i = v.iterator();

				int cntr = 0;
				while (i.next() && cntr < N) {
					Link l = new Link(new Long(getConcept(i.getId())), i.getValue());
					res.add(l);
					cntr++;
				}
			}

			return res;
		} catch (IOException ex) {
			Logger.getLogger(MonoLingualParagraphLinker.class.getName()).log(Level.SEVERE, null, ex);
		} catch (SQLException ex) {
			Logger.getLogger(MonoLingualParagraphLinker.class.getName()).log(Level.SEVERE, null, ex);
		}
		return null;
	}

	protected String[] splitText(String text) {
		return text.split("\n\n");
	}
}

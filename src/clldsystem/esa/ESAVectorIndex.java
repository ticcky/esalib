package clldsystem.esa;

import common.db.DB;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Lukas Zilka (l.zilka@open.ac.uk)
 * @year 2011
 */
public class ESAVectorIndex {

	DB db;
	PreparedStatement indexQuery;
	Map<Long, IConceptVector> cache;
	boolean caching = false;

	public void setCaching(boolean caching) {
		this.caching = caching;
	}

	public ESAVectorIndex(DB db, String indexTableName) {
		this.db = db;
		try {
			indexQuery = db.getConnection().prepareStatement("SELECT * FROM "
				+ indexTableName + " WHERE doc_id = ?");
		} catch (SQLException ex) {
			Logger.getLogger(ESAVectorIndex.class.getName()).log(Level.SEVERE, null, ex);
		}		
		cache = new HashMap<Long, IConceptVector>();


	}

	public IConceptVector getVector(long articleId) {
		try {
			if (caching && cache.containsKey(articleId)) {
				return cache.get(articleId);
			}

			indexQuery.setLong(1, articleId);
			ResultSet res = indexQuery.executeQuery();
			if (res.next()) {
				IConceptVector result = ESAAnalyzer.getVector(res.getBinaryStream("vector"));
				if (caching) {
					cache.put(articleId, result);
				}
				return result;
			}
		} catch (IOException ex) {
			Logger.getLogger(ESAVectorIndex.class.getName()).log(Level.SEVERE, null, ex);
		} catch (SQLException ex) {
			Logger.getLogger(ESAVectorIndex.class.getName()).log(Level.SEVERE, null, ex);
		}
		return null;

	}
}

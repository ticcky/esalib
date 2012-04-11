package common.wiki;

import common.db.DB;
import clldsystem.kmi.linking.RedirectResolver;
import com.thoughtworks.xstream.XStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Resolves wikipedia title to it's page id.
 * @author zilka
 */
public class WikipediaTitleResolver {

	DB db;
	PreparedStatement ps;
	PreparedStatement psInit;
	RedirectResolver redirectResolver;

	Map<String, Long> cache;
	boolean caching = false;
	PreparedStatement psReverse;

	public void setCaching(boolean caching) {
		this.caching = caching;
	}

	public void setRedirectResolver(RedirectResolver redirectResolver) {
		this.redirectResolver = redirectResolver;
	}

	public WikipediaTitleResolver(DB db) throws SQLException {
		this.db = db;
		this.psInit = db.getConnection().prepareStatement(
			"SELECT * FROM page p "
			//+ "LEFT JOIN redirect_mapping rm ON rm.page_id = p.page_id "
			+ "WHERE (page_title = ? OR page_title = ?)"
			//+ " AND rm.page_id IS NULL"
			);
		this.ps = db.getConnection().prepareStatement(
			"SELECT * FROM page p "
			//+ "LEFT JOIN redirect_mapping rm ON rm.page_id = p.page_id "
			+ "WHERE (page_title = ? OR page_title = ?)"
			+ " AND page_is_redirect = 0"
			//+ " AND rm.page_id IS NULL"
			);
		this.psReverse = db.getConnection().prepareStatement(
			"SELECT page_title FROM page p "
			//+ "LEFT JOIN redirect_mapping rm ON rm.page_id = p.page_id "
			+ "WHERE (page_id = ?)"
			//+ " AND rm.page_id IS NULL"
			);
		this.cache = new HashMap<String, Long>();
	}

	public void saveCache(String fn) throws FileNotFoundException {
		XStream xstream = new XStream();
		xstream.toXML(cache, new FileOutputStream(fn));
	}
	
	public void loadCache(String fn) throws FileNotFoundException {
		XStream xstream = new XStream();
		cache = (Map<String, Long>) xstream.fromXML(new FileInputStream(fn));
	}

	public Long getConceptIdFromTitle(String target, RedirectResolver rr) throws SQLException, IOException {
		long conceptId = getConceptIdFromTitle(target, psInit);
		String redDest;
		
		if ((redDest = rr.resolve(conceptId)) != null) {
			conceptId = getConceptIdFromTitle(redDest, ps);
		}
		return conceptId;
	}
	
	public Long getConceptIdFromTitle(String target) throws SQLException, IOException {
		if(redirectResolver == null)
			return getConceptIdFromTitle(target, ps);
		else
			return getConceptIdFromTitle(target, redirectResolver);
	}

	public Long getConceptIdFromTitle(String target, PreparedStatement pp) {
		// try cache if set as caching
		if(caching && cache.containsKey(target))
			return cache.get(target);

		try {
			pp.setString(2, target);
			target = target.replace("_", " ");
			pp.setString(1, target);

			ResultSet res2 = pp.executeQuery();
			long result = -1;
			if (res2.next()) {
				result = res2.getLong("page_id");
			}
			res2.close();

			//System.out.println(target);
			if(caching)
				this.cache.put(target, result);

			return result;
		} catch (SQLException ex) {
			Logger.getLogger(WikipediaTitleResolver.class.getName()).log(Level.SEVERE, null, ex);
			this.cache.put(target, new Long(-1));
			return new Long(-1);
		}
	}

	public String getTitleFromConceptId(Long conceptId) throws SQLException {
		psReverse.setLong(1, conceptId);
		ResultSet res = psReverse.executeQuery();
		if(res.next())
			return res.getString("page_title");
		else
			return null;

	}

	public Map<String, Long> getMap() {
		return new WTRMap();
	}

	class WTRMap implements Map<String, Long>  {

		@Override
		public int size() {
			return 0;
		}

		@Override
		public boolean isEmpty() {
			throw new UnsupportedOperationException("Not supported yet.");
		}

		@Override
		public boolean containsKey(Object key) {
			throw new UnsupportedOperationException("Not supported yet.");
		}

		@Override
		public boolean containsValue(Object value) {
			throw new UnsupportedOperationException("Not supported yet.");
		}

		@Override
		public Long get(Object key) {
			try {
				return getConceptIdFromTitle((String) key);
			} catch (SQLException ex) {
				Logger.getLogger(WikipediaTitleResolver.class.getName()).log(Level.SEVERE, null, ex);
				return null;
			} catch (IOException ex) {
				Logger.getLogger(WikipediaTitleResolver.class.getName()).log(Level.SEVERE, null, ex);
				return null;
			}
		}

		@Override
		public Long put(String key, Long value) {
			throw new UnsupportedOperationException("Not supported yet.");
		}

		@Override
		public Long remove(Object key) {
			throw new UnsupportedOperationException("Not supported yet.");
		}

		@Override
		public void putAll(Map<? extends String, ? extends Long> m) {
			throw new UnsupportedOperationException("Not supported yet.");
		}

		@Override
		public void clear() {
			throw new UnsupportedOperationException("Not supported yet.");
		}

		@Override
		public Set<String> keySet() {
			throw new UnsupportedOperationException("Not supported yet.");
		}

		@Override
		public Collection<Long> values() {
			throw new UnsupportedOperationException("Not supported yet.");
		}

		@Override
		public Set<Entry<String, Long>> entrySet() {
			throw new UnsupportedOperationException("Not supported yet.");
		}

	}

}

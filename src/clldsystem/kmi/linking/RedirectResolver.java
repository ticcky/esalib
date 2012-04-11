package clldsystem.kmi.linking;

import com.thoughtworks.xstream.XStream;
import common.db.DB;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Provides resolving of redirects on Wikipedia. Basically, it's a wrapper around
 * the redirect page.
 * @author zilka
 */
public class RedirectResolver {
	DB db;
	PreparedStatement ps;

	Map<Long, String> cache;
	boolean caching = false;

	public void setCaching(boolean caching) {
		this.caching = caching;
	}

	public RedirectResolver(DB db) throws SQLException {
		this.db = db;
		ps = db.getConnection().prepareStatement("SELECT * FROM redirect WHERE rd_from = ?");
		cache = new HashMap<Long, String>();
	}
	
	public void saveCache(String fn) throws FileNotFoundException {
		XStream xstream = new XStream();
		xstream.toXML(cache, new FileOutputStream(fn));
	}
	
	public void loadCache(String fn) throws FileNotFoundException {
		XStream xstream = new XStream();
		cache = (Map<Long, String>) xstream.fromXML(new FileInputStream(fn));
	}


	public String resolve(long pageId) throws SQLException, IOException {
		if(caching && cache.containsKey(pageId))
			return cache.get(pageId);
		
		String resStr;
		
		ps.setLong(1, pageId);
		ResultSet res = ps.executeQuery();
		if(res.next())
			resStr = DB.readBinaryStream(res.getBinaryStream("rd_title"));
		else
			resStr = null;

		if(caching)
			cache.put(pageId, resStr);

		return resStr;
	}
}

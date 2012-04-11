package clldsystem.kmi.linking;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.thoughtworks.xstream.XStream;
import common.db.DB;
import common.db.DBConfig;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Maps titles of wikipedia pages to their id's and vice versa. Can export the
 * titles into a xml file.
 * @author zilka
 */
public class TitleMapper {
	BiMap<String, Integer> pageMap;

	public void loadFromDb(DB db) throws SQLException {
		pageMap = HashBiMap.create();
		PreparedStatement ps = db.getConnection().prepareStatement("SELECT * FROM page");
		ResultSet res = ps.executeQuery();
		while(res.next()) {
			pageMap.forcePut(res.getString("page_title"), res.getInt("page_id"));
		}
	}

	public void saveToFile(String fn) throws FileNotFoundException {
		XStream xstream = new XStream();
		xstream.toXML(pageMap, new FileOutputStream(fn));
	}
	
	public void loadFromFile(String fn) throws FileNotFoundException {
		XStream xstream = new XStream();
		this.pageMap = (BiMap<String, Integer>) xstream.fromXML(new FileInputStream(fn));
	}


	public int getPageId(String pageTitle) {
		return pageMap.get(pageTitle);

	}
	
	public static void main(String[] args) throws FileNotFoundException, SQLException, ClassNotFoundException {
		DBConfig dbc = new DBConfig();
		dbc.setConnectionFromDrupalUrl("mysql://root:root@lwkm012/wikidb_en");
		DB db = new DB(dbc);

		TitleMapper tm = new TitleMapper();
		tm.loadFromDb(db);
		tm.saveToFile("/tmp/tm_en.xml");
	}
}

package clldsystem.kmi.linking;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.thoughtworks.xstream.XStream;
import common.db.DB;
import common.db.DBConfig;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang.NotImplementedException;

/**
 * Provides wrapper around the concept mapping table for a wikipedia. Allowing
 * mapping from page_id to concept_id and vice versa.
 * @author zilka
 */
public class ConceptMapper {

	BiMap<Long, Long> conceptMap;
	Multimap<Long, Long> conceptMapX;

	public ConceptMapper() {
	}

	public void loadFromDb(DB db) throws SQLException {
		conceptMap = HashBiMap.create(); // new HashMap<Long, Long>();
		conceptMapX = HashMultimap.create();

		PreparedStatement ps = db.getConnection().prepareStatement("SELECT * FROM concept_mapping");
		ResultSet res = ps.executeQuery();
		while (res.next()) {
			long conceptId = res.getLong("concept_id");
			long pageId = res.getLong("page_id");
			if (!conceptMap.containsKey(conceptId)) {
				conceptMap.forcePut(conceptId, pageId);
			}
			conceptMapX.put(conceptId, pageId);
		}
	}

	public void saveToFile(String fn) throws FileNotFoundException {
		XStream xstream = new XStream();
		xstream.toXML(conceptMap, new FileOutputStream(fn));
	}

	public void loadFromFile(String fn) throws FileNotFoundException {
		XStream xstream = new XStream();
		throw new NotImplementedException();
		//this.conceptMap = (HashMap<Long, Long>) xstream.fromXML(new FileInputStream(fn));
	}

	public long getConceptId(long pageId) {

		Long res = conceptMap.inverse().get(pageId);
		if (res != null) {
			return res;
		} else {
			return -1;
		}
	}

	public long getPageId(long conceptId) {
		long res = conceptMap.get(conceptId);
		return res;

	}

	public Set<Long> getPageIds(long conceptId) {
		return new HashSet<Long>(conceptMapX.get(conceptId));

	}

	public static void main(String[] args) throws FileNotFoundException, SQLException, ClassNotFoundException {
		DBConfig dbc = new DBConfig();
		dbc.setConnectionFromDrupalUrl("mysql://root:root@lwkm012/wikidb_zh");
		DB db = new DB(dbc);

		ConceptMapper cm = new ConceptMapper();
		cm.loadFromDb(db);
		cm.saveToFile("/tmp/cm_en2zh.xml");
	}
}

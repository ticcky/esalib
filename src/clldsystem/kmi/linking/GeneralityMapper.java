package clldsystem.kmi.linking;

import common.db.DB;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Provides a wrapper around a table with generality of concepts (according to the
 * Wikipedia category tree. Based on D.Milne's Wikipedia-miner data.
 * @author zilka
 */
public class GeneralityMapper {
	DB db;
	Map<Long, Integer> map;
	PreparedStatement ps;

	public GeneralityMapper(DB db) throws SQLException {
		this.db = db;
		ps = db.getConnection().prepareStatement("SELECT * FROM generality_en WHERE page_id = ?");
		map = new HashMap<Long, Integer>();
	}

	public void load(String file) throws FileNotFoundException, IOException {
		BufferedReader br = new BufferedReader(new FileReader(file));
		String line;
		while ((line = br.readLine()) != null) {
			String[] parts = line.split(", ");
			map.put(Long.parseLong(parts[0]), Integer.parseInt(parts[1]));
		}
	}

	public int get(Long conceptId) throws SQLException {
		if (map.containsKey(conceptId)) {
			return map.get(conceptId);
		} else {
			ps.setLong(1, conceptId);
			ResultSet res = ps.executeQuery();
			if(res.next()) {
				map.put(conceptId, res.getInt("generality"));
			} else {
				map.put(conceptId, -1);
			}
			return -1;
		}
	}
}

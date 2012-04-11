package common.db;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Iterates over huge number of database items, loads the results on-the-fly, 
 * not at once.
 * @author zilka
 */
public class DBDatasetIterator {

	ResultSet res;
	int curId = 0;
	PreparedStatement ps;
	String idField = "id";

	public DBDatasetIterator(DB db, String iterQuery) throws SQLException {
		ps = db.getConnection().prepareStatement(iterQuery);
	}

	public boolean next() {
		try {
			if (res == null || !res.next()) {
				loadNext();
				if (!res.next())
					return false;
			}
			curId = res.getInt(idField);
			return true;
		} catch (SQLException e) {
			System.err.println("next: " + e);
			return false;
		}

	}

	void loadNext() throws SQLException {
		ps.setInt(1, curId);
		res = ps.executeQuery();

	}

	public ResultSet getRes() {
		return res;
	}

	public void setIdField(String idField) {
		this.idField = idField;
	}


}

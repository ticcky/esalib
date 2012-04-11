package clldsystem.esa;

import common.db.DB;
import common.db.DBConfig;
import common.config.AppConfig;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Prints ESA vector from database. For debugging purposes.
 * @author zilka
 */
public class ESAVectorPrinter {

	public static void main(String[] args) throws SQLException, IOException, ClassNotFoundException {
		// config
		DBConfig dbc = new DBConfig();
		dbc.setConnectionFromDrupalUrl(AppConfig.getInstance().getString("ESAVectorPrinter.db"));
		DB db = new DB(dbc);

		// select from database
		PreparedStatement ps = db.getConnection().prepareStatement("SELECT * FROM esa_vector_index WHERE id = ?");
		ps.setInt(1, AppConfig.getInstance().getInt("ESAVectorPrinter.id"));
		ResultSet res = ps.executeQuery();
		
		// decode and print the dimensions
		int doc;
		float score;
		if (res.next()) {
			ByteArrayInputStream bais = new ByteArrayInputStream(res.getBytes("vector"));
			DataInputStream dis = new DataInputStream(bais);
			/**
			 * 4 bytes: int - length of array
			 * 4 byte (doc) - 8 byte (tfidf) pairs
			 */
			int plen = dis.readInt();
			//System.out.println("vector len: " + plen);
			//if(plen > 100)
			//	plen = 100;
			for (int k = 0; k < plen; k++) {
				doc = dis.readInt();
				score = dis.readFloat();
				System.out.println(doc + ": " + score);
			}

			bais.close();
			dis.close();

		}
	}
}

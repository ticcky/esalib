package common.wiki;

import common.db.DB;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Wraps the wikipedia database.
 * @author zilka
 */
public class Wikipedia {
	DB db;
	
	String articleQuery = "SELECT page_id AS id, page_id, page.page_title, text.old_text AS content FROM page AS page LEFT JOIN revision ON page_id = rev_page LEFT JOIN text ON rev_text_id = old_id WHERE page_id = ?";

	PreparedStatement psArticle;

	public void setDb(DB db) throws SQLException {
		this.db = db;

		psArticle = db.getConnection().prepareStatement(articleQuery);
	}

	

	public String getContent(Long pageId) throws SQLException, IOException {
		psArticle.setLong(1, pageId);
		ResultSet res = psArticle.executeQuery();

		if(res.next())		
			return DB.readBinaryStream(res.getBinaryStream("content"));
		else
			return null;

	}
	
}

package common.db;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.io.IOUtils;

/**
 * Database wrapper class. Now works only with MySQL, but it is possible to change
 * the inside and allow working even with other databases.
 *
 * @author pk3295
 * @author zilka
 */
public class DB {

	private Connection connection;
	private Statement stmt = null;
	private DBConfig _dbConfig = null;

	public DB(String serverName, String database, String username, String password, String schema) throws SQLException, ClassNotFoundException {
		this.connection = null;
		// Load the JDBC driver
		
		String driverName = "";
		String url = "";
		if(schema.equals("mysql")) {
			driverName = "org.gjt.mm.mysql.Driver"; // MySQL MM JDBC driver
			url = "jdbc:mysql://" + serverName + "/"
				+ database
				+ "?useUnicode=true&characterEncoding=utf8&useCursorFetch=true&autoReconnect=true";			
		}
		else if(schema.equals("sqlite")) {
			driverName = "org.sqlite.JDBC";
			url = "jdbc:sqlite:" + database;
		}
		
		Class.forName(driverName);
			
		this.connection = DriverManager.getConnection(url, username, password);
		this.stmt = this.connection.createStatement();
	}

	public DB(String conn) throws SQLException, ClassNotFoundException {
		this(new DBConfig(conn));
	}

	public DB(DBConfig c) throws SQLException, ClassNotFoundException {
		this(c.serverName, c.database, c.username, c.password, c.schema);
				
	}

	public ResultSet executeSelect(String sql) {
		ResultSet rs = null;
		try {
			rs = stmt.executeQuery(sql);
			//rs = stmt.getResultSet();
		} catch (SQLException e) {
			System.out.println("Cannot execute command: " + sql);
			System.out.println(e);
		}
		return rs;
	}

	public void executeUpdate(String sql) throws SQLException {
		stmt.executeUpdate(sql);
	}

	public String escape(String source) {
		String escaped = source;
		escaped = escaped.replaceAll("'", "\'");
		escaped = escaped.replaceAll("\\", "\\\\");

		return escaped;
	}

	public Connection getConnection() {
		return connection;
	}

	public static int getInsertedId(PreparedStatement ps) {
		try {
			ResultSet keys = ps.getGeneratedKeys();
			keys.next();
			return keys.getInt(1);
		} catch (SQLException ex) {
			Logger.getLogger(DB.class.getName()).log(Level.SEVERE, null, ex);
			return -1;
		}
	}

	public static Iterator<Integer> getInsertedIdsIterator(PreparedStatement ps) throws SQLException {
		ResultSet wordKeys = ps.getGeneratedKeys();
		ArrayList<Integer> wordKeysIds = new ArrayList<Integer>();
		while (wordKeys.next()) {
			wordKeysIds.add(wordKeys.getInt(1));
		}
		return wordKeysIds.iterator();
	}

	public static String readBinaryStream(InputStream contentStream) throws IOException {
		StringWriter writer = new StringWriter();
		IOUtils.copy(contentStream, writer, "UTF8");
		contentStream.close();

		String result = writer.toString();
		writer.close();
		return result;
	}
}

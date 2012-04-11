package common.db;

/**
 * Database configuration holder.
 *
 * @author pk3295
 * @author zilka
 */
public class DBConfig {

	public String serverName;
	public String database;
	public String username;
	public String password;

	public DBConfig() {
	}

	public DBConfig(String conn) {
		setConnectionFromDrupalUrl(conn);
	}

	public void setConnectionFromDrupalUrl(String url) {
		String[] tmp = url.split(":", 3);
		String user = tmp[1].replaceAll("/", "");
		Integer atPos = tmp[2].indexOf("@");
		String pass = tmp[2].substring(0, atPos);
		String[] tmp2 = url.split("/");
		String databaseName = tmp2[tmp2.length - 1];

		serverName = tmp[2].substring(atPos + 1, tmp[2].length()
			- databaseName.length() - 1);
		database = databaseName;
		password = pass;
		username = user;

	}

	public void setDatabase(String database) {
		this.database = database;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public void setServerName(String serverName) {
		this.serverName = serverName;
	}

	public void setUsername(String username) {
		this.username = username;
	}
}

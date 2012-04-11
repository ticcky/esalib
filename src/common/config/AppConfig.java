package common.config;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;

import java.util.Collection;
import java.util.List;

/**
 * Load configuration.
 * @author zilka
 */
public class AppConfig extends XMLConfiguration {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7367056243465281640L;
	private static AppConfig instance;
	private static String configFile = "config/config.xml";
	String section = "";

	// Singleton initialiser
	static {
		instance = new AppConfig(configFile);
	}

	private String getPartId() {
		String partId = instance.getString("ConfigId:" + section);
		return partId;

	}

	public String getSString(String param) {
		String partId = getPartId();
		return instance.getString(section + ":" + partId + "." + param);
	}

	public Integer getSInt(String param) {
		String partId = getPartId();
		return instance.getInt(section + ":" + partId + "." + param);
	}

	public List<String> getSList(String param) {
		String partId = getPartId();
		return instance.getList(section + ":" + partId + "." + param);
	}

	public void setSection(String section) {
		this.section = section;
	}



	/**
	 * Constructor
	 *
	 * @param fileName Configuration file name.
	 */
	private AppConfig(String fileName) {
		init(fileName);
	}

	/**
	 * Initialize the class.
	 *
	 * @param fileName Configuration file name.
	 */
	private void init(String fileName) {
		setFileName(fileName);
		try {
			load();
		} catch (ConfigurationException configEx) {
			configEx.printStackTrace();
		}
	}

	/**
	 * Singleton access method.
	 *
	 * @return Singleton
	 */
	public static AppConfig getInstance() {
		return instance;
	}

	public static void main(String args[]) {
		AppConfig config = AppConfig.getInstance();
		System.out.println(config.getString("text(param='a').name"));
		System.exit(0);
		System.out.println(config.getString("database.password"));

		Object obj = config.getProperty("lists.list");
		if (obj instanceof Collection) {
			int size = ((Collection) obj).size();
			for (int i = 0; i < size; i++) {
				System.out.println(config.getProperty("lists.list(" + i + ")"));
			}
		} else if (obj instanceof String) {
			System.out.println(config.getProperty("lists.list"));
		}

		obj = config.getProperty("batch-job.job.name");
		if (obj instanceof Collection) {
			int size = ((Collection) obj).size();
			for (int i = 0; i < size; i++) {
				System.out.println(config.getProperty("batch-job.job(" + i + ").name"));
			}
		} else if (obj instanceof String) {
			System.out.println(config.getProperty("batch-job.job.name"));
		}
	}
}

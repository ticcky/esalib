package common;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Scanner;

/**
 * Performs certain operation recursively on files under some directory.
 * @author zilka
 */
public abstract class RecursiveFileProcessor {

	String startFile = null;

	public void process(String path) throws IOException {
		process(new File(path));
	}

	public void setStartFile(String sf) {
		startFile = sf;
	}

	public void process(File path) throws IOException {
		// go through the path recursively
		Iterator<File> files = Arrays.asList(path.listFiles()).iterator();
		while (files.hasNext()) {
			File f = files.next();
			if (f.isDirectory()) {
				process(f);
			} else {
				if (startFile != null && startFile.equals(f.getName())) {
					startFile = null;
				}

				if (startFile == null) {
					processFile(f);
				}
			}
		}
	}

	public String readFile(File f) throws FileNotFoundException {
		StringBuilder text = new StringBuilder();

		Scanner scanner = new Scanner(new FileInputStream(f), "UTF-8");
		String NL = System.getProperty("line.separator");
		try {
			while (scanner.hasNextLine()) {
				text.append(scanner.nextLine() + NL);
			}
		} finally {
			scanner.close();
		}

		return text.toString();
	}

	public abstract void processFile(File f) throws IOException;
}

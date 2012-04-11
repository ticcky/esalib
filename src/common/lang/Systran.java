package common.lang;

import java.io.*;
import java.net.*;


/**
 * Provides translating capabilities through the Systran API.
 * @author zilka
 */
public class Systran {
	String serviceUrl;

	public String translate(String text, String from, String to) {
		String data = text;
		String result = "";
		
		
		try {
			URL url;
			url = new URL(serviceUrl + "/sai?lp=" + from + "_" + to
					+ "&service=translate");

			URLConnection conn = url.openConnection();
			conn.setDoOutput(true);
			OutputStreamWriter wr = new OutputStreamWriter(
					conn.getOutputStream());
			wr.write(data);

			wr.flush();

			BufferedReader rd = new BufferedReader(new InputStreamReader(
					conn.getInputStream()));
			String line;
			boolean head = false;
			
			while ((line = rd.readLine()) != null) {
				if (!head) {
					if (line.equals("body=")) {
						head = true;
					} else {
						System.out.println(line);
					}
				} else {
					result = result + line + "\n";
				}
			}			
			wr.close();
			rd.close();			
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return result;
	}

	public static void main(String[] args) throws IOException {
		Systran s = new Systran();
		System.out.println(s.translate("Hello, how are you?", "en",
				"zh"));

	}
}

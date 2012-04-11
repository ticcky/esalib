package common;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

/*
 * @author Eric Tang
 * 
 */
public class XmlToTxt {

	private static XmlToTxt instance = null;
	private static final String TAG_START_OPEN = "<";
	private static final String TAG_END_OPEN = "</";
	private static final char TAG_CLOSE = '>';

	public static byte parSeparator = 0x1C;
	public static byte secSeparator = 0x1D;

	public XmlToTxt() {
	}

	public static XmlToTxt getInstance() {
		if (instance == null) {
			instance = new XmlToTxt();
		}
		return instance;
	}

	public static int textLength(byte[] bytes, int byteOffset, int length) {
		byte[] newBytes = new byte[length];
		System.arraycopy(bytes, byteOffset, newBytes, 0, length);
		String source = null;
		try {
			source = new String(newBytes, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return source.length();
	}

	public static int byteOffsetToTextOffset(byte[] bytes, int byteOffset) {
		return textLength(bytes, 0, byteOffset);
	}

	public String replaceNonAlphabet(String source, String with) {
		source.replaceAll("[\\W]", with);
		return source;
	}

	public String cleanTag(String content/*, String tagName*/) {
		int pos = 0;
		int pre_pos = 0;
		StringBuffer sb = new StringBuffer();
		pos = content.indexOf(TAG_START_OPEN, pre_pos);
		if (pos > -1) {
			sb.append(content.substring(pre_pos, pos));
			while (pos > -1) {
				while (content.charAt(pos) != TAG_CLOSE) {
					++pos;
				}
				++pos;

				pre_pos = pos;

				pos = content.indexOf(TAG_START_OPEN, pre_pos);
				if (pos != -1) {
					sb.append(content.substring(pre_pos, pos));
				}
			}
			if (pos < 0 && pre_pos >= 0) {
				sb.append(content.substring(pre_pos));
			}
			return sb.toString();
		}
		return content;
	}

	/*
	 * This snippet code is written by Andrew Trotman initially
	 */
	public byte[] clean(byte[] file) {
		//byte[] ch; //, *from, *to;
		/*
		remove XML tags and remove all non-alnum (but keep case)
		 */
		//ch = file;
		//String tmp = "";
		int count = 0;
		while (count < file.length) {
			byte ch = file[count];
			if (ch == '<') // then remove the XML tags
			{
				while (file[count] != '>') {
					//tmp += (char) file[count];
					file[count++] = ' ';
				}
				// fuckin' ugly hack to recognize the paragraphs
				/*if (tmp.equals("<p")) {
					file[count - 2] = ' ';
					file[count - 1] = XmlToTxt.parSeparator;
				} else if (tmp.equals("<sec")) {
					file[count - 2] = ' ';
					file[count - 1] = XmlToTxt.secSeparator;
				}

				tmp = "";*/

				file[count] = ' '; // replace >
			}
//			else if (!isalnum(ch))	// then remove it
//				ch++ = ' ';
//			else
//				{
//				if (lower_case_only)
//					{
////					ch = (char)tolower(ch);
//					ch++;
//					}
//				else
//					ch++;
//				}
			++count;
		}


		/*
		now remove multiple, head, and tail spaces.
		 */
//		int offset = 0;
//		int length = file.length;
//		while (Character.isWhitespace(file[offset]) && offset < length)
//			++offset;
//		while (Character.isWhitespace(file[length - 1]) && length > 0)
//			--length;
//		length -= offset;
//		byte[] result = new byte[length];
//		System.arraycopy(file, offset, result, 0, length);
//		return result;
		return file;
	}

	private byte[] read(String xmlfile) throws IOException {
		int size;
		byte[] bytes = null;
		FileInputStream fis = new FileInputStream(xmlfile);
		size = fis.available();
		bytes = new byte[size];
		fis.read(bytes, 0, size);
		return bytes;
	}

	public byte[] convertFile(String xmlfile) throws IOException {


//	    for (int i = 0; i < size;)
//	        theChars[i] = (char)(bytes[i++]&0xff);
		return clean(read(xmlfile));
	}

	public byte[] convert(String xml) {
		return clean(xml.getBytes());
	}

	public String getText(String xmlfile) {
		String text = null;
		try {
			text = new String(convert(xmlfile), "ASCII");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return text;
	}

	public String getXmlFileText(String xmlfile) throws UnsupportedEncodingException, IOException {
		String text = null;
		text = new String(convertFile(xmlfile), "UTF-8");
		return text;
	}

	static void usage() {
		System.out.println("Usage: ");
		System.out.println("	XML2TXT [-o:offset:length] input_xml");
		System.out.println("		return the text with given offset and length.");
		System.out.println("Or ");
		System.out.println("	XML2TXT [-O:offset:length] input_xml");
		System.out.println("		return the text with given character offset and length.");
		System.out.println("Or ");
		System.out.println("	XML2TXT input_xml");
		System.out.println("		remove all the tags");
		//System.out.println("			[-r] -r replace the non-alphabet characters");
		System.exit(-1);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws IOException {
		args = new String[1];
		args[0] = "/tmp/aaa.xml";
		if (args.length < 1) {
			usage();
		}
		int offset = 0, length = -1;
		XmlToTxt converter = new XmlToTxt();
		String xmlfile = null;

		byte[] bytes = null;

		try {
			if (args[0].charAt(0) == '-') {
				if (args.length < 2 || args[0].length() != 2 || args[0].charAt(1)
					!= 'o' || args[0].charAt(1) != 'O') {
					usage();
				}
				int pos = 0, pos2 = 0;
				if ((pos = args[0].indexOf(":", pos)) != -1) {
					++pos;
					if ((pos2 = args[0].indexOf(":", pos))
						!= -1) {
						offset = Integer.valueOf(args[0].substring(pos, pos2)).intValue();
						length = Integer.valueOf(args[0].substring(pos2
							+ 1)).intValue();
					} else {
						offset = Integer.valueOf(args[0].substring(pos
							+ 1)).intValue();
					}
				}
				System.err.printf("Showing offset: %d with length %d\n", (Object[]) new Integer[]{new Integer(offset), new Integer(length)});
				xmlfile = args[1];

				bytes = converter.read(xmlfile);
				String text = null;
				if (args[0].charAt(1) == 'o') {
					if (length == -1) {
						length = bytes.length;
					}

					byte[] result = new byte[length];
					System.arraycopy(bytes, offset, result, 0, length);
					text = new String(result, "UTF-8");
				} else {
					String fulltext = new String(bytes, "UTF-8");
					if (length == -1) {
						text = fulltext.substring(offset);
					} else {
						text = fulltext.substring(offset, offset
							+ length);
					}
				}
				System.out.println("Text:\"" + text + "\"");

			} else {
				xmlfile = args[0];
				bytes = converter.convertFile(xmlfile);
				System.out.println(new String(bytes, "ASCII"));
			}
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
//		int count = 0;
//		while (count < length) {
//			System.out.print(new String(new byte[]{bytes[count + offset]}));
//			++count;
//		}
		//System.out.print("\n");

	}
}

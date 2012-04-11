package gnuplot;

import java.io.FileWriter;
import java.io.IOException;

/**
 * Generates gnuplot file for the given gnuplot data.
 * @author zilka
 */
public class GNUPlot {
	public static int PLOT_DOTS = 1;
	public static int PLOT_LINES = 2;

	int plotStyle = PLOT_DOTS;
	String gnuplotexepath = "/usr/local/bin/gnuplot";

	String yRange = "\n";
	String xRange = "\n";

	String yLabel = "\n";
	String xLabel = "\n";

	public void setxLabel(String xLabel) {
		this.xLabel = "set xlabel \"" + xLabel + "\"\n";
	}

	public void setyLabel(String yLabel) {
		this.yLabel = "set ylabel \"" + yLabel + "\"\n";
	}

	public void resetXLabel() {
		this.xLabel = "\n";
	}

	public void resetYLabel() {
		this.yLabel = "\n";
	}



	public void setYRange(float low, float high) {
		yRange = "set yrange [" + low + ":" + high + "]\n";
	}

	public void setXRange(float low, float high) {
		xRange = "set xrange [" + low + ":" + high + "]\n";
	}

	public void setPlotStyle(int plotStyle) {
		this.plotStyle = plotStyle;
	}

	public void plot(GNUPlotData data, String fileName) throws IOException {
		String cmd = "";
		cmd = cmd + "set terminal png font \"Tahoma,7\"\n";
		//cmd = cmd + "set size 2.5/5.0, 2.5/3.5\n";
		cmd = cmd + "set datafile missing 'NaN'\n";
		cmd = cmd + "set output '" + fileName + "'\n";
		//cmd = cmd + "set mxtics\n";
		cmd = cmd + "set grid xtics ytics mxtics\n";
		cmd = cmd + xRange;
		cmd = cmd + yRange;
		cmd = cmd + xLabel;
		cmd = cmd + yLabel;
		if(plotStyle == PLOT_DOTS)
			cmd = cmd + "plot '-' with dots pointtype 8 \n";
		else if(plotStyle == PLOT_LINES)
			cmd = cmd + "plot '-' with lines \n";
		cmd = cmd + data.toString();

		FileWriter fw = new FileWriter(fileName + ".gnuplot");
		fw.write(cmd);
		fw.close();
		return;
/*
		// create and start the process - redirect stderr to stdout
		ProcessBuilder processBuilder = new ProcessBuilder(gnuplotexepath);
		processBuilder.redirectErrorStream(true);

		// create our process
		Process process = processBuilder.start();

		// feed our baby
		OutputStreamWriter osw = new OutputStreamWriter(process.getOutputStream(), "UTF-8");
		osw.write(cmd);
		osw.close();
		try {
			process.waitFor();
		} catch (InterruptedException ex) {
			Logger.getLogger(GNUPlot.class.getName()).log(Level.SEVERE, null, ex);
		}
		//InputStreamReader isr = new InputStreamReader(process.getErrorStream());
		if(process.exitValue() != 0) {
			System.out.println("GNUPlot " + fileName + " (" + process.exitValue() + "): " + common.Utils.readBinaryStream(process.getErrorStream()));
		}
*/

	}

	public void resetXRange() {
		xRange = "";
	}

	public void resetYRange() {
		yRange = "";
	}

}

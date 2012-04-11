package common;

/**
 * Provides easier progress reporting.
 * @author zilka
 */
public class ProgressReporter {
	int progressCntr;
	long lastProgressReport = 0;
	String title;
	private long startTime;

	public ProgressReporter(String title) {
		this.title = title;
	}

	public void report() {
		report(1);
	}
	public void report(int cnt) {
		progressCntr += cnt;
		if (System.currentTimeMillis() - lastProgressReport > 1000) {
			System.out.println(title + ": " + progressCntr + "/1 second");
			lastProgressReport = System.currentTimeMillis();
			progressCntr = 0;
		}

	}

	public void start() {
		this.startTime = System.currentTimeMillis();
	}

	public void finish() {
		System.out.println(title + " took " + ((float)(System.currentTimeMillis() - startTime))/1000 + " seconds");
	}
}

package clldsystem.model;

/**
 * Holds information about a link.
 * @author zilka
 */
public class Link {
	public Long destId;
	public Double score;

	public Link(Long destId, Double score) {
		this.destId = destId;
		this.score = score;
	}

	@Override
	public boolean equals(Object obj) {
		//return ((Link)obj).destId == destId;
		return true;
	}

	@Override
	public int hashCode() {
		return destId.hashCode();
	}



}

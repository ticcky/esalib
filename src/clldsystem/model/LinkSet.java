package clldsystem.model;

import java.util.ArrayList;
import java.util.HashSet;

public class LinkSet extends ArrayList<Link> {

	public void removeDuplicates() {
		HashSet h = new HashSet(this);
		this.clear();
		this.addAll(h);
	}
}

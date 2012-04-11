package clldsystem.model;

/**
 * Holds information about an anchor in a document.
 * @author zilka
 */
public class Anchor {
	int offset;
	int length;
	String name;
	String target;
	int targetId;

	public Anchor() {

	}

	public Anchor(int offset2, int length2, String name2, String target2, int targetId2) {
		offset = offset2;
		length = length2;
		name = name2;
		target = target2;
		targetId = targetId2;
		
	}
	
	public int getLength() {
		return length;
	}
	
	public int getOffset() {
		return offset;
	}
	
	public String getTarget() {
		return target;
	}
	
	public String getName() {
		return name;
	}
	
	public int getTargetId() {
		return targetId;
	}

	public void setLength(int length) {
		this.length = length;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setTargetId(int targetId) {
		this.targetId = targetId;
	}

	public void setTarget(String target) {
		this.target = target;
	}

	public void setOffset(int offset) {
		this.offset = offset;
	}


}

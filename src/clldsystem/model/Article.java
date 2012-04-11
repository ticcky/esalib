package clldsystem.model;

import java.util.ArrayList;
import java.util.Scanner;


/**
 * Holds information about an article. Useful when inherited and extended.
 * @author zilka
 */
public class Article {	
	String title = "";
	String text = "";
	int id = -1;
	
	
	ArrayList<Anchor> anchors = new ArrayList<Anchor>();
	
	public Article() {

	}
	
	public Article(int id) {
		this.id = id;
	}
	
	
	public String getTitle() {
		return title;
	}
	
	public void setTitle(String title) {
		this.title = title;
	}
	
	public String getText() {
		return text;
	}
	
	public void setText(String text) {
		this.text = text;
	}
	
	public ArrayList<String> getParagraphs() {	
		Scanner scanner = new Scanner(text);
		
		ArrayList<String> result= new ArrayList<String>();
        
        while (scanner.hasNextLine()){
    		result.add(scanner.nextLine());
    	}
        
        return result;
	}
	
	public void addAnchor(int position, int length, String name, String target, int targetId) {
		anchors.add(new Anchor(position, length, name, target, targetId));
	}
	
	public ArrayList<Anchor> getAnchors() {
		return anchors;
	}

	public void setId(int nodeValue) {
		id = nodeValue;		
	}
	
	public int getId() {
		return id;
	}


	
	
	
}

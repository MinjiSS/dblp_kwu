import java.util.ArrayList;


public class XmlElements {
	String key;
	
	ArrayList<String> authors;
	ArrayList<String> cites;
	ArrayList<String> editors;
	
	String type;
	String title;
	String booktitle;
	int year;
	String journal;
	int volume;
	String month;
	String note;
	String series;
	String url;
	String ee;
	String coauthor;
	
	public XmlElements()
	{
		this.authors = new ArrayList<String>();
		this.cites = new ArrayList<String>();
		this.editors = new ArrayList<String>();
	}
	
}

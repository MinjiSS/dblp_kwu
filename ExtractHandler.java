import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.Attributes;

public class ExtractHandler extends DefaultHandler{
	
	XmlElements tag;
	HashSet<String> hs;
	Map<String, PutCharToXmlElements> map;
	String data;
	
	DBHandler dbh;
	
	private void InitHS()
	{
		hs = new HashSet<>(); //init HashSet - has dblp element
		hs.add("article");
		hs.add("inproceedings");
		hs.add("proceedings");
		hs.add("book");
		hs.add("incollection");
		hs.add("phdthesis");
		hs.add("mastersthesis");
		hs.add("www");
	}
	
	private void InitHM()
	{
		map = new HashMap<String, PutCharToXmlElements>();
		
		map.put("author", new PutCharToXmlElements() {
			public void put(XmlElements xe, String data) {
				xe.authors.add(data);
			};
		});
		
		map.put("cite", new PutCharToXmlElements() {
			public void put(XmlElements xe, String data) {
				xe.cites.add(data);
			};
		});
		
		map.put("editor", new PutCharToXmlElements() {
			public void put(XmlElements xe, String data) {
				xe.editors.add(data);
			};
		});
		/*
		map.put("type", new PutCharToXmlElements() {
			public void put(XmlElements xe, String data) {
				xe.type = data;
			};
		});*/
		
		map.put("title", new PutCharToXmlElements() {
			public void put(XmlElements xe, String data) {
				xe.title = data;
			};
		});
		
		map.put("booktitle", new PutCharToXmlElements() {
			public void put(XmlElements xe, String data) {
				xe.booktitle = data;
			};
		});
		
		map.put("year", new PutCharToXmlElements() {
			public void put(XmlElements xe, String data) {
				xe.year = Integer.parseInt(data);
			};
		});
		
		map.put("journal", new PutCharToXmlElements() {
			public void put(XmlElements xe, String data) {
				xe.journal = data;
			};
		});
		
		map.put("volume", new PutCharToXmlElements() {
			public void put(XmlElements xe, String data) {
				xe.volume = Integer.parseInt(data);
			};
		});
		
		map.put("month", new PutCharToXmlElements() {
			public void put(XmlElements xe, String data) {
				xe.month = data;
			};
		});
		
		map.put("note", new PutCharToXmlElements() {
			public void put(XmlElements xe, String data) {
				xe.note = data;
			};
		});
		
		map.put("series", new PutCharToXmlElements() {
			public void put(XmlElements xe, String data) {
				xe.series = data;
			};
		});
		
		map.put("url", new PutCharToXmlElements() {
			public void put(XmlElements xe, String data) {
				xe.url = data;
			};
		});
		
		map.put("ee", new PutCharToXmlElements() {
			public void put(XmlElements xe, String data) {
				xe.ee = data;
			};
		});
		
		map.put("coauthor", new PutCharToXmlElements() {
			public void put(XmlElements xe, String data) {
				xe.coauthor = data;
			};
		});
	}
	
	public ExtractHandler()
	{
		InitHS();
		InitHM();
		
		dbh = new DBHandler();
	}
	
	
	public boolean isElement(String localName)
	{
		return hs.contains(localName);
	}
	
	public void startDocument() 
	{	
		System.out.println("XML 문서 시작.");
		System.out.println("Database 접속 시작.");
		
		dbh.ConnectDB();
	}
	
	/*
	 * endDocument : 문서가 끝이 날 때 호출되는 이벤트 함수
	 * 하는일 : 아직 음슴. 메모리 정리정도로 사용 가능할 듯
	 * */
	public void endDocument() {}
	
	public void startElement(String uri, String localName, String qname, Attributes attr)
	{  
		if(isElement(qname))
		{
			tag = new XmlElements();
			tag.type = qname;
			tag.key = attr.getValue("key");
			return;
		}
	}
	
	public void endElement(String uri, String localName, String qname)
	{    		
		if(isElement(qname))
		{
			//end of get one element's info so call the DB insert Funtion
			// 여기서 tag 변수는 xml정보를 모두 담고 있으므로 가져다가 insert 하면 됨
			//dbh.InsertETC(tag);
			dbh.UpdateKeywordCount(tag);
			return;
		}
		try
		{
			map.get(qname).put(tag, data);
		}
		catch(Exception ex)
		{
		}
		
	}
	
	public void characters(char[] ch, int start, int length)
	{    
		if(length > 0)
		{
			data = new String(ch,start,length);
		}
	}
	
	public void ignorableWhitespace(char[] ch, int start, int length) {}

}

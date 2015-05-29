import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.sql.*;

//import jdk.internal.org.xml.sax.SAXException;

import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.Attributes;

public class ExtractHandler extends DefaultHandler{
	
	XmlElements tag;
	HashSet<String> hs;
	Map<String, PutCharToXmlElements> map;
	String data;
	
	Connection con = null;
	
	public ExtractHandler()
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
	
	
	public boolean isElement(String localName)
	{
		return hs.contains(localName);
	}
	
	public void startDocument() {
		
		System.out.println("XML 문서 시작.");
		System.out.println("Database 접속 시작.");
		
		try{
			Class.forName("org.gjt.mm.mysql.Driver");
			System.out.println("드라이버 검색 성공!!");	
		}catch(ClassNotFoundException e){
			System.out.println("드라이버 검색 실패!");
			return;
		}
		
		String url = "jdbc:mysql://dblp-db.cbrenledlob9.ap-northeast-1.rds.amazonaws.com";
		String user = "KW";
		String pass = "dblp2015";
		try{
			con = DriverManager.getConnection(url,user,pass);
			System.out.println("My-SQL 접속 성공!!");
		}catch(SQLException e){
			System.out.println("My-SQL 접속 실패");
		}
	}
	
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
			
			
			int n;
			
			//etc 테이블부터 채워보자
			PreparedStatement ps = null;
			String sql = "insert into DBLP_DB.etc values(?,?,?,?,?,?,?,?,?,?,?,?,?)"; //쿼리문
			//key, type, title, booktitle, year, journal, volume, month, note, series, url, ee, coauthor
			
			try{
				ps = con.prepareStatement(sql);
				ps.setString(1,tag.key); //키값
				ps.setString(2,tag.type); //type
				ps.setString(3,tag.title); //title
				ps.setString(4,tag.booktitle); //booktitle
				ps.setInt(5,tag.year); //year
				ps.setString(6,tag.journal); //journal
				ps.setInt(7,tag.volume); //volume
				ps.setString(8,tag.month); //month
				ps.setString(9,tag.note); //note
				ps.setString(10,tag.series); //시리즈
				ps.setString(11,tag.url); //url
				ps.setString(12,tag.ee); //ee
				ps.setString(13,tag.coauthor); //공동저자
				
				
				//퀴리문과 현재 tag의 값 출력
				System.out.println(ps);
				System.out.println("1." + tag.key + " 2." + tag.type + " 3." + tag.title + " 4." + tag.booktitle + " 5." + tag.year
						 + " 6." + tag.journal + " 7." + tag.volume + " 8." + tag.month + " 9." + tag.note + " 10." + tag.series
						 + " 11." + tag.url + " 12." + tag.ee + " 13." + tag.coauthor);
				
				System.out.println();
				
				/*
				n = ps.executeUpdate(); //데이터 삽입
				if(n<=0){
					System.out.println("etc table 데이터추가 실패");
					return;
				}*/
			}
			catch(SQLException e){
				e.printStackTrace();
			}
			
			//author table 데이터 추가
			/*
			int i;
			ps = null; //초기화
			sql = "insert into DBLP_DB.author values(?,?)"; //쿼리문
			
			try{
				ps = con.prepareStatement(sql);
				ps.setString(1,tag.key); //키값
				
				for(i=0;i<tag.authors.size(); i++)
				{//리스트에 있는만큼 저자들을 추가
					ps.setString(2,tag.authors.get(i)); //type

					n = ps.executeUpdate(); //데이터 삽입
					if(n<=0){
						System.out.println("author table 데이터추가 실패");
						return;
					}
				}
			}
			catch(SQLException e){
				e.printStackTrace();
			}
			
			
			//cite 테이블
			ps = null; //초기화
			sql = "insert into DBLP_DB.cite values(?,?)"; //쿼리문
			
			try{
				ps = con.prepareStatement(sql);
				ps.setString(1,tag.key); //키값
				
				for(i=0;i<tag.cites.size(); i++)
				{//리스트에 있는만큼 인용정보를 추가
					ps.setString(2,tag.cites.get(i)); //type

					n = ps.executeUpdate(); //데이터 삽입
					if(n<=0){
						System.out.println("cite table 데이터추가 실패");
						return;
					}
				}
			}
			catch(SQLException e){
				e.printStackTrace();
			}
			
			
			//editor 테이블
			ps = null; //초기화
			sql = "insert into DBLP_DB.editor values(?,?)"; //쿼리문
			
			try{
				ps = con.prepareStatement(sql);
				ps.setString(1,tag.key); //키값
				
				for(i=0;i<tag.editors.size(); i++)
				{//리스트에 있는만큼 인용정보를 추가
					ps.setString(2,tag.editors.get(i)); //type

					n = ps.executeUpdate(); //데이터 삽입
					if(n<=0){
						System.out.println("editor table 데이터추가 실패");
						return;
					}
				}
			}
			catch(SQLException e){
				e.printStackTrace();
			}*/
			
			System.out.println("1." + tag.key + " 2." + tag.type + " 3." + tag.title + " 4." + tag.booktitle + " 5." + tag.year
					 + " 6." + tag.journal + " 7." + tag.volume + " 8." + tag.month + " 9." + tag.note + " 10." + tag.series
					 + " 11." + tag.url + " 12." + tag.ee + " 13." + tag.coauthor);
			
			
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

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;

import com.mysql.jdbc.exceptions.jdbc4.MySQLIntegrityConstraintViolationException;

public class DBHandler {
	
	Connection con = null;
	
	private String url = "jdbc:mysql://dblp-db.cbrenledlob9.ap-northeast-1.rds.amazonaws.com";;
	private String user = "KW";
	private String pass = "dblp2015";
	
	private static final int CONF_OR_JOURNAL = 1;
	private static final int CONF_OR_JOURNAL_NAME = 2;
	
	private static final int KEY = 1; // 
	private static final int TYPE = 2;
	private static final int TITLE = 3;
	private static final int BOOKTITLE = 4;
	private static final int YEAR = 5;
	private static final int JOURNAL = 6;
	private static final int VOLUME = 7;
	private static final int MONTH = 8;
	private static final int NOTE = 9;
	private static final int SERIES = 10;
	private static final int URL = 11;
	private static final int EE = 12;
	private static final int COAUTHOR = 13;
	
	/*
	 * GetServerAccountInto()
	 * 아마존 서버뿐만이 아니라 다른 서버를 사용하고 싶을 때
	 * 입력을 통해 새로운 서버에 접속할 수 있도록 해주는 input 함수
	 * 단 다른 서버의 db, table의 이름도 아마존꺼랑 같아야함
	 */
	private void GetServerAccountInto()
	{
		Scanner sc = new Scanner(System.in);
		System.out.println("아마존 계정 사용 -> 아무거나 입력 \n개인 계정 사용 ->q 입력");
		String accountSelection = sc.nextLine();
		
		if("q".compareTo(accountSelection) == 0 || "Q".compareTo(accountSelection) == 0)
		{
			//사설계정 이용하신 답니다.
			System.out.print("input your url : ");
			url = "jdbc:mysql://" + sc.nextLine();
			System.out.print("input your user ID : ");
			user = sc.nextLine();
			System.out.print("input your user PASSWORD : ");
			pass = sc.nextLine();
		}
		
		sc.close();
	}
	
	/*
	 * ConnectDB()
	 * DB에 접속하기만 할 뿐인 함수
	 */
	public void ConnectDB()
	{
		GetServerAccountInto();
		
		try
		{
			Class.forName("org.gjt.mm.mysql.Driver");
			System.out.println("드라이버 검색 성공!!");	
		}
		catch(ClassNotFoundException e)
		{
			System.out.println("드라이버 검색 실패!");
			return;
		}
		
		try
		{
			con = DriverManager.getConnection(url,user,pass);
			System.out.println("My-SQL 접속 성공!!");
		}
		catch(SQLException e)
		{
			System.out.println("My-SQL 접속 실패");
		}
	}

	/*
	 * InsertETC(ArrayList<XmlElements> xeList)
	 * 다량의 XmlElements 들을 DB insert시
	 * InsertETC 자체를 여러번 호출하는게 아니라 
	 * XmlElements를 ArrayList의 형태로 전달하여
	 * insert를 반복해주도록 오버로딩된 함수
	 */
	public void InsertETC(ArrayList<XmlElements> xeList)
	{
		for(int i = 0; i < xeList.size(); ++i)
		{
			InsertETC(xeList.get(i));
		}
	}
	
	/*
	 * InsertETC(XmlElements tag)
	 * XmlElements 하나를 인자로 받아 DB ETC table에
	 * insert 해주는 함수
	 */
	public void InsertETC(XmlElements tag)
	{
		int n = 0;
		
		//etc 테이블부터 채워보자
		PreparedStatement ps = null;
		String sql = "insert into dblp.etc values(?,?,?,?,?,?,?,?,?,?,?,?,?)"; //쿼리문
		//key, type, title, booktitle, year, journal, volume, month, note, series,q url, ee, coauthor
		
		try
		{
			ps = con.prepareStatement(sql);
			ps.setString(KEY, tag.key); //키값
			ps.setString(TYPE, tag.type); //type
			ps.setString(TITLE, tag.title); //title
			ps.setString(BOOKTITLE, tag.booktitle); //booktitle
			ps.setInt(YEAR, tag.year); //year
			ps.setString(JOURNAL, tag.journal); //journal
			ps.setInt(VOLUME, tag.volume); //volume
			ps.setString(MONTH, tag.month); //month
			ps.setString(NOTE, tag.note); //note
			ps.setString(SERIES, tag.series); //시리즈
			ps.setString(URL, tag.url); //url
			ps.setString(EE, tag.ee); //ee
			ps.setString(COAUTHOR, tag.coauthor); //공동저자
			
			
			//퀴리문과 현재 tag의 값 출력
			/*
			System.out.println(ps);
			System.out.println("1." + tag.key + " 2." + tag.type + " 3." + tag.title + " 4." + tag.booktitle + " 5." + tag.year
					 + " 6." + tag.journal + " 7." + tag.volume + " 8." + tag.month + " 9." + tag.note + " 10." + tag.series
					 + " 11." + tag.url + " 12." + tag.ee + " 13." + tag.coauthor);
			
			System.out.println();
			*/
			
			n = ps.executeUpdate(); //데이터 삽입
			
			if(n<=0)
			{
				System.out.println("etc table 데이터추가 실패");
				return;
			}
		}
		/* 키값 중복 예외등이 발생시 하는 예외*/
		catch(MySQLIntegrityConstraintViolationException e) 
		{
			e.printStackTrace();
		}
		catch(SQLException e)
		{
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

	}
	
	/*
	 * public void UpdatekeywordCount(XmlElements tag)
	 * tag 값을 인자로 받아와 journal or conference
	 * 테이블에 word count를 update 해 주는 함수
	 * 업데이트 시도 후 해당 컬럼이 없으면
	 * insert 한다.
	 */
	public void UpdateKeywordCount(XmlElements tag)
	{
		String[] slicedUrl = tag.url.split("/");
		
		if(slicedUrl[CONF_OR_JOURNAL].compareTo("journals") == 0) //이거 좀 노답
			slicedUrl[CONF_OR_JOURNAL] = "journal";
		
		ArrayList<String> keywordList = new ArrayList<String>(Arrays.asList(tag.title.split(" "))); //title 을 split 하되, 원활한 필터링을 위해 list로 만듬
		FilteringKeyword(keywordList);
		TrimingKeyword(keywordList);
		
		UpdateCountTable(slicedUrl, keywordList);
	}
	
	public void InsertCountTable(String[] slicedUrl, String insertKeyword)
	{
		String insertQuery = "insert into dblp." + slicedUrl[CONF_OR_JOURNAL] + "keywordcount values(?,?,1)";
		int n;
		
		try 
		{
			PreparedStatement ps1 = con.prepareStatement(insertQuery);
			ps1.setString(1, slicedUrl[CONF_OR_JOURNAL_NAME]);
			ps1.setString(2, insertKeyword);
			n = ps1.executeUpdate(); //데이터 삽입
			
			if(n<=0)
			{
				System.out.println("keywordcounttable insert fail");
				return;
			}
		} catch (SQLException e1) 
		{
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
	
	public void UpdateCountTable(String[] slicedUrl, ArrayList<String> keywordList)
	{
		int n = 0;
	
		String updateQuery = "update dblp." + slicedUrl[CONF_OR_JOURNAL] + "keywordcount set count = count + 1 where " + slicedUrl[CONF_OR_JOURNAL] +" = ? AND keyword = ?";
		
		for(int i = 0; i < keywordList.size(); ++i)
		{
			try 
			{
				PreparedStatement ps = con.prepareStatement(updateQuery);
				ps.setString(1, slicedUrl[CONF_OR_JOURNAL_NAME]);
				ps.setString(2, keywordList.get(i));
				n = ps.executeUpdate();
				
				if(n<=0)
				{
					System.out.println("keywordcounttable update fail");
					return;
				}
			} 
			catch (SQLException e) 
			{
				// TODO Auto-generated catch block
				//여기서 insert 해줘야지
				e.printStackTrace();
				InsertCountTable(slicedUrl, keywordList.get(i));
			}
		}
		
	}
	
	public void FilteringKeyword(ArrayList<String> keywordList)
	{
		//나중에 기능구현하지뭐 and what that 같은거 제거해줌
	}
	public void TrimingKeyword(ArrayList<String> keywordList)
	{
		/*나중에 기능구현22 키워드에 "database" 같은걸 database 로 바꿔줌
		  한마디로 키워드에 붙어있는 특수문자 제거함 */
	}
}

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
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
	 * 아마존 서버뿐만이 아니라 다른 mysql서버를 사용하고 싶을 때
	 * 입력을 통해 새로운 서버에 접속할 수 있도록 해주는 input 함수
	 * 단 다른 서버의 db, table의 이름도 아마존꺼랑 같아야함 관련 기능 동작함. 로컬디비 이용하려고 추가
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
		else if("qwer".compareTo(accountSelection) == 0)
		{
			url = "jdbc:mysql://127.0.0.1";
			user = "opi";
			pass = "2011726041";
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
	 * insert를 반복해주도록 오버로딩된 함수s
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
	 * tag.url 에 conf 인지 journal인지 그곳의 이름은 무언지 정보가 있음.
	 * slicedUrl[CONF_OR_JOURNAL] 가 conf 인지 journal인지의 정보
	 * slicedUrl[CONF_OR_JOURNAL_NAME] 가 그곳의 이름 정보
	 * 정보를 추출한 뒤 실질적인 update 부분은 UpdateCountTable를 호출하여 진행  
	 */
	public void UpdateKeywordCount(XmlElements tag)
	{
		if(tag.url == null || "".compareTo(tag.url) == 0)
			return;
		
		String[] slicedUrl = tag.url.split("/");
		
		if(slicedUrl[CONF_OR_JOURNAL].compareTo("journals") == 0) //이거 좀 노답
			slicedUrl[CONF_OR_JOURNAL] = "journal";
		
		ArrayList<String> keywordList = new ArrayList<String>(Arrays.asList(tag.title.split(" "))); //title 을 split 하되, 원활한 필터링을 위해 list로 만듬
		TrimingKeyword(keywordList);
		FilteringKeyword(keywordList);
		
		UpdateCountTable(slicedUrl, keywordList);
	}
	
	/*
	 * void UpdateCountTable(String[] slicedUrl, ArrayList<String> keywordList)
	 * slicedUrl은 tag.url을 "/"로 split 한 값.
	 * keywordList는 tag.title을 " "로 split 하여 전처리 한 값들의 리스트 전처리 완벽하지 않음
	 */
	public void UpdateCountTable(String[] slicedUrl, ArrayList<String> keywordList)
	{	
		try
		{
			String insertOnDupQuery = "INSERT INTO dblp." + slicedUrl[CONF_OR_JOURNAL] + "keywordcount (journal, keyword, count) values(?,?,1) ON DUPLICATE KEY UPDATE count = count + 1";
			PreparedStatement ps = con.prepareStatement(insertOnDupQuery);
			
			con.setAutoCommit(false);
			for(int i = 0; i < keywordList.size(); ++i)
			{
				ps.setString(1, slicedUrl[CONF_OR_JOURNAL_NAME]);
				ps.setString(2, keywordList.get(i));
				ps.addBatch();
			}
			ps.executeBatch(); //오류검사 안한다면 리턴값 받을 필요x
			con.commit();
			ps.close();
		}
		catch(SQLException e)
		{
			e.printStackTrace();
		}
	}
	
	public void FilteringKeyword(ArrayList<String> keywordList)
	{
		//나중에 기능구현하지뭐 and what that 같은거 제거해줌
		int i;
	
		for(i =0; i < keywordList.size(); ++i) 
			if(isTrashValue(keywordList.get(i))) //숫자, null, length = 0 일때 지워버림
				keywordList.remove(i);
		
		for(i =0; i < keywordList.size(); ++i)
			keywordList.set(i, keywordList.get(i).toLowerCase());
	}
	
	public void TrimingKeyword(ArrayList<String> keywordList)
	{
		/*나중에 기능구현22 키워드에 "database" 같은걸 database 로 바꿔줌
		  한마디로 키워드에 붙어있는 특수문자 제거함 */
		for(int i = 0; i < keywordList.size(); ++i)
		{
			keywordList.set(i, keywordList.get(i).replaceAll("[^a-zA-Z0-9\\s]", ""));
			//앞, 뒤, 문장 중간에 관계없이 알파벳만 남기고 다 없앰
			//Triming 방식에 대해서는 재고의 여지가 있음.
		}
	}
	
	public static boolean isTrashValue(String str) {
		if (str == null) {
			return true;
		}
		if ("".compareTo(str) == 0) {
			return true;
		}
		int length = str.length();
		if (length == 0) {
			return true;
		}
		int i = 0;
		if (str.charAt(0) == '-') {
			if (length == 1) {
				return false;
			}
			i = 1;
		}
		for (; i < length; i++) {
			char c = str.charAt(i);
			if (c <= '/' || c >= ':') {
				return false;
			}
		}
		return true;
	}
	
	/*
	 * DB에 count value가 엄청 작아서 의미가 없는 애들을 지워보자.
	 * 삭제 기준 count value / Sum(count value of one conference) < x
	 * 전체 단어 수 중 비율이 x이하인 값들을 지운다.
	 * whichTable을 인자로 받아서 어느 테이블의 쓰레기 값을 지울 것인지 정할 수 있도록 한다.-> 위험부담을 덜기 위해
	 */
	public void DeleteTrashValueInDB(String whichTable) //whichTable 은 conf or journal 일것
	{
		PreparedStatement psSQToCI = null, psSQToKWC = null; //psSQToCI = ps select query to countinfo, psSQToKWC = ps select query to keywordcount
		PreparedStatement psDQ = null; // psDQ = ps Delete Query
		
		ResultSet rsToCI = null, rsToKWC = null;
		
		String strSQToCI = "SELECT * FROM dblp.countinfo WHERE confOrJour = ?"; // ? Table
		String strSQToKWC = "SELECT keyword, count FROM dblp." + whichTable + "keywordcount WHERE " + whichTable + " = ?"; // ? = eventName
		String strDQ = "DELETE FROM dblp." + whichTable + "keywordcount WHERE " + whichTable + " = ? AND keyword = ?";
		
		double countSum;
		String eventName;
		String keyword;
		
		int keywordCount;
		final double DELETE_THRESHOLD = 0.01;
		try 
		{
			psSQToCI = con.prepareStatement(strSQToCI);
			psSQToKWC = con.prepareStatement(strSQToKWC);
			psDQ = con.prepareStatement(strDQ);
			
			psSQToCI.setString(1, whichTable);
			rsToCI = psSQToCI.executeQuery();
			while(rsToCI.next()) // countinfo table의 모든 row에 대해서 반복
			{
				countSum = (double)rsToCI.getInt("countSum");
				eventName = rsToCI.getString("eventName");
				
				
				psSQToKWC.setString(1, eventName);
				rsToKWC = psSQToKWC.executeQuery();
				while(rsToKWC.next()) //해당 eventName의 모든 경우에 대해서 비율비교하고 삭제 반복
				{
					keywordCount = rsToKWC.getInt("count");
					keyword = rsToKWC.getString("keyword");
					if(keywordCount == 1 || (keywordCount / countSum) < DELETE_THRESHOLD)
					{
						//지울 녀석 추가요
						psDQ.setString(1, eventName);
						psDQ.setString(2, keyword);
						psDQ.addBatch();
					}
				}
				psDQ.executeBatch(); //추가된 삭제리스트 한번에 삭제
			}
		} 
		catch (SQLException e) 
		{		
			e.printStackTrace();
		}
		finally
		{
			if(psDQ != null) try {psDQ.close();psDQ = null;} catch(SQLException ex){}
			if(rsToKWC != null) try {rsToKWC.close();rsToKWC = null;} catch(SQLException ex){}
			if(psSQToKWC != null) try {psSQToKWC.close();psSQToKWC = null;} catch(SQLException ex){}
			if(rsToCI != null) try {rsToCI.close();rsToCI = null;} catch(SQLException ex){}
			if(psSQToCI != null) try {psSQToCI.close();psSQToCI = null;} catch(SQLException ex){}
		}
	}
	
	public void InsertCountInfo(String whichTable) //whichTable 은 conf or journal 일것
	{
		PreparedStatement psSQ = null, psIQ = null;
		ResultSet rsSQ = null;
		
		String selectQuery = "SELECT " + whichTable + ", SUM(count) as sum FROM dblp." + whichTable + "keywordcount GROUP BY " + whichTable;
		String insertQuery = "INSERT INTO dblp.countinfo (confOrJour, eventName, countSum) values(?,?,?)";
		try 
		{
			psSQ = con.prepareStatement(selectQuery);
			rsSQ = psSQ.executeQuery();
			
			psIQ = con.prepareStatement(insertQuery);
			con.setAutoCommit(false);
			
			String dbg; //dbg
			int dbgc; //dbg
			
			int i = 0;
			final int batchSize = 10;
			while(rsSQ.next())
			{				
				dbg = rsSQ.getString(whichTable); //dbg
				dbgc = rsSQ.getInt("sum"); //dbg
				System.out.println(dbg + "  " + dbgc);
				psIQ.setString(1, whichTable);
				psIQ.setString(2, rsSQ.getString(whichTable));
				psIQ.setInt(3, rsSQ.getInt("sum"));
				psIQ.addBatch();
				if (++i == batchSize)
				{
					psIQ.executeBatch();
					con.commit();
					i = 0;
				}
			}
			psIQ.executeBatch();
			con.commit();
		} 
		catch (SQLException e) 
		{
			try { con.rollback(); } catch (SQLException e1) { e1.printStackTrace();}
			e.printStackTrace();
		}
		finally
		{
			if(psIQ != null) try {psIQ.close();psIQ = null;} catch(SQLException ex){}
			if(rsSQ != null) try {rsSQ.close();rsSQ = null;} catch(SQLException ex){}
			if(psSQ != null) try {psSQ.close();psSQ = null;} catch(SQLException ex){}
		}
	}
	
	public int SumKeywordCount(String tableName)
	{
		//String sumQuery = "SELECT SUM(count) From " + tableName;
		try 
		{
			PreparedStatement ps = con.prepareStatement("SELECT SUM(count) From " + tableName + "where conf =");
			ResultSet result = ps.executeQuery();
			result.next();
		} 
		catch (SQLException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
	}
	
	/*
	 * DB에 what that how 같은 애들 날려주는 함수를 만들어 보자
	 */
	public void DeleteUselessValueInDB(String whichTable)
	{
		ArrayList<String> al = new ArrayList<String>();
		
		al.add("in");
		al.add("on");
		al.add("the");
		al.add("of");
		al.add("a");
		al.add("for");
		al.add("by");
		al.add("with");
		al.add("to");
		al.add("an");
		al.add("and");
		al.add("using");
		al.add("problem");
		al.add("systems");
		
		String deleteQuery = "DELETE FROM dblp." + whichTable + "keywordcount WHERE keyword = ?";
		PreparedStatement ps = null;
		try 
		{
			ps = con.prepareStatement(deleteQuery);
			con.setAutoCommit(false);
			for(int i = 0; i < al.size(); ++i)
			{
				ps.setString(1, al.get(i));
				ps.addBatch();
			}
			ps.executeBatch();
			con.commit();
		} 
		catch (SQLException e) 
		{
			if(con != null) try { con.rollback(); } catch (SQLException ex) {}
			e.printStackTrace();
		}
		finally 
		{
			if(ps != null) try { ps.close(); } catch(SQLException ex) {}
		}
	}
}

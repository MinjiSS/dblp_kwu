import java.io.*;
import javax.xml.parsers.*;
import org.xml.sax.SAXException;

public class Main{
	public static void main(String[] args)
	{
		File xmlFile = new File("dblp.xml");
		process(xmlFile);
	}
	
	private static void process(File file){
		SAXParserFactory spf = SAXParserFactory.newInstance();
		SAXParser parser = null;
		spf.setNamespaceAware(true); // namespace의 사용여부를 결정
		spf.setValidating(true);  // 유효성 검사 여부를 지정
		try{
			parser = spf.newSAXParser();
		} catch(SAXException e)
		{
			e.printStackTrace(System.err);
			System.exit(1);
		}catch(ParserConfigurationException e){
			e.printStackTrace(System.err);
			System.exit(1);
		}
		
		System.out.println("\n파싱 시작 : "+file+"\n");
		ExtractHandler handler = new ExtractHandler();
		try{
			parser.parse(file, handler);
		}catch(IOException e){
			e.printStackTrace(System.err);
		}catch(SAXException e){
			e.printStackTrace(System.err);
		}
		System.out.println("\n파싱 끝 : "+file+"\n");
	}
}
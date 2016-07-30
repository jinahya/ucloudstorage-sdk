import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.HashMap;
import org.apache.log4j.Logger;
import com.olleh.ucloudbiz.ucloudstorage.FilesClientExt;
import com.olleh.ucloudbiz.ucloudstorage.FilesContainerInfoExt;
import com.olleh.ucloudbiz.ucloudstorage.FilesObjectMetaDataExt;
import com.olleh.ucloudbiz.ucloudstorage.FilesConstantsExt;

import com.rackspacecloud.client.cloudfiles.FilesObject;

public class GetContainerInfo {
	private String container;
	private Properties prop;
	
	public static void main(String[] args) {
		GetContainerInfo t = new GetContainerInfo();
		FilesClientExt fc = new FilesClientExt("your e-mail", 
		                                       "your api_key", 
		                                       "https://api.ucloudbiz.olleh.com/storage/v1/auth/", 3000);
		FilesContainerInfoExt cinfo = null;                                       
		try {
			fc.login();	
			cinfo = fc.getContainerInfoExt(t.container);
			System.out.println(cinfo.getName());     // container name
			System.out.println(cinfo.getObjectCount()); // 저장하고 있는 총 파일 개수
			System.out.println(cinfo.getTotalSize());   // 저장하고 있는 전체 용량
			System.out.println(cinfo.getLoggingStatus());  // logging 설정 상태
			System.out.println(cinfo.getStaticWebsiteConfig()); // staticweb 설정상태
			System.out.println(cinfo.getWebCss());              
			System.out.println(cinfo.getWebError());            
			System.out.println(cinfo.getWebIndex());            
			System.out.println(cinfo.getWebListings());         
		}   
		catch(Exception e) {
			System.out.println("Error!");	
			e.printStackTrace();		
		} 
		
	}
		
	private GetContainerInfo() { getConfig(); }                                                          	
	                                                           	
	private void getConfig() {                                 	
		String srcFile = ".\\conf\\test2.cfg";                    	
		prop = new Properties();                                  	
		try { prop.load(new FileInputStream(new File(srcFile))); }	
		catch(IOException e) {                                    	
			System.out.println("I/O Error!!!");                      	
			e.printStackTrace();                                     	
		} 		                                                      	
		container = prop.getProperty("container").trim();
	}
}
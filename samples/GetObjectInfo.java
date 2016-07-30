import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.log4j.Logger;
import com.olleh.ucloudbiz.ucloudstorage.FilesClientExt;
import com.olleh.ucloudbiz.ucloudstorage.FilesContainerInfoExt;
import com.olleh.ucloudbiz.ucloudstorage.FilesObjectMetaDataExt;

import com.rackspacecloud.client.cloudfiles.FilesObject;

public class GetObjectInfo {
	private String container;
	private String object;
	private Properties prop;
	
	public static void main(String[] args) {
		GetObjectInfo t1 = new GetObjectInfo();
		FilesClientExt fc = new FilesClientExt("your e-mail", 
		                                       "your api_key", 
		                                       "https://ssproxy.ucloudbiz.olleh.com/auth/v1.0", 3000);
		FilesObjectMetaDataExt objMeta = null;
		
		try {
			fc.login();	
			//System.out.println(t1.object);
			objMeta = fc.objectExists(t1.container, t1.object);		
			if(objMeta == null) {
				System.out.println("no file");
			}
			else {
				System.out.println("file size : " + objMeta.getContentLength());
				System.out.println("file etag : " + objMeta.getETag());				
			}
		}
		catch(Exception e) {
			System.out.println("Error!");	
			e.printStackTrace();		
		} 
		
	}
		
	private GetObjectInfo() {                                          	
		getConfig();			                                           	
	}                                                          	
	                                                           	
	private void getConfig() {                                 	
		String srcFile = ".\\conf\\test1.cfg";                    	
		prop = new Properties();                                  	
		try { prop.load(new FileInputStream(new File(srcFile))); }	
		catch(IOException e) {                                    	
			System.out.println("I/O Error!!!");                      	
			e.printStackTrace();                                     	
		} 		                                                      	
		container = prop.getProperty("container").trim();             	
		object    = prop.getProperty("object").trim();  	          	
	}
}
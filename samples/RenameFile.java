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

public class RenameFile {
	private String container;
	private String objname1;
	private String objname2;
	private Properties prop;
	
	public static void main(String[] args) {
		RenameFile t = new RenameFile();
		FilesClientExt fc = new FilesClientExt("your e-mail", 
		                                       "your api_key", 
		                                       "https://ssproxy.ucloudbiz.olleh.com/auth/v1.0", 3000);
		
		try {
			fc.login();	
			System.out.println(fc.renameObject(t.container, t.objname1, t.objname2));	
		}
		catch(Exception e) {
			System.out.println("Error!");	
			e.printStackTrace();		
		} 
		
	}
		
	private RenameFile() {                                          	
		getConfig();			                                           	
	}                                                          	
	                                                           	
	private void getConfig() {                                 	
		String srcFile = ".\\conf\\test3.cfg";                    	
		prop = new Properties();                                  	
		try { prop.load(new FileInputStream(new File(srcFile))); }	
		catch(IOException e) {                                    	
			System.out.println("I/O Error!!!");                      	
			e.printStackTrace();                                     	
		} 		                                                      	
		container = prop.getProperty("container").trim(); 
		objname1  = prop.getProperty("objname1").trim();
		objname2  = prop.getProperty("objname2").trim();  
	}
}
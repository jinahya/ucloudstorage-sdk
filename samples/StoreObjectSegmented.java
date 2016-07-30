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
import com.olleh.ucloudbiz.ucloudstorage.RepeatableFileInputStream;
import com.olleh.ucloudbiz.ucloudstorage.InputSubstream;

                                    
import com.rackspacecloud.client.cloudfiles.FilesObject;

public class StoreObject {
	private String container;
	private String objname;
	private String saveas;
	private Properties prop;
	
	public static void main(String[] args) {
		StoreObject t = new StoreObject();
		FilesClientExt fc = new FilesClientExt("your e-mail", 
		                                       "your api_key", 
		                                       "https://ssproxy.ucloudbiz.olleh.com/auth/v1.0", 3000);
		File obj = null;    
		inputStream = new InputSubstream(new RepeatableFileInputStream(uploadPartRequest.getFile()),
                                                 uploadPartRequest.getFileOffset(), partSize);                                   
		try {
			obj = new File(".\\testData\\" + t.objname);
			fc.login();	
			System.out.println(fc.storeObjectSegmentedAs(t.container, obj, t.saveas));
		}
		catch(Exception e) {
			System.out.println("Error!");	
			e.printStackTrace();		
		} 
		
	}
		
	private StoreObjectSegmented() { getConfig(); }                                                          	
	                                                           	
	private void getConfig() {                                 	
		String srcFile = ".\\conf\\test4.cfg";                    	
		prop = new Properties();                                  	
		try { prop.load(new FileInputStream(new File(srcFile))); }	
		catch(IOException e) {                                    	
			System.out.println("I/O Error!!!");                      	
			e.printStackTrace();                                     	
		} 		                                                      	
		container = prop.getProperty("container").trim();
		objname = prop.getProperty("objname").trim();
		saveas = prop.getProperty("saveas").trim();
	}
}
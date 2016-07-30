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
import com.olleh.ucloudbiz.ucloudstorage.FilesContainerExt;

public class ContainerLogging {
	private String containerName = "test";
	
	public static void main(String[] args) {
		ContainerLogging t = new ContainerLogging();
		FilesClientExt fc = new FilesClientExt("your e-mail", 
		                                       "your api_key", 
		                                       "https://api.ucloudbiz.olleh.com/storage/v1/auth/", 3000);
		FilesContainerExt fcx = null;
		
		try {
			fc.login();	
			fcx = new FilesContainerExt(containerName, fc);
			System.out.println(fcx.setContainerLogging(true));	
		}
		catch(Exception e) {
			System.out.println("Error!");	
			e.printStackTrace();		
		} 
		
	}
}
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package local;


import com.rackspacecloud.client.cloudfiles.FilesClient;
import com.rackspacecloud.client.cloudfiles.FilesContainerInfo;
import com.rackspacecloud.client.cloudfiles.FilesObject;
import java.util.HashMap;
import java.util.List;
import static org.junit.Assert.assertTrue;
import org.junit.Test;


/**
 *
 * @author Jin Kwon &lt;jinahya_at_gmail.com&gt;
 */
public class LocalTest {


    @Test
    public void test() throws Exception {
        final String user = "MTQ2Njc2MTg4MTE0NjY3NTgyNzQyNTEx";
        final String pass = "MTQ2Njc2MDAxMDE0NjY3NTgyNzQxMTQ3";
        final String url = "https://api.ucloudbiz.olleh.com/storage/v1/auth";
        final FilesClient client = new FilesClient(user, pass, url, 10);
        assertTrue(client.login());
        final String container = "test1";
        final String object = "object";
        if (!client.containerExists(container)) {
            client.createContainer(container);
            System.out.println("container created");
        }
        client.storeObject(container, new byte[0], "application/octet-stream", object, new HashMap<String, String>());
        System.out.println("object stored");

        final FilesContainerInfo containerInfo = client.getContainerInfo(container);
        System.out.println("container info retrieved");
        System.out.println("container.name: " + containerInfo.getName());
        System.out.println("container.objectCount: " + containerInfo.getObjectCount());
        System.out.println("container.totalSize: " + containerInfo.getTotalSize());

        final List<FilesObject> files = client.listObjects(container);
        for (final FilesObject file : files) {
            System.out.println("file.name: " + file.getName());
        }
        client.deleteObject(container, object);
        System.out.println("ojbect deleted");
        client.deleteContainer(container);
        System.out.println("container deleted");
    }

}


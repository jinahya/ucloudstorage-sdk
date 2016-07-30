/*
 * See COPYING for license information.
 */ 

package com.olleh.ucloudbiz.ucloudstorage;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.io.IOException;
import org.apache.http.HttpException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.Header;
import org.apache.http.HttpStatus;
import org.apache.http.HttpException;

import com.olleh.ucloudbiz.ucloudstorage.utils.SegmentObject;
import com.olleh.ucloudbiz.ucloudstorage.utils.HmacDigest;

import com.rackspacecloud.client.cloudfiles.wrapper.RequestEntityWrapper;
import com.rackspacecloud.client.cloudfiles.FilesClient;
import com.rackspacecloud.client.cloudfiles.FilesConstants;
import com.rackspacecloud.client.cloudfiles.FilesException;
import com.rackspacecloud.client.cloudfiles.FilesAuthorizationException;
import com.rackspacecloud.client.cloudfiles.FilesInvalidNameException;
import com.rackspacecloud.client.cloudfiles.FilesNotFoundException;

import com.rackspacecloud.client.cloudfiles.IFilesTransferCallback; 

/**
 * <p>
 * �� Ŭ������ FilesClient�� Ȯ�� Ŭ������ �߰����� API�� ���� �ִ�.
 * </p>
 * <p>
 * �߰��� �����ϴ� API�� ���� �̵�, ���� �̸� �����ϱ⸦ �����Ͽ� static website �� container logging
 * ���� �� ���� ��ȸ ����� �����Ѵ�. �� Ŭ������ FilesClient�� ��� ����� �����ϰ� ������
 * FilesClient�� ����� ��� �߰��� �����Ǵ� API�� ����� �� ����.
 * ���� ���ε带 ��û�� ���, ������ ũ��� ���� ���ε� ���� �������� ���� ���
 * �⺻������ ������ ũ��� 10MB, ���� ���ε� ó�� ���� 5�� �����Ǿ� �����Ѵ�.
 * ������ ������ ũ�⺸�� ���� ������ ���Ҿ��ε�� ��û�� ���, FilesClientExt.storeObject�� ó���Ѵ�.  
 * </p>
 * 
 * <p>
 * <blockquote><pre>
 * // Native FilesClient Class ��� 
 * FilesClient = new FilesClient("email", "DA0NjEzMjIzODIzMzIyN", "https://api.ucloudbiz.olleh.com/storage/v1/auth", 3000); 
 * 
 * // FilesClientExt Class�� ����� ��
 * FilesClientExt = new FilesClientExt("email", "DA0NjEzMjIzODIzMzIyN", "https://api.ucloudbiz.olleh.com/storage/v1/auth", 3000); 
 * </pre></blockquote>
 * </p>
 * 
 * @see	com.rackspacecloud.client.cloudfiles.FilesClient
 * 
 * 
 * @author KT Ŭ���彺�丮����
 */
 
public class FilesClientExt extends FilesClient {

    /**
     * @param client  The HttpClient to talk to KT ucloud storage service
     * @param email   ucloudbiz ��Ż ����(e-mail) 
     * @param apikey  �ش� ������ API KEY
     * @param authUrl authUrl(https://api.ucloudbiz.olleh.com/storage/v1/auth)
     * @param connectionTimeOut  The connection timeout, in ms.
     */
    public FilesClientExt(HttpClient client, String email, String apikey, String authUrl, int connectionTimeOut) {
        super(client, email, apikey, authUrl, connectionTimeOut);
     }
	
    /**
     * @param email   ucloudbiz ��Ż ����(e-mail) 
     * @param apikey  �ش� ������ API KEY
     * @param authUrl authUrl(https://api.ucloudbiz.olleh.com/storage/v1/auth)
     * @param connectionTimeOut  The connection timeout, in ms.
     */
    public FilesClientExt(String email, String apikey, String authUrl, final int connectionTimeOut) {
		super(email, apikey, authUrl, connectionTimeOut);
    }

    /**
     * @param email   ucloudbiz ��Ż ����(e-mail) 
     * @param apikey  �ش� ������ API KEY
     * @param authUrl authUrl(https://api.ucloudbiz.olleh.com/storage/v1/auth)
     */
    public FilesClientExt(String email, String apikey, String authUrl) {
        super(email, apikey, authUrl);
    }

    /**
     * @param email   Your CloudFiles username
     * @param apikey  Your CloudFiles API Access Key
     */
    public FilesClientExt(String email, String apikey) {
        super(email, apikey);
    }

    public FilesClientExt() {
        super();
    }
    
	 /**
     * <p>
     * ������ ���� ������ Ȯ���Ѵ�.
     * </p>
     *
     * @param containerName
     *            ������ ��ġ�ϰ� �ִ� container �̸�
     * @param objName
     *            ���� �̸�
     * @return �����ϸ� FilesObjectMetaDataExt, �������� ������ null
     *
     */   
     public FilesObjectMetaDataExt objectExists(String containerName, String objName) throws IOException,
     																						 FilesNotFoundException, 
     																						 FilesAuthorizationException,
     																						 FilesInvalidNameException, 
     																						 HttpException {
	    if(isValidContainerName(containerName) && isValidObjectName(objName)) {
	    	if(this.isLoggedin()) {
        		try {
        			return getObjectMetaDataExt(containerName, objName);
        		}
        		catch(FilesNotFoundException fne) {
        			return null;
        		}
    		}
    		else {
       			throw new FilesAuthorizationException("You must be logged in", null, null);
    		}
		}
		else {
			if(!isValidObjectName(objName)) {
				throw new FilesInvalidNameException(objName);
    		}
    		else {
    			throw new FilesInvalidNameException(containerName);
    		}
		}
     }
     
     /**
     * <p>
     * ������ �̸��� �����Ѵ�.
     * </p>
     *
     * @param containerName
     *            ������ ��ġ�ϰ� �ִ� container �̸�
     * @param originName
     *            ���� �����̸�
     * @param targetName
     *            ���� �����̸�
     * @return �����ϸ� true, �����ϸ� false
     *
     */   
     public boolean renameObject(String containerName, String originName, String targetName) throws IOException, 
     																								FilesAuthorizationException,
     																								FilesInvalidNameException,
     																								HttpException {
      	if(isValidContainerName(containerName) && isValidObjectName(originName) && isValidObjectName(targetName)) {																						
	    	if(this.isLoggedin()) {
	    		if(!containerExists(containerName)) {
					throw new FileNotFoundException("Container: " + containerName + " did not exist");    
	    		}
	    		FilesObjectMetaDataExt objectMeta = objectExists(containerName, originName);
	    		if(objectMeta == null) 
				   	throw new FileNotFoundException("File: " + originName + " did not exist");		    
	    		if(originName.equals(targetName)) return true;
	    		String manifest = objectMeta.getObjectManifest();
	    		String contentType = objectMeta.getContentType();
	    		if(manifest == null) {
	    		 	if(copyObject(containerName, originName, containerName, targetName) != null) {
	    		 		deleteObject(containerName, originName);
	    		 		return true;
    			 	}
    			 	return false;
			 	}
			 	else {
				 	if(createManifestObject(containerName, contentType, targetName, manifest, objectMeta.getMetaData())) {
				 		deleteObject(containerName, originName);
				 		return true;
			 		}
				 	return false;
			 	}
			}
			else {
       			throw new FilesAuthorizationException("You must be logged in", null, null);
    		}	
		}
		else {
			if(!isValidObjectName(originName)) {
				throw new FilesInvalidNameException(originName);
    		}
    		else if(!isValidContainerName(containerName)) {
    			throw new FilesInvalidNameException(containerName);
    		}
    		else {
	    		throw new FilesInvalidNameException(targetName);
    		}
		}
    }
    
    /**
    * <p>
    * ������ �̵���Ų��.
    * </p>
    *
    * @param sourceContainer
    *            ������ ��ġ�ϰ� �ִ� container �̸�
    * @param targetContainer
    *            �̵���Ű���� �ϴ� container �̸�
    * @param objName
    *            ���� �̸�
    * @return �����ϸ� true, �����ϸ� false
    *
    */ 
    public boolean moveObject(String sourceContainer, String targetContainer, String objName) throws IOException, 
                                                                                                     HttpException,
                                                                                                     FilesAuthorizationException,
                                                                                                     FilesInvalidNameException {
		if(isValidContainerName(sourceContainer) && isValidContainerName(targetContainer) && isValidObjectName(objName)) {
			if(this.isLoggedin()) {
				if(!containerExists(sourceContainer) || !containerExists(targetContainer)) {
					throw new FileNotFoundException("Both containers" + sourceContainer + " did not exist");    		
				}
				FilesObjectMetaDataExt objectMeta = objectExists(sourceContainer, objName);
				if(objectMeta == null) 
					throw new FileNotFoundException("File: " + objName + " did not exist");	
				if(sourceContainer.equals(targetContainer)) return true;		
				String manifest = objectMeta.getObjectManifest();
	    		String contentType = objectMeta.getContentType();
	    		if(manifest == null) {
	    			if(copyObject(sourceContainer, objName, targetContainer, objName) != null) {
					    deleteObject(sourceContainer, objName);   
					    return true;
	    			}
	    			return false;
    			}
    			else {
	    			if(createManifestObject(targetContainer, contentType, objName, manifest, objectMeta.getMetaData())) {
	    				deleteObject(sourceContainer, objName);
	    				return true;
    				}
    				return false;
    			}
    		}
    		else {
       			throw new FilesAuthorizationException("You must be logged in", null, null);
    		}
		}
		else {
			if(!isValidContainerName(sourceContainer)) {
				throw new FilesInvalidNameException(sourceContainer);
    		}
    		else if(!isValidContainerName(targetContainer)) {
    			throw new FilesInvalidNameException(targetContainer);
    		}
    		else {
	    		throw new FilesInvalidNameException(objName);
    		}			
		}
    }  
    
    /**
    * <p>
    * FilesContainerInfo�� Ȯ���Ͽ� static website ���������� container logging ���� ������ �����´�. 
    * </p>
    *
    * @param containerName
    *            �ش� container �̸�
    * @return FilesContainerInfoExt
    *
    * @see  <A HREF="../../../../com/rackspacecloud/client/cloudfiles/FilesClient.html#getContainerInfo(java.lang.String)"><CODE>FilesClient.getContainerInfo(String)</CODE></A>,
    *       <A HREF="../../../../com/rackspacecloud/client/cloudfiles/FilesContainerInfo.html"><CODE>FilesContainerInfo</CODE></A>,
    *       <A HREF="../../../../com/kt/client/ucloudstorage/FilesContainerInfoExt.html"><CODE>FilesContainerInfoExt</CODE></A>
    */   
    public FilesContainerInfoExt getContainerInfoExt(String containerName) throws IOException, 
    																		      HttpException,
    																		      FilesAuthorizationException,
    																		      FilesNotFoundException,
    																		      FilesInvalidNameException, 
    																		      FilesException {
    	if (this.isLoggedin())
    	{
    		if (isValidContainerName(containerName))
    		{
    			HttpHead method = null;
    			try {
    				method = new HttpHead(storageURL+"/"+sanitizeForURI(containerName));
    				method.getParams().setIntParameter("http.socket.timeout", connectionTimeOut);
    				method.setHeader(FilesConstants.X_AUTH_TOKEN, authToken);
    				FilesResponseExt response = new FilesResponseExt(client.execute(method));

    				if (response.getStatusCode() == HttpStatus.SC_UNAUTHORIZED) {
    					method.removeHeaders(FilesConstants.X_AUTH_TOKEN);
    					if(login()) {
    						method = new HttpHead(storageURL+"/"+sanitizeForURI(containerName));
    						method.getParams().setIntParameter("http.socket.timeout", connectionTimeOut);
    						method.setHeader(FilesConstants.X_AUTH_TOKEN, authToken);
     						response = new FilesResponseExt(client.execute(method));
    					}
    					else {
    						throw new FilesAuthorizationException("Re-login failed", response.getResponseHeaders(), response.getStatusLine());
    					}
    				}

    				if (response.getStatusCode() == HttpStatus.SC_NO_CONTENT)
    				{
    					int objCount = response.getContainerObjectCount();
    					long objSize  = response.getContainerBytesUsed();
    					String webIndex = response.getWebIndex();
    					String webError = response.getWebError();
    					boolean statusListing = response.getWebListings();
    					String webCss = response.getWebCss();
    					boolean statusLogging = response.getContainerLogging();
    					
    					return new FilesContainerInfoExt(containerName, objCount, objSize,
    					                                 webIndex, webError, statusListing, webCss, statusLogging);
    				}
    				else if (response.getStatusCode() == HttpStatus.SC_NOT_FOUND)
    				{
    					throw new FilesNotFoundException("Container not found: " + containerName, response.getResponseHeaders(), response.getStatusLine());
    				}
    				else {
    					throw new FilesException("Unexpected result from server", response.getResponseHeaders(), response.getStatusLine());
    				}
    			}
    			finally {
    				if (method != null) method.abort();
    			}
    		}
    		else {
    			throw new FilesInvalidNameException(containerName);
    		}
    	}
    	else
       		throw new FilesAuthorizationException("You must be logged in", null, null);
    }
    
    /**
    * <p>
    * FilesObjectMetaData�� Ȯ���Ͽ� manifest�� ���� ������ �����´�. 
    * </p>
    *
    * @param containerName
    *            �ش� container �̸�
    * @param objName
    *            �ش� ���� �̸�
	*
    * @return FilesObjectMetaDataExt
    *
    * @see  <A HREF="../../../../com/rackspacecloud/client/cloudfiles/FilesClient.html#getObjectMetaData(java.lang.String, java.lang.String)"><CODE>FilesClient.getObjectMetaData(String, String)</CODE></A>,
    *       <A HREF="../../../../com/rackspacecloud/client/cloudfiles/FilesObjectMetaData.html"><CODE>FilesObjectMetaData</CODE></A>,
    *       <A HREF="../../../../com/kt/client/ucloudstorage/FilesObjectMetaDataExt.html"><CODE>FilesObjectMetaDataExt</CODE></A>
    */ 
    public FilesObjectMetaDataExt getObjectMetaDataExt(String containerName, String objName) throws IOException, 
    																								FilesNotFoundException, 
    																								HttpException, 
    																								FilesAuthorizationException, 
    																								FilesInvalidNameException {
    	FilesObjectMetaDataExt metaData;
    	if (this.isLoggedin())
    	{
    		if (isValidContainerName(containerName) && isValidObjectName(objName))
    		{
    			HttpHead method = new HttpHead(storageURL+"/"+sanitizeForURI(containerName)+"/"+sanitizeForURI(objName));
    			try {
    				method.getParams().setIntParameter("http.socket.timeout", connectionTimeOut);
    				method.setHeader(FilesConstants.X_AUTH_TOKEN, authToken);
    				FilesResponseExt response = new FilesResponseExt(client.execute(method));
   				
           			if (response.getStatusCode() == HttpStatus.SC_UNAUTHORIZED) {
           				method.abort();
        				login();
           				method.getParams().setIntParameter("http.socket.timeout", connectionTimeOut);
        				method.setHeader(FilesConstants.X_AUTH_TOKEN, authToken);
        				response = new FilesResponseExt(client.execute(method));
        			}

           			if (response.getStatusCode() == HttpStatus.SC_NO_CONTENT ||
           			    response.getStatusCode() == HttpStatus.SC_OK)
    				{
    					logger.debug ("Object metadata retreived  : "+objName);
    					String mimeType = response.getContentType();
    					String lastModified = response.getLastModified();
    					String eTag = response.getETag();
    					String contentLength = response.getContentLength();
    					String contentType = response.getContentType();
    					String objectManifest = response.getObjectManifest();

    					metaData = new FilesObjectMetaDataExt(mimeType, contentLength, eTag, lastModified, contentType, objectManifest);

    					Header[] headers = response.getResponseHeaders();
    					HashMap<String,String> headerMap = new HashMap<String, String>();

    					for (Header h: headers)
    					{
    						if ( h.getName().startsWith(FilesConstants.X_OBJECT_META) )
    						{
    							headerMap.put(h.getName().substring(FilesConstants.X_OBJECT_META.length()), unencodeURI(h.getValue()));
    						}
    					}
    					if (headerMap.size() > 0)
    						metaData.setMetaData(headerMap);

    					return metaData;
    				}
    				else if (response.getStatusCode() == HttpStatus.SC_NOT_FOUND)
    				{
    					throw new FilesNotFoundException("Container: " + containerName + " did not have object " + objName, 
								   response.getResponseHeaders(), response.getStatusLine());
    				}
    				else {
    						throw new FilesException("Unexpected Return Code from Server", 
    								response.getResponseHeaders(), response.getStatusLine());
    				}
    			}
    			finally { method.abort(); }
    		}
    		else {
    			if (!isValidObjectName(objName)) 
    				throw new FilesInvalidNameException(objName);
    			else 
    				throw new FilesInvalidNameException(containerName);
    		}
    	}
    	else {
       		throw new FilesAuthorizationException("You must be logged in", null, null);
    	}
    }
    
    /**
    * <p>
    * �ش� container�� static website�� �����Ѵ�. �ڼ��� ���� ������ <A HREF="https://ucloudbiz.olleh.com/manual/ucloud_storage_Static_Web_service_user_guide.pdf"><CODE>ucloud storage Static Web ���� �̿� ���̵�</CODE></A>�� �����Ѵ�.
    * </p>
    *
    * @param containerName
    *            �ش� container �̸�
    * @param config
    *            ��������
    * @return �����ϸ� true
    *
    */   
    public boolean enableStaticWebsiteConfig(String containerName, Map<String, String> config) throws IOException, 
                                                                                                      FilesException,
                                                                                                      FilesNotFoundException, 
                                                                                                      FilesAuthorizationException,
                                                                                                      HttpException {
		config.put(FilesConstantsExt.X_CONTAINER_READ, ".r:*");
	    return this.setContainerMeta(containerName, config);     
    }
    
    /**
    * <p>
    * static website�� ������ container�� ��Ȱ��ȭ��Ų��. �׷��� ���� ���� ������ �������� �ʴ´�. ���� ���ο� 
    * ���������� �����̸� ���� ��� enableStaticWebsiteConfig�� �̿��Ѵ�.
    * �ڼ��� ���� ������ <A HREF="https://ucloudbiz.olleh.com/manual/ucloud_storage_Static_Web_service_user_guide.pdf"><CODE>ucloud storage Static Web ���� �̿� ���̵�</CODE></A>�� �����Ѵ�.
    * </p>
    * @param containerName
    *            �ش� container �̸�
    * @return �����ϸ� true
    *
    */  
    public boolean disableStaticWebsiteConfig(String containerName) throws IOException, 
                                                                           FilesException,
                                                                           FilesNotFoundException, 
                                                                           FilesAuthorizationException,
                                                                           HttpException {
	    return this.setContainerMeta(containerName, FilesConstantsExt.X_CONTAINER_READ, ".r:-*");
	}
	
    /**
    * <p>
    * �ش� container�� ���� ���ٷα׸� �����Ѵ�. �ڼ��� ���� ������ <A HREF="https://ucloudbiz.olleh.com/manual/ucloud_storage_log_save_service_user_guide.pdf"><CODE>ucloud storage �α� ���� ���� �̿� ���̵�</CODE></A>�� �����Ѵ�.
    * </p>
    *
    * @param containerName
    *            �ش� container �̸�
    * @param active
    *            ����(true/false)
    * @return �����ϸ� true
    *
    */
    public boolean setContainerLogging(String containerName, boolean active) throws IOException, FilesException,
                                                                                    FilesNotFoundException, 
                                                                                    FilesAuthorizationException,
                                                                                    HttpException {
		return this.setContainerMeta(containerName, FilesConstantsExt.X_CONTAINER_ACCESS_LOGGING, Boolean.toString(active));   				
    }    
   
	/**
   	* <p>
    * ���� ���ε带 �����Ѵ�. ������ ũ��� 10MB, ���� ó�� ���� ���� 3���� ����ȴ�.
    * ��û ������ ũ�Ⱑ 10MB������ ���, ���Ҿ��ε带 �������� �ʰ� ���� ���� ���ε带 �����Ѵ�. 
    * </p>
    * @param containerName  ������ ������ container �̸�
    * @param obj 		    ���ε� ��� ����
    * @return               �����ϸ� true, ���н� false
    */
    public boolean storeObjectSegmented(String containerName, File obj) throws IOException, HttpException, FilesException {
		return storeObjectSegmentedAs(containerName, obj, null, null, null);
	} 

	/**
   	* <p>
    * ���� ���ε带 �����Ѵ�. ������ ũ��� 10MB, ���� ó�� ���� ���� 3���� ����ȴ�.
    * ��û ������ ũ�Ⱑ 10MB������ ���, ���Ҿ��ε带 �������� �ʰ� ���� ���� ���ε带 �����Ѵ�. 
    * </p>
    * @param containerName  ������ ������ container �̸�
    * @param obj 		    ���ε� ��� ����
    * @param contentType    ������ Ÿ��
    * @return               �����ϸ� true, ���н� false
    */		
    public boolean storeObjectSegmented(String containerName, File obj, String contentType) throws IOException, 
	                                                                                               HttpException, 
	                                                                                               FilesException {
		return storeObjectSegmentedAs(containerName, obj, contentType, null, null);
	} 

	/**
   	* <p>
    * ���������� �̸��� �����Ͽ� ���� ���ε带 �����Ѵ�. ������ ũ��� 10MB, ���� ó�� ���� ���� 3���� ����ȴ�.
    * ��û ������ ũ�Ⱑ 10MB������ ���, ���Ҿ��ε带 �������� �ʰ� ���� ���� ���ε带 �����Ѵ�. 
    * </p>
    * @param containerName  ������ ������ container �̸�
    * @param obj 		    ���ε� ��� ����
    * @param objName	    ����Ǿ��� ���� �̸�
    * @return               �����ϸ� true, ���н� false
    */			
	public boolean storeObjectSegmentedAs(String containerName, File obj, String objName) throws IOException, 
	                                                                                             HttpException, 
	                                                                                             FilesException {
		return storeObjectSegmentedAs(containerName, obj, null, objName, null);
	} 

	/**
   	* <p>
    * ���������� �̸��� �����Ͽ� ���� ���ε带 �����Ѵ�. ������ ũ��� 10MB, ���� ó�� ���� ���� 3���� ����ȴ�.
    * ��û ������ ũ�Ⱑ 10MB������ ���, ���Ҿ��ε带 �������� �ʰ� ���� ���� ���ε带 �����Ѵ�. 
    * </p>
    * @param containerName  ������ ������ container �̸�
    * @param obj 		    ���ε� ��� ����
    * @param contentType    ������ Ÿ��
    * @param objName	    ����Ǿ��� ���� �̸�
    * @return               �����ϸ� true, ���н� false
    */			
	public boolean storeObjectSegmentedAs(String containerName, File obj, 
	 									  String contentType, String objName) throws IOException, 
	                                                                                 HttpException, 
	                                                                                 FilesException {
		return storeObjectSegmentedAs(containerName, obj, contentType, objName, null);
	} 

	/**
   	* <p>
    * ���������� �̸��� �����Ͽ� ���� ���ε带 �����Ѵ�. ������ ũ��� 10MB, ���� ó�� ���� ���� 3���� ����ȴ�.
    * ��û ������ ũ�Ⱑ 10MB������ ���, ���Ҿ��ε带 �������� �ʰ� ���� ���� ���ε带 �����Ѵ�. 
    * </p>
    * @param containerName  ������ ������ container �̸�
    * @param obj 		    ���ε� ��� ����
    * @param contentType    ������ Ÿ��
    * @param objName	    ����Ǿ��� ���� �̸�
    * @param objmeta	    ���Ͽ� ���� metadata
    * @return               �����ϸ� true, ���н� false
    */		
	public boolean storeObjectSegmentedAs(String containerName, File obj, String contentType,
	                                      String objName, Map<String, String> objmeta) throws IOException, 
	                                                                              HttpException, 
	                                                                              FilesException {
		SegmentObject segobj = new SegmentObject(this);
		return segobj.storeObjectSegmentedAs(containerName, obj, contentType, objName, objmeta);
	} 

	/**
   	* <p>
    * ����ũ��� ����ó�� Ƚ���� �����Ͽ� ���� ���ε带 �����Ѵ�.
    * ������ ũ��� 10MB �̻� 500MB ���Ϸ� ������ �����ϰ�, 10MB���� ���� ��� 10MB�� �����Ǹ� 500MB �̻��� ��� 500MB�� �����ȴ�.
    * ���� ó�� Ƚ���� �⺻ 3�̰� �ִ� 10���� ������ �����ϸ�, 10�̻� ��û�� ��� 10���� �����ȴ�. 
    * </p>
    * @param containerName  ������ ������ container �̸�
    * @param obj 		    ���ε� ��� ����
    * @param segmentSize    ������ ũ��(byte)
    * @param concurrent     ���ü��� ���Ҿ��ε� ��
    * @return               �����ϸ� true, ���н� false
    */
	public boolean storeObjectSegmented(String containerName, File obj, 
	                                    long segmentSize, int concurrent) throws IOException, 
	                                                                             HttpException, 
	                                                                             FilesException {
		return storeObjectSegmentedAs(containerName, obj, null, null, null, null, segmentSize, concurrent);
	} 

	/**
   	* <p>
    * ����ũ��� ����ó�� Ƚ���� �����Ͽ� ���� ���ε带 �����Ѵ�.
    * ������ ũ��� 10MB �̻� 500MB ���Ϸ� ������ �����ϰ�, 10MB���� ���� ��� 10MB�� �����Ǹ� 500MB �̻��� ��� 500MB�� �����ȴ�.
    * ���� ó�� Ƚ���� �⺻ 3�̰� �ִ� 10���� ������ �����ϸ�, 10�̻� ��û�� ��� 10���� �����ȴ�. 
    * </p>
    * @param containerName  ������ ������ container �̸�
    * @param obj 		    ���ε� ��� ����
    * @param contentType    ������ Ÿ��
    * @param segmentSize    ������ ũ��(byte)
    * @param concurrent     ���ü��� ���Ҿ��ε� ��
    * @return               �����ϸ� true, ���н� false
    */	
    public boolean storeObjectSegmented(String containerName, File obj, String contentType,
    									long segmentSize, int concurrent) throws IOException, HttpException,
    									 FilesException {
		return storeObjectSegmentedAs(containerName, obj, contentType, null, null, null, segmentSize, concurrent);
	} 
	
	/**
   	* <p>
    * ����ũ��� ����ó�� Ƚ���� �����Ͽ� ������ �����̸����� ���� ���ε带 �����Ѵ�.
    * ������ ũ��� 10MB �̻� 500MB ���Ϸ� ������ �����ϰ�, 10MB���� ���� ��� 10MB�� �����Ǹ� 500MB �̻��� ��� 500MB�� �����ȴ�.
    * ���� ó�� Ƚ���� �⺻ 3�̰� �ִ� 10���� ������ �����ϸ�, 10�̻� ��û�� ��� 10���� �����ȴ�. 
    * </p>
    * @param containerName  ������ ������ container �̸�
    * @param obj 		    ���ε� ��� ����
    * @param objName	    ����Ǿ��� ���� �̸�
    * @param segmentSize    ������ ũ��(byte)
    * @param concurrent     ���ü��� ���Ҿ��ε� ��
    * @return               �����ϸ� true, ���н� false
    */	
	public boolean storeObjectSegmentedAs(String containerName, File obj, String objName, 
										  long segmentSize, int concurrent) throws IOException, 
	                                                                               HttpException, 
	                                                                               FilesException {
		return storeObjectSegmentedAs(containerName, obj, null, objName, null, null, segmentSize, concurrent);
	} 
	
	/**
   	* <p>
    * ����ũ��� ����ó�� Ƚ���� �����Ͽ� ������ �����̸����� ���� ���ε带 �����Ѵ�.
    * ������ ũ��� 10MB �̻� 500MB ���Ϸ� ������ �����ϰ�, 10MB���� ���� ��� 10MB�� �����Ǹ� 500MB �̻��� ��� 500MB�� �����ȴ�.
    * ���� ó�� Ƚ���� �⺻ 3�̰� �ִ� 10���� ������ �����ϸ�, 10�̻� ��û�� ��� 10���� �����ȴ�. 
    * </p>
    * @param containerName  ������ ������ container �̸�
    * @param obj 		    ���ε� ��� ����
    * @param contentType    ������ Ÿ��
    * @param objName	    ����Ǿ��� ���� �̸�
    * @param segmentSize    ������ ũ��(byte)
    * @param concurrent     ���ü��� ���Ҿ��ε� ��
    * @return               �����ϸ� true, ���н� false
    */
	public boolean storeObjectSegmentedAs(String containerName, File obj, 
	 									  String contentType, String objName, 
	 									  long segmentSize, int concurrent) throws IOException, HttpException, FilesException {
		return storeObjectSegmentedAs(containerName, obj, contentType, objName, null, null, segmentSize, concurrent);
	} 
	
	/**
   	* <p>
    * ����ũ��� ����ó�� Ƚ���� �����Ͽ� ������ �����̸����� metadata�� �߰��Ͽ� ���� ���ε带 �����Ѵ�.
    * ������ ũ��� 10MB �̻� 500MB ���Ϸ� ������ �����ϰ�, 10MB���� ���� ��� 10MB�� �����Ǹ� 500MB �̻��� ��� 500MB�� �����ȴ�.
    * ���� ó�� Ƚ���� �⺻ 3�̰� �ִ� 10���� ������ �����ϸ�, 10�̻� ��û�� ��� 10���� �����ȴ�. 
    * </p>
    * @param containerName  ������ ������ container �̸�
    * @param obj 		    ���ε� ��� ����
    * @param contentType    ������ Ÿ��
    * @param objName	    ����Ǿ��� ���� �̸�
    * @param objmeta	    ���Ͽ� ���� metadata
    * @param segmentSize    ������ ũ��(byte)
    * @param concurrent     ���ü��� ���Ҿ��ε� ��
    * @return               �����ϸ� true, ���н� false
    */
	public boolean storeObjectSegmentedAs(String containerName, File obj, String contentType,
	                                      String objName, Map<String, String> objmeta,
	                                      long segmentSize, int concurrent) throws IOException, HttpException, FilesException {
		return storeObjectSegmentedAs(containerName, obj, contentType, objName, objmeta, null, segmentSize, concurrent);
	} 
	
	/**
   	* <p>
    * ����ũ��� ����ó�� Ƚ���� �����Ͽ� ������ �����̸����� metadata�� �߰��Ͽ� ���� ���ε带 �����Ѵ�.
    * ������ ũ��� 10MB �̻� 500MB ���Ϸ� ������ �����ϰ�, 10MB���� ���� ��� 10MB�� �����Ǹ� 500MB �̻��� ��� 500MB�� �����ȴ�.
    * ���� ó�� Ƚ���� �⺻ 3�̰� �ִ� 10���� ������ �����ϸ�, 10�̻� ��û�� ��� 10���� �����ȴ�. 
    * </p>
    * @param containerName  ������ ������ container �̸�
    * @param obj 		    ���ε� ��� ����
    * @param contentType    ������ Ÿ��
    * @param objName	    ����Ǿ��� ���� �̸�
    * @param objmeta	    ���Ͽ� ���� metadata
    * @param callback	    �ݹ� object
    * @param segmentSize    ������ ũ��(byte)
    * @param concurrent     ���ü��� ���Ҿ��ε� ��
    * @return               �����ϸ� true, ���н� false
    */
	public boolean storeObjectSegmentedAs(String containerName, File obj, String contentType,
	                                      String objName, Map<String, String> objmeta, 
	                                      IFilesTransferCallback callback, 
	                                      long segmentSize, int concurrent) throws IOException, 
	                                                                               HttpException, 
	                                                                               FilesException {
	   	if(concurrent < 2) concurrent = 3;
		SegmentObject segobj = new SegmentObject(this, segmentSize, concurrent);
		return segobj.storeObjectSegmentedAs(containerName, obj, contentType, objName, objmeta, callback);
	} 
	
	/**
   	* <p>
    * ���� ���ε�� manifest ���� �� ��� ���ҵ��� �����Ѵ�.
    * </p>
    * @param containerName  ������ ������ container �̸�
    * @param objName	    ���� ���� �̸�
    * @return               �����ϸ� true, ���н� false
    */
	public boolean deleteSegmentsManifest(String containerName, String objName) throws IOException, 
																					   HttpException, FilesException {
		SegmentObject segobj = new SegmentObject(this, 5);
		return segobj.deleteSegmentsManifest(containerName, objName);
	} 
	
	/**
   	* <p>
    * ���Ͽ� ���� TempURL�� �����Ѵ�. TempURL�� ��ȿ�ð��� ��(sec) ������ �Է��ؾ� �ϸ�
    * 1���� 60, 1�ð��� 3600, 1���� 86,400 ���� �� ������ ȯ���Ͽ� ó���ؾ� �Ѵ�. 
    * </p>
    * @param containerName  ������ ������ container �̸�
    * @param objName	    ��� ����
    * @param lifetime	    TempURL�� ��ȿ�ð�(��)
    * @return               ������ ���Ͽ� ���� Temp URL�� �����Ѵ�. 
    */
	public String getObejctTempURL(String containerName, String objName, long lifetime) throws FileNotFoundException,
																							   IOException, FilesException, 
																							   HttpException {
		String key = getAccountMeta(FilesConstantsExt.X_ACCOUNT_TEMPURL_KEY);
		System.out.println("TempURL Key : " + key); // test
		String storageURL = getStorageURL();
		if(key == null) {
			throw new FileNotFoundException("Account Temp URL Key not found!");	
		}
		else {
			if(this.objectExists(containerName, objName) == null) {
				throw new FileNotFoundException("Container or Object not found!");
			}
			else {
				long now = System.currentTimeMillis()/1000;
				long expires = now + lifetime;
				String[] arr = storageURL.split("/");
				String account = arr[arr.length-1];
				String body = "GET\n" + String.valueOf(expires) + "\n/v1/" + account + "/" + containerName + "/" + objName;
				return storageURL + "/" + containerName + "/" + objName + "?temp_url_sig=" 
				       + HmacDigest.hmacDigest(body, key, "HmacSHA1") + "&temp_url_expires=" + expires;
			}
		}
	}
	
	/**
   	* <p>
    * TempURL ������ ���� key �����Ѵ�. 
    * </p>
    * @param key	TempURL ������ ���� key ��
    * @return       �����ϸ� true, ���н� false 
    */
	public boolean setTempURLKey(String key) throws IOException, FilesException,
                                                    FilesAuthorizationException, HttpException {
		return this.setAccountMeta(FilesConstantsExt.X_ACCOUNT_TEMPURL_KEY, key);	
	}


//////////////////////////////////////////////	
//////// private methods (2014.02.21)/////////
//////////////////////////////////////////////	

	private boolean setContainerMeta(String containerName, 
	                                 String metaKey, 
	                                 String metaValue) throws IOException, FilesException,
                                                              FilesInvalidNameException,
                                                              FilesNotFoundException, 
                                                              FilesAuthorizationException, HttpException {
		if (this.isLoggedin()) {
    		if (isValidContainerName(containerName)) {
    			HttpPost method = null;
    			
    			try {
    				method = new HttpPost(storageURL+"/"+sanitizeForURI(containerName));
    				method.getParams().setIntParameter("http.socket.timeout", connectionTimeOut);
    				method.setHeader(FilesConstants.X_AUTH_TOKEN, authToken); 
    				method.setHeader(metaKey, metaValue);   				
    				FilesResponseExt response = new FilesResponseExt(client.execute(method));

    				if (response.getStatusCode() == HttpStatus.SC_UNAUTHORIZED) {
    					method.removeHeaders(FilesConstants.X_AUTH_TOKEN);
    					if(login()) {
    						method = new HttpPost(storageURL+"/"+sanitizeForURI(containerName));
    						method.getParams().setIntParameter("http.socket.timeout", connectionTimeOut);
    						method.setHeader(FilesConstants.X_AUTH_TOKEN, authToken);
    						method.setHeader(metaKey, metaValue); 
     						response = new FilesResponseExt(client.execute(method));
    					}
    					else 
    						throw new FilesAuthorizationException("Re-login failed", response.getResponseHeaders(), response.getStatusLine());
					}
					
    				if (response.getStatusCode() == HttpStatus.SC_NO_CONTENT) 
    					return true;
    				else if (response.getStatusCode() == HttpStatus.SC_NOT_FOUND) 
    					throw new FilesNotFoundException("Container not found: " + containerName, response.getResponseHeaders(), response.getStatusLine());
    				else 
    					throw new FilesException("Unexpected result from server", response.getResponseHeaders(), response.getStatusLine());
    			}
    			finally { if (method != null) method.abort(); }
    		}
    		else 
    			throw new FilesInvalidNameException(containerName);
    	}
    	else
       		throw new FilesAuthorizationException("You must be logged in", null, null);
	}
	
	private boolean setContainerMeta(String containerName, Map<String, String> config) throws IOException, 
                                                                                              FilesException,
                                                                                              FilesInvalidNameException,
                                                                                              FilesNotFoundException, 
                                                                                              FilesAuthorizationException,
                                                                                              HttpException {
	    if (this.isLoggedin()) {
    		if (isValidContainerName(containerName)) {
    			HttpPost method = null;
    			if(config == null) { return true; }
    			Iterator<String> i = config.keySet().iterator();
    			String key = null;
    			String value = null;
    			FilesResponseExt response = null;
    			
    			try {
    				method = new HttpPost(storageURL+"/"+sanitizeForURI(containerName));
    				method.getParams().setIntParameter("http.socket.timeout", connectionTimeOut);
    				method.setHeader(FilesConstants.X_AUTH_TOKEN, authToken);  
    				method.setHeader(FilesConstantsExt.X_CONTAINER_READ, ".r:*");  				
    				while(i.hasNext()) {
	    				key = i.next();
	    				method.setHeader(key, config.get(key));	    				
    				}
    				
    				response = new FilesResponseExt(client.execute(method));
                    
    				if (response.getStatusCode() == HttpStatus.SC_UNAUTHORIZED) {
    					method.removeHeaders(FilesConstants.X_AUTH_TOKEN);
    					if(login()) {
    						method = new HttpPost(storageURL+"/"+sanitizeForURI(containerName));
    						Iterator<String> j = config.keySet().iterator();
    						method.getParams().setIntParameter("http.socket.timeout", connectionTimeOut);
    						method.setHeader(FilesConstants.X_AUTH_TOKEN, authToken);
    						while(j.hasNext()) {
	    						key = j.next();
	    						method.setHeader(key, config.get(key));	    				
    						}
     						response = new FilesResponseExt(client.execute(method));
    					}
    					else 
    						throw new FilesAuthorizationException("Re-login failed", response.getResponseHeaders(), response.getStatusLine());
    				}

    				if (response.getStatusCode() == HttpStatus.SC_NO_CONTENT)
    					return true;
    				else if (response.getStatusCode() == HttpStatus.SC_NOT_FOUND)
    					throw new FilesNotFoundException("Container not found: " + containerName, response.getResponseHeaders(), response.getStatusLine());
    				else 
    					throw new FilesException("Unexpected result from server", response.getResponseHeaders(), response.getStatusLine());
    			}
    			finally { if (method != null) method.abort(); }
    		}
    		else
    			throw new FilesInvalidNameException(containerName);
    	}
    	else
       		throw new FilesAuthorizationException("You must be logged in", null, null);
    }
	
	private boolean setAccountMeta(String metaKey, String metaValue) throws IOException, 
                                                                            FilesException,
                                                                            FilesAuthorizationException,
                                                                            HttpException {
		if (this.isLoggedin()) {
    		HttpPost method = null;
    		try {
    			method = new HttpPost(storageURL);
    			method.getParams().setIntParameter("http.socket.timeout", connectionTimeOut);
    			method.setHeader(FilesConstants.X_AUTH_TOKEN, authToken); 
    			method.setHeader(metaKey, metaValue);   				
    			FilesResponseExt response = new FilesResponseExt(client.execute(method));

    			if (response.getStatusCode() == HttpStatus.SC_UNAUTHORIZED) {
    				method.removeHeaders(FilesConstants.X_AUTH_TOKEN);
    				if(login()) {
    					method = new HttpPost(storageURL);
    					method.getParams().setIntParameter("http.socket.timeout", connectionTimeOut);
    					method.setHeader(FilesConstants.X_AUTH_TOKEN, authToken);
    					method.setHeader(metaKey, metaValue); 
     					response = new FilesResponseExt(client.execute(method));
    				}
    				else 
    					throw new FilesAuthorizationException("Re-login failed", response.getResponseHeaders(), response.getStatusLine());
    			}

    			if (response.getStatusCode() == HttpStatus.SC_NO_CONTENT) 
    				return true;
    			else 
    				throw new FilesException("Unexpected result from server", response.getResponseHeaders(), response.getStatusLine());
    		}
    		finally { if (method != null) method.abort(); }
    	}
    	else
       		throw new FilesAuthorizationException("You must be logged in", null, null);
	}
	
	private String getAccountMeta(String metaKey) throws IOException, FilesException,
                                                         FilesAuthorizationException, HttpException {                 
	//	FilesResponseExt response = null;
		if (this.isLoggedin()) {
    		HttpHead method = new HttpHead(storageURL);
    		try {
    			method.getParams().setIntParameter("http.socket.timeout", connectionTimeOut);
    			method.setHeader(FilesConstants.X_AUTH_TOKEN, authToken);
    			FilesResponseExt response = new FilesResponseExt(client.execute(method));
   			
           		if (response.getStatusCode() == HttpStatus.SC_UNAUTHORIZED) {
           			method.abort();
        			login();
           			method.getParams().setIntParameter("http.socket.timeout", connectionTimeOut);
        			method.setHeader(FilesConstants.X_AUTH_TOKEN, authToken);
        			response = new FilesResponseExt(client.execute(method));
        		}

           		if (response.getStatusCode() == HttpStatus.SC_NO_CONTENT) {
           			Header header = response.getResponseHeader(metaKey);
           			if(header != null) return header.getValue();
    				else return null;
				}
    			else 
    				throw new FilesException("Unexpected result from server", response.getResponseHeaders(), response.getStatusLine());
			}
			finally { if (method != null) method.abort(); }
    	}
    	else
       		throw new FilesAuthorizationException("You must be logged in", null, null);
	}

    private boolean setAccountMeta(Map<String, String> config) throws IOException, FilesException,
                                                                      FilesAuthorizationException, HttpException {
	    if (this.isLoggedin()) {
    		HttpPost method = null;
    		if(config == null) return true;
    		Iterator<String> i = config.keySet().iterator();
    		String key = null;
    		String value = null;
    		FilesResponseExt response = null;
    		
    		try {
    			method = new HttpPost(storageURL);
    			method.getParams().setIntParameter("http.socket.timeout", connectionTimeOut);
    			method.setHeader(FilesConstants.X_AUTH_TOKEN, authToken);  
    			while(i.hasNext()) {
	    			key = i.next();
	    			method.setHeader(key, config.get(key));	    				
    			}
    			
    			response = new FilesResponseExt(client.execute(method));
                   
    			if (response.getStatusCode() == HttpStatus.SC_UNAUTHORIZED) {
    				method.removeHeaders(FilesConstants.X_AUTH_TOKEN);
    				if(login()) {
    					method = new HttpPost(storageURL);
    					Iterator<String> j = config.keySet().iterator();
    					method.getParams().setIntParameter("http.socket.timeout", connectionTimeOut);
    					method.setHeader(FilesConstants.X_AUTH_TOKEN, authToken);
    					while(j.hasNext()) {
	    					key = j.next();
	    					method.setHeader(key, config.get(key));	    				
    					}
     					response = new FilesResponseExt(client.execute(method));
    				}
    				else 
    					throw new FilesAuthorizationException("Re-login failed", response.getResponseHeaders(), response.getStatusLine());
    			}

    			if (response.getStatusCode() == HttpStatus.SC_NO_CONTENT)
    				return true;
    			else 
    				throw new FilesException("Unexpected result from server", response.getResponseHeaders(), response.getStatusLine());
    		}
    		finally { if (method != null) method.abort(); }
		}
    	else
       		throw new FilesAuthorizationException("You must be logged in", null, null);
    }
}
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
 * 이 클래스는 FilesClient의 확장 클래스로 추가적인 API를 갖고 있다.
 * </p>
 * <p>
 * 추가로 제공하는 API는 파일 이동, 파일 이름 변경하기를 포함하여 static website 및 container logging
 * 구성 및 상태 조회 기능을 제공한다. 이 클래스는 FilesClient의 모든 기능을 포함하고 있으며
 * FilesClient를 사용할 경우 추가로 제공되는 API는 사용할 수 없다.
 * 분할 업로드를 요청할 경우, 분할의 크기와 동시 업로드 수를 지정하지 않을 경우
 * 기본적으로 분할의 크기는 10MB, 동시 업로드 처리 수는 5로 설정되어 수행한다.
 * 지정한 분할의 크기보다 작은 파일을 분할업로드로 요청할 경우, FilesClientExt.storeObject로 처리한다.  
 * </p>
 * 
 * <p>
 * <blockquote><pre>
 * // Native FilesClient Class 사용 
 * FilesClient = new FilesClient("email", "DA0NjEzMjIzODIzMzIyN", "https://api.ucloudbiz.olleh.com/storage/v1/auth", 3000); 
 * 
 * // FilesClientExt Class를 사용할 때
 * FilesClientExt = new FilesClientExt("email", "DA0NjEzMjIzODIzMzIyN", "https://api.ucloudbiz.olleh.com/storage/v1/auth", 3000); 
 * </pre></blockquote>
 * </p>
 * 
 * @see	com.rackspacecloud.client.cloudfiles.FilesClient
 * 
 * 
 * @author KT 클라우드스토리지팀
 */
 
public class FilesClientExt extends FilesClient {

    /**
     * @param client  The HttpClient to talk to KT ucloud storage service
     * @param email   ucloudbiz 포탈 계정(e-mail) 
     * @param apikey  해당 계정의 API KEY
     * @param authUrl authUrl(https://api.ucloudbiz.olleh.com/storage/v1/auth)
     * @param connectionTimeOut  The connection timeout, in ms.
     */
    public FilesClientExt(HttpClient client, String email, String apikey, String authUrl, int connectionTimeOut) {
        super(client, email, apikey, authUrl, connectionTimeOut);
     }
	
    /**
     * @param email   ucloudbiz 포탈 계정(e-mail) 
     * @param apikey  해당 계정의 API KEY
     * @param authUrl authUrl(https://api.ucloudbiz.olleh.com/storage/v1/auth)
     * @param connectionTimeOut  The connection timeout, in ms.
     */
    public FilesClientExt(String email, String apikey, String authUrl, final int connectionTimeOut) {
		super(email, apikey, authUrl, connectionTimeOut);
    }

    /**
     * @param email   ucloudbiz 포탈 계정(e-mail) 
     * @param apikey  해당 계정의 API KEY
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
     * 파일의 존재 유무를 확인한다.
     * </p>
     *
     * @param containerName
     *            파일이 위치하고 있는 container 이름
     * @param objName
     *            파일 이름
     * @return 존재하면 FilesObjectMetaDataExt, 존재하지 않으면 null
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
     * 파일의 이름을 변경한다.
     * </p>
     *
     * @param containerName
     *            파일이 위치하고 있는 container 이름
     * @param originName
     *            원본 파일이름
     * @param targetName
     *            변경 파일이름
     * @return 성공하면 true, 실패하면 false
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
    * 파일을 이동시킨다.
    * </p>
    *
    * @param sourceContainer
    *            파일이 위치하고 있는 container 이름
    * @param targetContainer
    *            이동시키고자 하는 container 이름
    * @param objName
    *            파일 이름
    * @return 성공하면 true, 실패하면 false
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
    * FilesContainerInfo를 확장하여 static website 구성정보와 container logging 상태 정보를 가져온다. 
    * </p>
    *
    * @param containerName
    *            해당 container 이름
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
    * FilesObjectMetaData를 확장하여 manifest에 대한 정보를 가져온다. 
    * </p>
    *
    * @param containerName
    *            해당 container 이름
    * @param objName
    *            해당 파일 이름
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
    * 해당 container를 static website로 구성한다. 자세한 서비스 설명은 <A HREF="https://ucloudbiz.olleh.com/manual/ucloud_storage_Static_Web_service_user_guide.pdf"><CODE>ucloud storage Static Web 서비스 이용 가이드</CODE></A>를 참고한다.
    * </p>
    *
    * @param containerName
    *            해당 container 이름
    * @param config
    *            구성정보
    * @return 성공하면 true
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
    * static website로 구성된 container를 불활성화시킨다. 그러나 이전 구성 정보는 삭제되지 않는다. 만일 새로운 
    * 구성정보로 업데이를 원할 경우 enableStaticWebsiteConfig를 이용한다.
    * 자세한 서비스 설명은 <A HREF="https://ucloudbiz.olleh.com/manual/ucloud_storage_Static_Web_service_user_guide.pdf"><CODE>ucloud storage Static Web 서비스 이용 가이드</CODE></A>를 참고한다.
    * </p>
    * @param containerName
    *            해당 container 이름
    * @return 성공하면 true
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
    * 해당 container에 대한 접근로그를 저장한다. 자세한 서비스 설명은 <A HREF="https://ucloudbiz.olleh.com/manual/ucloud_storage_log_save_service_user_guide.pdf"><CODE>ucloud storage 로그 저장 서비스 이용 가이드</CODE></A>를 참고한다.
    * </p>
    *
    * @param containerName
    *            해당 container 이름
    * @param active
    *            설정(true/false)
    * @return 성공하면 true
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
    * 분할 업로드를 수행한다. 분할의 크기는 10MB, 동시 처리 세션 수는 3으로 수행된다.
    * 요청 파일의 크기가 10MB이하인 경우, 분할업로드를 수행하지 않고 단일 파일 업로드를 수행한다. 
    * </p>
    * @param containerName  파일을 저장할 container 이름
    * @param obj 		    업로드 대상 파일
    * @return               성공하면 true, 실패시 false
    */
    public boolean storeObjectSegmented(String containerName, File obj) throws IOException, HttpException, FilesException {
		return storeObjectSegmentedAs(containerName, obj, null, null, null);
	} 

	/**
   	* <p>
    * 분할 업로드를 수행한다. 분할의 크기는 10MB, 동시 처리 세션 수는 3으로 수행된다.
    * 요청 파일의 크기가 10MB이하인 경우, 분할업로드를 수행하지 않고 단일 파일 업로드를 수행한다. 
    * </p>
    * @param containerName  파일을 저장할 container 이름
    * @param obj 		    업로드 대상 파일
    * @param contentType    컨텐츠 타입
    * @return               성공하면 true, 실패시 false
    */		
    public boolean storeObjectSegmented(String containerName, File obj, String contentType) throws IOException, 
	                                                                                               HttpException, 
	                                                                                               FilesException {
		return storeObjectSegmentedAs(containerName, obj, contentType, null, null);
	} 

	/**
   	* <p>
    * 저장파일의 이름을 지정하여 분할 업로드를 수행한다. 분할의 크기는 10MB, 동시 처리 세션 수는 3으로 수행된다.
    * 요청 파일의 크기가 10MB이하인 경우, 분할업로드를 수행하지 않고 단일 파일 업로드를 수행한다. 
    * </p>
    * @param containerName  파일을 저장할 container 이름
    * @param obj 		    업로드 대상 파일
    * @param objName	    저장되어질 파일 이름
    * @return               성공하면 true, 실패시 false
    */			
	public boolean storeObjectSegmentedAs(String containerName, File obj, String objName) throws IOException, 
	                                                                                             HttpException, 
	                                                                                             FilesException {
		return storeObjectSegmentedAs(containerName, obj, null, objName, null);
	} 

	/**
   	* <p>
    * 저장파일의 이름을 지정하여 분할 업로드를 수행한다. 분할의 크기는 10MB, 동시 처리 세션 수는 3으로 수행된다.
    * 요청 파일의 크기가 10MB이하인 경우, 분할업로드를 수행하지 않고 단일 파일 업로드를 수행한다. 
    * </p>
    * @param containerName  파일을 저장할 container 이름
    * @param obj 		    업로드 대상 파일
    * @param contentType    컨텐츠 타입
    * @param objName	    저장되어질 파일 이름
    * @return               성공하면 true, 실패시 false
    */			
	public boolean storeObjectSegmentedAs(String containerName, File obj, 
	 									  String contentType, String objName) throws IOException, 
	                                                                                 HttpException, 
	                                                                                 FilesException {
		return storeObjectSegmentedAs(containerName, obj, contentType, objName, null);
	} 

	/**
   	* <p>
    * 저장파일의 이름을 지정하여 분할 업로드를 수행한다. 분할의 크기는 10MB, 동시 처리 세션 수는 3으로 수행된다.
    * 요청 파일의 크기가 10MB이하인 경우, 분할업로드를 수행하지 않고 단일 파일 업로드를 수행한다. 
    * </p>
    * @param containerName  파일을 저장할 container 이름
    * @param obj 		    업로드 대상 파일
    * @param contentType    컨텐츠 타입
    * @param objName	    저장되어질 파일 이름
    * @param objmeta	    파일에 대한 metadata
    * @return               성공하면 true, 실패시 false
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
    * 분할크기와 동시처리 횟수를 선택하여 분할 업로드를 수행한다.
    * 분할의 크기는 10MB 이상 500MB 이하로 설정이 가능하고, 10MB보다 작을 경우 10MB로 설정되며 500MB 이상인 경우 500MB로 설정된다.
    * 동시 처리 횟수는 기본 3이고 최대 10까지 설정이 가능하며, 10이상 요청된 경우 10으로 설정된다. 
    * </p>
    * @param containerName  파일을 저장할 container 이름
    * @param obj 		    업로드 대상 파일
    * @param segmentSize    분할의 크기(byte)
    * @param concurrent     동시수행 분할업로드 수
    * @return               성공하면 true, 실패시 false
    */
	public boolean storeObjectSegmented(String containerName, File obj, 
	                                    long segmentSize, int concurrent) throws IOException, 
	                                                                             HttpException, 
	                                                                             FilesException {
		return storeObjectSegmentedAs(containerName, obj, null, null, null, null, segmentSize, concurrent);
	} 

	/**
   	* <p>
    * 분할크기와 동시처리 횟수를 선택하여 분할 업로드를 수행한다.
    * 분할의 크기는 10MB 이상 500MB 이하로 설정이 가능하고, 10MB보다 작을 경우 10MB로 설정되며 500MB 이상인 경우 500MB로 설정된다.
    * 동시 처리 횟수는 기본 3이고 최대 10까지 설정이 가능하며, 10이상 요청된 경우 10으로 설정된다. 
    * </p>
    * @param containerName  파일을 저장할 container 이름
    * @param obj 		    업로드 대상 파일
    * @param contentType    컨텐츠 타입
    * @param segmentSize    분할의 크기(byte)
    * @param concurrent     동시수행 분할업로드 수
    * @return               성공하면 true, 실패시 false
    */	
    public boolean storeObjectSegmented(String containerName, File obj, String contentType,
    									long segmentSize, int concurrent) throws IOException, HttpException,
    									 FilesException {
		return storeObjectSegmentedAs(containerName, obj, contentType, null, null, null, segmentSize, concurrent);
	} 
	
	/**
   	* <p>
    * 분할크기와 동시처리 횟수를 선택하여 지정된 파일이름으로 분할 업로드를 수행한다.
    * 분할의 크기는 10MB 이상 500MB 이하로 설정이 가능하고, 10MB보다 작을 경우 10MB로 설정되며 500MB 이상인 경우 500MB로 설정된다.
    * 동시 처리 횟수는 기본 3이고 최대 10까지 설정이 가능하며, 10이상 요청된 경우 10으로 설정된다. 
    * </p>
    * @param containerName  파일을 저장할 container 이름
    * @param obj 		    업로드 대상 파일
    * @param objName	    저장되어질 파일 이름
    * @param segmentSize    분할의 크기(byte)
    * @param concurrent     동시수행 분할업로드 수
    * @return               성공하면 true, 실패시 false
    */	
	public boolean storeObjectSegmentedAs(String containerName, File obj, String objName, 
										  long segmentSize, int concurrent) throws IOException, 
	                                                                               HttpException, 
	                                                                               FilesException {
		return storeObjectSegmentedAs(containerName, obj, null, objName, null, null, segmentSize, concurrent);
	} 
	
	/**
   	* <p>
    * 분할크기와 동시처리 횟수를 선택하여 지정된 파일이름으로 분할 업로드를 수행한다.
    * 분할의 크기는 10MB 이상 500MB 이하로 설정이 가능하고, 10MB보다 작을 경우 10MB로 설정되며 500MB 이상인 경우 500MB로 설정된다.
    * 동시 처리 횟수는 기본 3이고 최대 10까지 설정이 가능하며, 10이상 요청된 경우 10으로 설정된다. 
    * </p>
    * @param containerName  파일을 저장할 container 이름
    * @param obj 		    업로드 대상 파일
    * @param contentType    컨텐츠 타입
    * @param objName	    저장되어질 파일 이름
    * @param segmentSize    분할의 크기(byte)
    * @param concurrent     동시수행 분할업로드 수
    * @return               성공하면 true, 실패시 false
    */
	public boolean storeObjectSegmentedAs(String containerName, File obj, 
	 									  String contentType, String objName, 
	 									  long segmentSize, int concurrent) throws IOException, HttpException, FilesException {
		return storeObjectSegmentedAs(containerName, obj, contentType, objName, null, null, segmentSize, concurrent);
	} 
	
	/**
   	* <p>
    * 분할크기와 동시처리 횟수를 선택하여 지정된 파일이름으로 metadata를 추가하여 분할 업로드를 수행한다.
    * 분할의 크기는 10MB 이상 500MB 이하로 설정이 가능하고, 10MB보다 작을 경우 10MB로 설정되며 500MB 이상인 경우 500MB로 설정된다.
    * 동시 처리 횟수는 기본 3이고 최대 10까지 설정이 가능하며, 10이상 요청된 경우 10으로 설정된다. 
    * </p>
    * @param containerName  파일을 저장할 container 이름
    * @param obj 		    업로드 대상 파일
    * @param contentType    컨텐츠 타입
    * @param objName	    저장되어질 파일 이름
    * @param objmeta	    파일에 대한 metadata
    * @param segmentSize    분할의 크기(byte)
    * @param concurrent     동시수행 분할업로드 수
    * @return               성공하면 true, 실패시 false
    */
	public boolean storeObjectSegmentedAs(String containerName, File obj, String contentType,
	                                      String objName, Map<String, String> objmeta,
	                                      long segmentSize, int concurrent) throws IOException, HttpException, FilesException {
		return storeObjectSegmentedAs(containerName, obj, contentType, objName, objmeta, null, segmentSize, concurrent);
	} 
	
	/**
   	* <p>
    * 분할크기와 동시처리 횟수를 선택하여 지정된 파일이름으로 metadata를 추가하여 분할 업로드를 수행한다.
    * 분할의 크기는 10MB 이상 500MB 이하로 설정이 가능하고, 10MB보다 작을 경우 10MB로 설정되며 500MB 이상인 경우 500MB로 설정된다.
    * 동시 처리 횟수는 기본 3이고 최대 10까지 설정이 가능하며, 10이상 요청된 경우 10으로 설정된다. 
    * </p>
    * @param containerName  파일을 저장할 container 이름
    * @param obj 		    업로드 대상 파일
    * @param contentType    컨텐츠 타입
    * @param objName	    저장되어질 파일 이름
    * @param objmeta	    파일에 대한 metadata
    * @param callback	    콜백 object
    * @param segmentSize    분할의 크기(byte)
    * @param concurrent     동시수행 분할업로드 수
    * @return               성공하면 true, 실패시 false
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
    * 분할 업로드된 manifest 파일 및 모든 분할들을 삭제한다.
    * </p>
    * @param containerName  파일을 저장할 container 이름
    * @param objName	    저장 파일 이름
    * @return               성공하면 true, 실패시 false
    */
	public boolean deleteSegmentsManifest(String containerName, String objName) throws IOException, 
																					   HttpException, FilesException {
		SegmentObject segobj = new SegmentObject(this, 5);
		return segobj.deleteSegmentsManifest(containerName, objName);
	} 
	
	/**
   	* <p>
    * 파일에 대한 TempURL을 생성한다. TempURL의 유효시간은 초(sec) 단위로 입력해야 하며
    * 1분은 60, 1시간은 3600, 1일은 86,400 같이 초 단위로 환산하여 처리해야 한다. 
    * </p>
    * @param containerName  파일을 저장할 container 이름
    * @param objName	    대상 파일
    * @param lifetime	    TempURL의 유효시간(초)
    * @return               성공시 파일에 대한 Temp URL을 전달한다. 
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
    * TempURL 생성을 위한 key 설정한다. 
    * </p>
    * @param key	TempURL 생성을 위한 key 값
    * @return       성공하면 true, 실패시 false 
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
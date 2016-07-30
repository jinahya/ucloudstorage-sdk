package com.olleh.ucloudbiz.ucloudstorage.utils;

import java.io.File;
import java.io.InputStream;
import java.io.FileNotFoundException;
import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.http.HttpException;
import com.olleh.ucloudbiz.ucloudstorage.FilesClientExt;
import com.olleh.ucloudbiz.ucloudstorage.FilesObjectMetaDataExt;
import com.rackspacecloud.client.cloudfiles.FilesObject;
import com.rackspacecloud.client.cloudfiles.FilesException;
import com.rackspacecloud.client.cloudfiles.FilesNotFoundException;
import com.rackspacecloud.client.cloudfiles.FilesInvalidNameException;
import com.rackspacecloud.client.cloudfiles.FilesAuthorizationException;
import com.rackspacecloud.client.cloudfiles.IFilesTransferCallback; 

public class SegmentObject {
	private static final long minSegmentSize = 10485760L;
	private static final long maxSegmentSize = 524288000L;
	protected long segmentSize;
	protected FilesClientExt fclient;
	protected TicketPool tp;
	
	public SegmentObject(FilesClientExt fclient) {
		this.fclient = fclient;		
		tp = TicketPool.getInstance();
	}
	
	public SegmentObject(FilesClientExt fclient, int concurrent) {
		this.fclient = fclient;		
		tp = TicketPool.getInstance(concurrent);
	}
	
	public SegmentObject(FilesClientExt fclient, long requestSize, int concurrent) {
		this.fclient = fclient;	
		if((requestSize > 10485760L) && (requestSize < 524288000L)) 
			this.segmentSize = requestSize;
		else if(requestSize <= 10485760L) 
			this.segmentSize = 10485760L;
		else //(requestSize => 524288000L) 
			this.segmentSize = 524288000L;
		
		if((concurrent > 1) && (concurrent < 11)) 
			tp = TicketPool.getInstance(concurrent);
		else if(concurrent >= 11) 
			tp = TicketPool.getInstance(10);
		else //concurrent <= 1
			tp = TicketPool.getInstance(3);		
	}	
	
	public boolean storeObjectSegmented(String containerName, File obj) throws IOException, 
	                                                                          HttpException, 
	                                                                          FilesException {
		return storeObjectSegmentedAs(containerName, obj, null, null, null);
	}
	
	public boolean storeObjectSegmented(String containerName, File obj, String contentType) throws IOException, 
	                                                                                           HttpException, 
	                                                                                           FilesException {
		return storeObjectSegmentedAs(containerName, obj, contentType, null, null);
	}

	
	public boolean storeObjectSegmentedAs(String containerName, File obj, String objName) throws IOException, 
	                                                                                            HttpException, 
	                                                                                            FilesException,
	                                                                                            FilesInvalidNameException {
		return storeObjectSegmentedAs(containerName, obj, null, objName, null, null);
	}
	
	public boolean storeObjectSegmentedAs(String containerName, File obj, String contentType, String objName) throws IOException, 
	                                                                                                          HttpException, 
	                                                                                                          FilesException,
	                                                                                                          FilesInvalidNameException {
		return storeObjectSegmentedAs(containerName, obj, contentType, objName, null, null);
	}
	
	public boolean storeObjectSegmentedAs(String containerName, File obj, String contentType, 
	                                         String objName, Map<String, String> objmeta) throws IOException, 
	                                         													 HttpException, 
	                                                                                             FilesException, 
	                                                                                             FilesInvalidNameException {
		return storeObjectSegmentedAs(containerName, obj, contentType, objName, objmeta, null);
	}
	
	public boolean storeObjectSegmentedAs(String containerName, File obj, String contentType, 
	                                         String objName, Map<String, String> objmeta, 
	                                         IFilesTransferCallback callback) throws IOException, 
	                                         										 HttpException, 
	                                                                                 FilesException, 
	                                                                                 FilesInvalidNameException {
		long fsize = obj.length();
		if(fsize <= segmentSize) {
			try {
				fclient.storeObjectAs(containerName, obj, contentType, objName, callback);
				return true;
			}
			catch(Exception e) {
				e.printStackTrace();
		   		return false;
	   		}
	    }
		String segmentsContainer = containerName + "_segments";
	    if(!fclient.containerExists(segmentsContainer)) fclient.createContainer(segmentsContainer);	  
	    if(!fclient.containerExists(containerName)) 
	    	throw new FileNotFoundException("Container: " + containerName + " did not exist");
	    if(obj == null) 
			throw new FileNotFoundException("No File!");
		if(objName == null) 
			objName = obj.getName();
		if(contentType == null) 
			contentType = "application/octet-stream";
		if(objmeta == null) 
			objmeta = new Hashtable<String, String>(); 
		
		long currentTime = System.currentTimeMillis();	
		String objPath = objName + "/" + currentTime + "/" + fsize + "/";	
		String NewManifest = segmentsContainer + "/" + objPath;
		
		FilesObjectMetaDataExt objectMeta = fclient.objectExists(containerName, objName);  // name checking
		String OldManifest = null;
		
		if(objectMeta == null) {
			if(fclient.createManifestObject(containerName, contentType, objName, NewManifest, objmeta, callback))
				return uploadSegment(containerName, obj, contentType, objName, objmeta, objPath, NewManifest, callback);
			else 
				return false;
		}
		else {
			OldManifest = objectMeta.getObjectManifest();
			System.out.println("OldManifest : " + OldManifest); // debug
			if(OldManifest == null) {
				if(fsize < segmentSize) {
					fclient.storeObjectAs(containerName, obj, contentType, objName, callback);
		    		return true;		
				}
				else {
					if(fclient.createManifestObject(containerName, contentType, objName, NewManifest, objmeta, callback))
						return uploadSegment(containerName, obj, contentType, objName, objmeta, objPath, NewManifest, callback);
					else return false;		
				}			
			}
			else {
				if(deleteSegments(containerName, objName, OldManifest)) {
					if(fsize < segmentSize) {
						fclient.storeObjectAs(containerName, obj, contentType, objName, callback);
		    			return true;	
					}
					else {
						if(fclient.updateObjectMetadataAndManifest(containerName, objName, objmeta, NewManifest)) 				 
							return uploadSegment(containerName, obj, contentType, objName, objmeta, objPath, NewManifest, callback);
						else {
							if(fclient.updateObjectMetadataAndManifest(containerName, objName, objmeta, NewManifest))
								return uploadSegment(containerName, obj, contentType, objName, objmeta, objPath, NewManifest, callback);
							else 
								return false;
						}
					}
				}
				else return false;
			}
		}
	}
	
	private boolean uploadSegment(String containerName, File obj, String contentType, 
	                              String objName, Map<String, String> objmeta, 
	                              String objPath, String manifest,
	                              IFilesTransferCallback callback) throws IOException, 
	                                                                      HttpException, 
	                                                                      FilesException {
	    InputStream fileStream = null;
	    
	    long fsize = obj.length();
	    long offset = 0L;
	   	
	    if(fsize <= segmentSize) {
		    fclient.storeObjectAs(containerName, obj, contentType, objName);
		    return true;
	    }	
	        
	    long currentTime = System.currentTimeMillis();
	    String segmentsContainer = containerName + "_segments";
	 	int segNum = 0;
	 	
	 	while(fsize > segmentSize) {
		 	fileStream = new InputSubstream(new RepeatableFileInputStream(obj), offset, segmentSize, true);

	        String ticket = tp.getTicket();
		 	String segmentID = objPath + String.format("%08d", segNum);
		 	
		 	while(ticket == null) {
	        	try {
					Thread.currentThread().sleep(100); 
				}
				catch(InterruptedException e) {
					fclient.createManifestObject(containerName, contentType, objName, manifest, objmeta, callback);
					return false;
			    }
				tp.refreshPool();
				ticket = tp.getTicket();
			}
			
			SegmentUploader suder = new SegmentUploader(segmentsContainer, segmentID, fileStream, 
        	    										contentType, fclient, ticket);
        	suder.start();
        	fsize -= segmentSize;
		 	offset += segmentSize;
		 	++segNum;
	 	}
	 	
	 	if(fsize > 0) {
		 	fileStream = new InputSubstream(new RepeatableFileInputStream(obj), offset, fsize, true);
	        String ticket = tp.getTicket();
		 	String segmentID = objPath + String.format("%08d", segNum);
		 	
		 	while(ticket == null) {
	        	try {
					Thread.currentThread().sleep(100); 
				}
				catch(InterruptedException e) {
					fclient.createManifestObject(containerName, contentType, objName, manifest, objmeta, callback);
			    }
				tp.refreshPool();
				ticket = tp.getTicket();
			}
			
			SegmentUploader suder = new SegmentUploader(segmentsContainer, segmentID, fileStream, 
        	    										contentType, fclient, ticket);
        	suder.start();
	 	}
	 	
	 	while(SegmentUploader.isFinished() == false) {
		 	try { Thread.currentThread().sleep(1000); } 
			catch(InterruptedException e) {
				fclient.createManifestObject(containerName, contentType, objName, manifest, objmeta, callback);
				return false;
			}
	 	}	 	
	 	return SegmentUploader.isSucceeded();
 	}
 	
	public boolean deleteSegmentsManifest(String containerName, String objName) throws IOException, FileNotFoundException,
																					   FilesNotFoundException,	
																				       HttpException, 
																					   FilesException {
		if(deleteSegments(containerName, objName)) {
			try{
				fclient.deleteObject(containerName, objName);
				return true;
			}
			catch(Exception e) {
				e.printStackTrace();
				return false;
			}
		}
		return false;
	}
	
	private boolean deleteSegments(String containerName, String objName) throws IOException, 
																				FileNotFoundException,               
		                                                                        HttpException,              
		                                                                        FilesAuthorizationException,
		                                                                        FilesNotFoundException,     
		                                                                        FilesInvalidNameException,  
		                                                                        FilesException {            
		return deleteSegments(containerName, objName, null);
	}																					 
																				 
	private boolean deleteSegments(String containerName, String objName, String objectManifest) throws IOException,                
	                                                                                                   HttpException,              
	                                                                                                   FilesAuthorizationException,
	                                                                                                   FilesNotFoundException,     
	                                                                                                   FilesInvalidNameException,  
	                                                                                                   FilesException {            
		if(!fclient.containerExists(containerName)) 
			throw new FileNotFoundException("Container: " + containerName + " did not exist");
		int j = 0;
		boolean delete = false;			
		do {
			delete = deleteOnlySegments(containerName, objName, objectManifest);
			++j;
		}
		while(j < 3);
		
		return delete;
	}
																			 
	private boolean deleteOnlySegments(String containerName, String objName, String objectManifest)
																					throws IOException,                
	                                                                                       HttpException,              
	                                                                                       FilesAuthorizationException,
	                                                                                       FilesNotFoundException,     
	                                                                                       FilesInvalidNameException,  
	                                                                                       FilesException {
		if(fclient.objectExists(containerName, objName) == null) 
			throw new FileNotFoundException("Container or Manifest not found!"); 
		String segmentContainer = null;
		String path = null;
		StringTokenizer st = null;
		
		if(objectManifest == null) {
			FilesObjectMetaDataExt objectMeta = fclient.objectExists(containerName, objName);
			if(objectMeta != null) {
				String manifest = objectMeta.getObjectManifest();
				if(manifest != null) {
					st = new StringTokenizer(manifest,"/");
					segmentContainer = st.nextToken();
					path = manifest.substring(manifest.indexOf("/") + 1);
				}
				else {
					fclient.deleteObject(containerName, objName);
					return true;
				}
			}
			else { throw new FileNotFoundException("Container: " + containerName + " did not have object " + objName); }
		}
		else {
			st = new StringTokenizer(objectManifest,"/");
			segmentContainer = st.nextToken();
			path = objectManifest.substring(objectManifest.indexOf("/") + 1);			
		}
		
		if(fclient.containerExists(segmentContainer)) {
			List<FilesObject> objects = fclient.listObjects(segmentContainer, path);
			if(objects.size() == 0) {
				SegmentRemover.setFinished(true);
				SegmentRemover.setSucceeded(true);
			}
			else {
				Iterator<FilesObject> i = objects.iterator();
				while(i.hasNext()) {
					String ticket = tp.getTicket();
					String segmentID = i.next().getName();
					while(ticket == null) {
						try {
							Thread.currentThread().sleep(100); 
						}
						catch(InterruptedException e) {}
						tp.refreshPool();
						ticket = tp.getTicket();
					}
					SegmentRemover remover = new SegmentRemover(segmentContainer, segmentID, fclient, ticket);
					remover.start();
				} 	
			}
			
			while(SegmentRemover.isFinished() == false) {
			    try { Thread.currentThread().sleep (1000); } 
			    catch (InterruptedException e) { return false; }    
	    	}    
   			return SegmentRemover.isSucceeded();
		}
		else return true;
	}
}
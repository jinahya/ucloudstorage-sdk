package com.olleh.ucloudbiz.ucloudstorage.utils;

import java.util.Map;
import java.util.HashMap;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;

import com.olleh.ucloudbiz.ucloudstorage.FilesClientExt;
import com.rackspacecloud.client.cloudfiles.IFilesTransferCallback; 

class SegmentUploader extends Thread {
	private static boolean up_isFinished = false;
	private final String segmentID;
	private final String containerName;
	private final String contentType;
	private InputStream segmentData;
	private static final AtomicInteger up_alive = new AtomicInteger();
	private static AtomicBoolean up_isSucceeded = new AtomicBoolean(true);
	private FilesClientExt fclient;
	private TicketPool tp;
	private String ticket;
	
	protected SegmentUploader(String containerName, String segmentID, InputStream segmentData, 
	 						  String contentType, FilesClientExt fclient, String ticket) {
		this.containerName = containerName;
		this.segmentID   = segmentID;
		this.segmentData = segmentData;
		this.contentType = contentType;
		this.fclient = fclient;
		this.ticket  = ticket;
		tp = TicketPool.getInstance();
	}
	
	public void run() {
		up_alive.incrementAndGet();
		try {
			fclient.storeStreamedObject(containerName, segmentData, contentType, segmentID, 
			                            new HashMap<String, String>());
			System.out.println("upload : " + segmentID); //debug
		}
		catch(Exception e1) {
			try {
				fclient.storeStreamedObject(containerName, segmentData, contentType, segmentID, 
				                            new HashMap<String, String>());
				System.out.println("upload : " + segmentID); //debug
			}
			catch(Exception e2) {
				try {
					fclient.storeStreamedObject(containerName, segmentData, contentType, segmentID, 
					                            new HashMap<String, String>());
					System.out.println("upload : " + segmentID); //debug
				}
				catch(Exception e3) {
					e3.printStackTrace();
					up_isSucceeded.set(false);	
					up_isFinished = true;
				}
			}
		}
		finally {
			tp.freeTicket(ticket);
			up_alive.decrementAndGet();	
			if(up_alive.get() == 0) { up_isFinished = true; }			
		}
	}
	
	protected String getID() { return this.segmentID; }
	protected InputStream getData() { return this.segmentData; }
	protected static boolean isFinished() { return up_isFinished; }
	protected static boolean isSucceeded() { return up_isSucceeded.get(); }
}
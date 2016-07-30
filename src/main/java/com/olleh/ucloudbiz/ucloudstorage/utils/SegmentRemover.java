package com.olleh.ucloudbiz.ucloudstorage.utils;

import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;

import com.olleh.ucloudbiz.ucloudstorage.FilesClientExt;

class SegmentRemover extends Thread {
	private static boolean del_isFinished = false;
	private final String segmentID;
	private final String containerName;
	private static final AtomicInteger del_alive = new AtomicInteger();
	private static AtomicBoolean del_isSucceeded = new AtomicBoolean(true);
	private FilesClientExt fclient;
	private TicketPool tp;
	private String ticket;
	
	protected SegmentRemover(String containerName, String segmentID, 
						     FilesClientExt fclient, String ticket) {
		this.containerName = containerName;
		this.segmentID = segmentID;
		this.fclient = fclient;
		this.ticket = ticket;
		tp = TicketPool.getInstance();
	}
	
	public void run() {
		del_alive.incrementAndGet(); 
		try {
			fclient.deleteObject(containerName, segmentID);
			System.out.println("deleted : " + segmentID);  // test
		}
		catch(Exception e1) {
			try {
				fclient.deleteObject(containerName, segmentID);
				System.out.println("deleted : " + segmentID);  // test
			}
			catch(Exception e2) {
				try {
					fclient.deleteObject(containerName, segmentID);
					System.out.println("deleted : " + segmentID);  // test
				}
				catch(Exception e3) {
					e3.printStackTrace();    
					del_isSucceeded.set(false);	
					del_isFinished = true; 
				}  
			}
		}
		finally {
			tp.freeTicket(ticket);
			del_alive.decrementAndGet();
			if(del_alive.get() == 0) { del_isFinished = true; }
		}
	}
	
	protected String getID() { return this.segmentID; }
	protected static boolean isFinished() { return del_isFinished; }
	protected static boolean isSucceeded() { return del_isSucceeded.get(); }
	protected static void setFinished(boolean v) { del_isFinished = v; }
	protected static void setSucceeded(boolean v) { del_isSucceeded.set(true); }
}
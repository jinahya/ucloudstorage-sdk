package com.olleh.ucloudbiz.ucloudstorage.utils;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

public class HmacDigest {
	public static String hmacDigest(String msg, String keyString, String algorithm) {
		String digest = null;
		try {
			SecretKeySpec key = new SecretKeySpec(keyString.getBytes("UTF-8"), algorithm);
			Mac mac = Mac.getInstance(algorithm);
			mac.init(key);
			
			byte[] bytes = mac.doFinal(msg.getBytes("ASCII"));
			StringBuffer hash = new StringBuffer();
			
			for(int i = 0; i < bytes.length; ++i) {
				String hex = Integer.toHexString(0xFF & bytes[i]);
				if(hex.length() == 1) hash.append("0");
				hash.append(hex);
			}
			digest = hash.toString();
		}
		catch(UnsupportedEncodingException e) {
			e.printStackTrace();
			System.err.println("UnsupportedEncodingException");
		}			
		catch(InvalidKeyException e) {
			e.printStackTrace();
			System.err.println("InvalidKeyException");
		}
		catch(NoSuchAlgorithmException e) {
			e.printStackTrace();
			System.err.println("NoSuchAlgorithmException");	
		}
		return digest;
	}
}		    
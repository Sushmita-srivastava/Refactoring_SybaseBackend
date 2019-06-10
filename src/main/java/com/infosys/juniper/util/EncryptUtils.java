package com.infosys.juniper.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Base64;
import java.util.Scanner;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import org.springframework.stereotype.Component;


@Component
public class EncryptUtils {
	


	public static SecretKey decodeKeyFromString(String keyStr) {
		/* Decodes a Base64 encoded String into a byte array */
		String keyString = keyStr.trim();
		byte[] decodedKey = Base64.getDecoder().decode(keyString);

		/* Constructs a secret key from the given byte array */
		SecretKey secretKey = new SecretKeySpec(decodedKey, 0, decodedKey.length, "AES");

		return secretKey;
	}
	
	public static String readFile(String pathname) throws IOException {
		System.out.println("PATH: "+ pathname);
		File file = new File(pathname);
		StringBuilder fileContents = new StringBuilder((int) file.length());
		Scanner scanner = new Scanner(new BufferedReader(new FileReader(file)));
		String lineSeparator = System.getProperty("line.separator");

		try {
			if (scanner.hasNextLine()) {
				fileContents.append(scanner.nextLine());
			}
			while (scanner.hasNextLine()) {
				fileContents.append(lineSeparator + scanner.nextLine());
			}
			System.out.println("fileContents.toString()" + fileContents.toString());
			return fileContents.toString();
		} finally {
			scanner.close();
		}
	}
	
	
	
	
	public  static String decryptText(byte[] byteCipherText, SecretKey secKey) throws Exception {

		// AES defaults to AES/ECB/PKCS5Padding in Java 7
		
		

		System.out.println(secKey.toString());

		Cipher aesCipher = Cipher.getInstance("AES");

		aesCipher.init(Cipher.DECRYPT_MODE, secKey);

		byte[] bytePlainText = aesCipher.doFinal(byteCipherText);
		
		

		return new String(bytePlainText);

	}
	
	public static byte[] encryptText(String plainText, String key) throws Exception {

		
		// AES defaults to AES/ECB/PKCS5Padding in Java 7
		System.out.println("key is "+key);
		SecretKey secKey=decodeKeyFromString(key);

		Cipher aesCipher = Cipher.getInstance("AES");

		aesCipher.init(Cipher.ENCRYPT_MODE, secKey);

		byte[] byteCipherText = aesCipher.doFinal(plainText.getBytes());

		
		
		return byteCipherText;

	}



	
	

}

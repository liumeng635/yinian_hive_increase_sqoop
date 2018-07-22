package com.yinian.alysis.util;

import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.tomcat.util.codec.binary.Base64;  
  
public class EncrypDES {  
	public static String key_gen = "yinian";  
    
    
    
    private static final String CipherMode ="AES/CBC/PKCS5Padding";  
    
    // /** 创建密钥 **/  
    private static SecretKeySpec createKey(String key) {  
    byte[] data = null;  
    if (key == null) {  
    key ="";  
   }  
    StringBuffer sb = new StringBuffer(16);  
   sb.append(key);  
    while (sb.length() < 16) {  
   sb.append("0");  
   }  
    if (sb.length() > 16) {  
   sb.setLength(16);  
   }  
 
    try {  
    data = sb.toString().getBytes("UTF-8");  
    } catch (UnsupportedEncodingException e) {  
   e.printStackTrace();  
   }  
    return new SecretKeySpec(data,"AES");  
   }  
 
    private static IvParameterSpec createIV(String password) {  
    byte[] data = null;  
    if (password == null) {  
    password ="";  
   }  
    StringBuffer sb = new StringBuffer(16);  
   sb.append(password);  
    while (sb.length() < 16) {  
   sb.append("0");  
   }  
    if (sb.length() > 16) {  
   sb.setLength(16);  
   }  
 
    try {  
    data = sb.toString().getBytes("UTF-8");  
    } catch (UnsupportedEncodingException e) {  
   e.printStackTrace();  
   }  
    return new IvParameterSpec(data);  
   }  
 
    // /** 加密字节数据 **/  
    public static byte[] encrypt(byte[] content, String password, String iv) {  
    try {  
    SecretKeySpec key = createKey(password);  
    Cipher cipher = Cipher.getInstance(CipherMode);  
    cipher.init(Cipher.ENCRYPT_MODE, key, createIV(iv));  
    byte[] result = cipher.doFinal(content);  
    return result;  
    } catch (Exception e) {  
   e.printStackTrace();  
   }  
    return null;  
   }  
 
    // /** 加密(结果为16进制字符串) **/  
    public static String encrypt(String content) {  
    byte[] data = null;  
    try {  
    data = content.getBytes("UTF-8");  
    } catch (Exception e) {  
   e.printStackTrace();  
   }  
    data = encrypt(data, key_gen, key_gen);  
    String result = new Base64().encodeToString(data);  
    return result;  
   }  
 
    // /** 解密字节数组 **/  
    public static byte[] decrypt(byte[] content, String password, String iv) {  
    try {  
    SecretKeySpec key = createKey(password);  
    Cipher cipher = Cipher.getInstance(CipherMode);  
    cipher.init(Cipher.DECRYPT_MODE, key, createIV(iv));  
    byte[] result = cipher.doFinal(content);  
    return result;  
    } catch (Exception e) {  
   e.printStackTrace();  
   }  
    return null;  
   }  
 
    // /** 解密 **/  
    public static String decrypt(String content) {  
    byte[] data = null;  
    try {  
    data =  new Base64().decode(content);//先用base64解密  
    } catch (Exception e) {  
   e.printStackTrace();  
   }  
    data = decrypt(data, key_gen, key_gen);  
    if (data == null)  
    return null;  
    String result = null;  
    try {  
    result = new String(data,"UTF-8");  
    } catch (UnsupportedEncodingException e) {  
   e.printStackTrace();  
   }  
    return result;  
   }  
    
    /** 
     * @param args 
     * @throws NoSuchAlgorithmException  
     * @throws BadPaddingException  
     * @throws IllegalBlockSizeException  
     * @throws NoSuchPaddingException  
     * @throws InvalidKeyException  
     */  
    public static void main(String[] args) throws NoSuchAlgorithmException, InvalidKeyException, NoSuchPaddingException, IllegalBlockSizeException, BadPaddingException {  
        String msg = "yinian2018@";
        System.out.println("加密:"+encrypt(msg));
        String s = "N42KvDUs+hsDaf12r+z1Mg==";
        System.out.println("解密:"+decrypt(s));
    } 
  
}  

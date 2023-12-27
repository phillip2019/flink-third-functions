package com.chinagoods.bigdata.util;

import javax.crypto.Cipher;
import java.io.ByteArrayOutputStream;
import java.security.Key;
import java.security.KeyFactory;
import java.security.spec.X509EncodedKeySpec;

/**
 * 
 * {@code @ClassName:} RSAUtils
 * {@code @Description:} RSA公钥/私钥/签名工具包
 * 字符串格式的密钥在未在特殊说明情况下都为BASE64编码格式,
 * 由于非对称加密速度极其缓慢,一般文件不使用它来加密而是使用对称加密，
 * 非对称加密算法可以用来对对称加密的密钥加密,这样保证密钥的安全也就保证了数据的安全
 * 
 * @author ZhuGuoku
 */
public class RSAUtils {

	/** 
	* 加密算法RSA
	*/
	public static final String KEY_ALGORITHM = "RSA";

	/**
	* 签名算法
	*/
	public static final String SIGNATURE_ALGORITHM = "MD5withRSA";

	/**
	* 获取公钥的key
	*/
	private static final String PUBLIC_KEY = "RSAPublicKey";

	/**
	* 获取私钥的key
	*/
	private static final String PRIVATE_KEY = "RSAPrivateKey";

	/**
	* RSA最大加密明文大小
	*/
	private static final int MAX_ENCRYPT_BLOCK = 117;

	/**
	* RSA最大解密密文大小
	*/
	private static final int MAX_DECRYPT_BLOCK = 128;

	/**
	* 公钥解密
	* 
	* @param encryptedData 已加密数据
	* @param publicKey 公钥(BASE64编码)
	* @return 解密之后的公钥
	* @throws Exception 异常
	*/
	public static byte[] decryptByPublicKey(byte[] encryptedData, String publicKey) throws Exception {
		byte[] keyBytes = Base64Utils.decode(publicKey);
		X509EncodedKeySpec x509KeySpec = new X509EncodedKeySpec(keyBytes);
		KeyFactory keyFactory = KeyFactory.getInstance(KEY_ALGORITHM);
		Key publicK = keyFactory.generatePublic(x509KeySpec);
		Cipher cipher = Cipher.getInstance(keyFactory.getAlgorithm());
		cipher.init(Cipher.DECRYPT_MODE, publicK);
		int inputLen = encryptedData.length;
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		int offSet = 0;
		byte[] cache;
		int i = 0;
		// 对数据分段解密
		while (inputLen - offSet > 0) {
			if (inputLen - offSet > MAX_DECRYPT_BLOCK) {
				cache = cipher.doFinal(encryptedData, offSet, MAX_DECRYPT_BLOCK);
			} else {
				cache = cipher.doFinal(encryptedData, offSet, inputLen - offSet);
			}
			out.write(cache, 0, cache.length);
			i++;
			offSet = i * MAX_DECRYPT_BLOCK;
		}
		byte[] decryptedData = out.toByteArray();
		out.close();
		return decryptedData;
	}

	/**
	* 公钥加密
	* 
	* @param data 源数据
	* @param publicKey 公钥(BASE64编码)
	* @return 加密之后的内容
	* @throws Exception 异常
	*/
	public static byte[] encryptByPublicKey(byte[] data, String publicKey) throws Exception {
		byte[] keyBytes = Base64Utils.decode(publicKey);
		X509EncodedKeySpec x509KeySpec = new X509EncodedKeySpec(keyBytes);
		KeyFactory keyFactory = KeyFactory.getInstance(KEY_ALGORITHM);
		Key publicK = keyFactory.generatePublic(x509KeySpec);
		// 对数据加密
		Cipher cipher = Cipher.getInstance(keyFactory.getAlgorithm());
		cipher.init(Cipher.ENCRYPT_MODE, publicK);
		int inputLen = data.length;
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		int offSet = 0;
		byte[] cache;
		int i = 0;
		// 对数据分段加密
		while (inputLen - offSet > 0) {
			if (inputLen - offSet > MAX_ENCRYPT_BLOCK) {
				cache = cipher.doFinal(data, offSet, MAX_ENCRYPT_BLOCK);
			} else {
				cache = cipher.doFinal(data, offSet, inputLen - offSet);
			}
			out.write(cache, 0, cache.length);
			i++;
			offSet = i * MAX_ENCRYPT_BLOCK;
		}
		byte[] encryptedData = out.toByteArray();
		out.close();
		return encryptedData;
	}
}

package com.convertlab.data.util.crypto;

import org.apache.commons.lang3.StringUtils;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;

public class AESECBUtils {

    private AESECBUtils() {}

    private static final String KEY_ALGORITHM = "AES";
    private static final String CIPHER_ALGORITHM = "AES/ECB/PKCS5Padding";
    private static final String KEY_PASSWORD = "UWNzrlZIRnN3n5Ch";

    public static String encrypt(String value) throws Exception {
        return encrypt(value, KEY_PASSWORD);
    }

    public static String decrypt(String value) throws Exception {
        return decrypt(value, KEY_PASSWORD);
    }

    public static String encrypt(String value, String password) throws Exception {
        if (StringUtils.isBlank(value)) {
            return value;
        }
        SecretKeySpec secretKeySpec = new SecretKeySpec(password.getBytes(StandardCharsets.UTF_8), KEY_ALGORITHM);
        Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);
        cipher.init(Cipher.ENCRYPT_MODE, secretKeySpec);
        byte[] content = value.getBytes(StandardCharsets.UTF_8);
        byte[] encryptData = cipher.doFinal(content);
        return byte2hex(encryptData);
    }

    public static String decrypt(String value, String password) throws Exception {
        if (StringUtils.isBlank(value)) {
            return value;
        }
        SecretKeySpec secretKeySpec = new SecretKeySpec(password.getBytes(StandardCharsets.UTF_8), KEY_ALGORITHM);
        byte[] encryptData = hex2byte(value);
        Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);
        cipher.init(Cipher.DECRYPT_MODE, secretKeySpec);
        byte[] content = cipher.doFinal(encryptData);
        return new String(content, StandardCharsets.UTF_8);
    }

    private static String byte2hex(byte[] b) {
        StringBuilder hs = new StringBuilder();
        String stmp;
        for (int i = 0; i < b.length; i++) {
            stmp = Integer.toHexString(b[i] & 0xFF).toUpperCase();
            if (stmp.length() == 1) {
                hs.append("0").append(stmp);
            } else {
                hs.append(stmp);
            }
        }
        return hs.toString();
    }

    private static byte[] hex2byte(String hex) throws IllegalArgumentException {
        if (hex.length() % 2 != 0) {
            throw new IllegalArgumentException("invalid hex string");
        }
        char[] arr = hex.toCharArray();
        byte[] b = new byte[hex.length() / 2];
        for (int i = 0, j = 0, l = hex.length(); i < l; i++, j++) {
            String swap = "" + arr[i++] + arr[i];
            int byteInt = Integer.parseInt(swap, 16) & 0xFF;
            b[j] = new Integer(byteInt).byteValue();
        }
        return b;
    }
}

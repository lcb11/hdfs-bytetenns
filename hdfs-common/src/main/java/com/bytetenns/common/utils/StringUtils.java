package com.bytetenns.common.utils;

import org.apache.commons.codec.digest.DigestUtils;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.io.File;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Random;

/**
 * 字符串工具
 */
public class StringUtils {

    public static final Random RANDOM = new Random();
    public static final String BASE_KEY = "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";


    /**
     * 获取随机字符串
     *
     * @param length 字符串长度
     * @return 随机字符串
     */
    public static String getRandomString(int length) {
        return getRandomString(length, BASE_KEY);
    }

    /**
     * 获取随机字符串
     *
     * @param length 字符串长度
     * @return 随机字符串
     */
    public static String getRandomString(int length, boolean upperCase) {
        return getRandomString(length, BASE_KEY, upperCase);
    }

    /**
     * 获取随机字符串
     *
     * @param length 字符串长度
     * @return 随机字符串
     */
    public static String getRandomString(int length, String key) {
        return getRandomString(length, key, false);
    }

    /**
     * 获取随机字符串
     *
     * @param length 字符串长度
     * @return 随机字符串
     */
    public static String getRandomString(int length, String key, boolean upperCase) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            int number = RANDOM.nextInt(key.length());
            sb.append(key.charAt(number));
        }
        String s = sb.toString();
        return upperCase ? s.toUpperCase() : s;
    }

    public static int hash(String source, int maxSize) {
        int hash = toPositive(murmur2(source.getBytes()));
        return hash % maxSize;
    }


    public static int toPositive(int number) {
        return number & 0x7fffffff;
    }

    /**
     * Generates 32 bit murmur2 hash from byte array
     *
     * @param data byte array to hash
     * @return 32 bit hash of the given array
     */
    public static int murmur2(final byte[] data) {
        int length = data.length;
        int seed = 0x9747b28c;
        // 'm' and 'r' are mixing constants generated offline.
        // They're not really 'magic', they just happen to work well.
        final int m = 0x5bd1e995;
        final int r = 24;

        // Initialize the hash to a random value
        int h = seed ^ length;
        int length4 = length / 4;

        for (int i = 0; i < length4; i++) {
            final int i4 = i * 4;
            int k = (data[i4 + 0] & 0xff) + ((data[i4 + 1] & 0xff) << 8) + ((data[i4 + 2] & 0xff) << 16) + ((data[i4 + 3] & 0xff) << 24);
            k *= m;
            k ^= k >>> r;
            k *= m;
            h *= m;
            h ^= k;
        }

        // Handle the last few bytes of the input array
        switch (length % 4) {
            case 3:
                h ^= (data[(length & ~3) + 2] & 0xff) << 16;
            case 2:
                h ^= (data[(length & ~3) + 1] & 0xff) << 8;
            case 1:
                h ^= data[length & ~3] & 0xff;
                h *= m;
        }

        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;
        return h;
    }

    public static String md5(String src) {
        return DigestUtils.md5Hex(src.getBytes());
    }

    public static String aesEncrypt(String key, String content) throws Exception {
        Cipher cipher = getCipher(key, Cipher.ENCRYPT_MODE);
        byte[] byteEncode = content.getBytes(StandardCharsets.UTF_8);
        byte[] byteAes = cipher.doFinal(byteEncode);
        return Base64.getUrlEncoder().encodeToString(byteAes);
    }

    public static String aesDecrypt(String key, String content) throws Exception {
        Cipher cipher = getCipher(key, Cipher.DECRYPT_MODE);
        byte[] byteContent = Base64.getUrlDecoder().decode(content);
        byte[] byteDecode = cipher.doFinal(byteContent);
        return new String(byteDecode, StandardCharsets.UTF_8);
    }

    private static Cipher getCipher(String seed, int mode) throws Exception {
        KeyGenerator keygen = KeyGenerator.getInstance("AES");
        SecureRandom secureRandom = SecureRandom.getInstance("SHA1PRNG");
        secureRandom.setSeed(seed.getBytes());
        keygen.init(128, secureRandom);
        SecretKey originalKey = keygen.generateKey();
        byte[] raw = originalKey.getEncoded();
        SecretKey key = new SecretKeySpec(raw, "AES");
        Cipher cipher = Cipher.getInstance("AES");
        cipher.init(mode, key);
        return cipher;
    }


    public static boolean validateFileName(String filename) {
        String osName = System.getProperty("os.name").toLowerCase();
        boolean win = osName.startsWith("win");
        if (!win && !filename.startsWith(File.separator)) {
            return false;
        }
        String name = new File(filename).getName();
        if (filename.contains("//")) {
            return false;
        }
        return !name.startsWith(".");
    }


    public static String getPercent(Long total, Long value) {
        if (total == 0L) {
            return "0";
        }
        return new BigDecimal(value).multiply(new BigDecimal(100))
                .divide(new BigDecimal(total), 2, RoundingMode.HALF_UP).toString();
    }

    public static String[] split(String str, char c) {
        return org.apache.commons.lang3.StringUtils.split(str, c);
    }


    public static String format(int i) {
        if (i >= 100) {
            return String.valueOf(i);
        }
        if (i >= 10) {
            return "0" + i;
        }
        if (i >= 0) {
            return "00" + i;
        }
        return "";
    }
}

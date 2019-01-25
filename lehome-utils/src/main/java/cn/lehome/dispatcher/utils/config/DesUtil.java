package cn.lehome.dispatcher.utils.config;

import cn.lehome.framework.base.api.core.exception.BaseApiException;
import sun.misc.BASE64Decoder;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESKeySpec;
import java.security.SecureRandom;

public class DesUtil {

    private final static String DES = "DES";

    private final static String ENCODE = "UTF-8";

    public static String decryptPhone(String phoneDes, String KEY) throws BaseApiException {
        try {
            BASE64Decoder decoder = new BASE64Decoder();
            byte[] buf = decoder.decodeBuffer(phoneDes);
            byte[] bt = decrypt(buf, KEY.getBytes(ENCODE));
            return new String(bt, ENCODE);
        } catch (Exception e) {
            throw new BaseApiException("DES解密错误");
        }
    }

    private static byte[] decrypt(byte[] data, byte[] key) throws Exception {
        // 生成一个可信任的随机数源
        SecureRandom sr = new SecureRandom();

        // 从原始密钥数据创建DESKeySpec对象
        DESKeySpec dks = new DESKeySpec(key);

        // 创建一个密钥工厂，然后用它把DESKeySpec转换成SecretKey对象
        SecretKeyFactory keyFactory = SecretKeyFactory.getInstance(DES);
        SecretKey securekey = keyFactory.generateSecret(dks);

        // Cipher对象实际完成解密操作
        Cipher cipher = Cipher.getInstance(DES);

        // 用密钥初始化Cipher对象
        cipher.init(Cipher.DECRYPT_MODE, securekey, sr);

        return cipher.doFinal(data);
    }

    public static void main(String[] args) {
        System.out.println(DesUtil.decryptPhone("", ""));
    }

}

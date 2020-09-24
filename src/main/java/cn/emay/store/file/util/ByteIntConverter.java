package cn.emay.store.file.util;

/**
 * Byte 与 Int 转换工具
 *
 * @author Frank
 */
public class ByteIntConverter {

    /**
     * int 到4位字节
     *
     * @param value int
     * @return 字节
     */
    public static byte[] toBytes(int value) {
        byte[] src = new byte[4];
        src[0] = (byte) ((value >> 24) & 0xFF);
        src[1] = (byte) ((value >> 16) & 0xFF);
        src[2] = (byte) ((value >> 8) & 0xFF);
        src[3] = (byte) (value & 0xFF);
        return src;
    }

    /**
     * 字节转int
     *
     * @param src 字节
     * @return int
     */
    public static int toInt(byte[] src) {
        return ((src[0] & 0xFF) << 24) | ((src[1] & 0xFF) << 16) | ((src[2] & 0xFF) << 8) | (src[3] & 0xFF);
    }

}

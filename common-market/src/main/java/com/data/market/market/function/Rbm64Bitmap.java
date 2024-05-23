package com.data.market.market.function;

import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 功能：Rbm64Bitmap
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2024/5/16 23:08
 */
public class Rbm64Bitmap {
    private Roaring64NavigableMap bitmap;


    public Rbm64Bitmap() {
        this.bitmap = new Roaring64NavigableMap();
    }

    public Rbm64Bitmap(Long value) {
        this.bitmap = new Roaring64NavigableMap();
        this.bitmap.addLong(value);
    }

    public Rbm64Bitmap(Roaring64NavigableMap bitmap) {
        this.bitmap = bitmap;
    }

    /**
     * bytes 转换为 Bitmap
     * @param bytes
     * @return
     * @throws IOException
     */
    public static Rbm64Bitmap bytesToBitmap(byte[] bytes) throws IOException {
        Rbm64Bitmap bitmap = new Rbm64Bitmap();
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
            bitmap.deserialize(in);
        } catch (IOException e) {
            throw new IOException("Error deserializing bitmap: ", e);
        }
        return bitmap;
    }

    /**
     * Bitmap 转换为 bytes
     * @param bitmap
     * @return
     * @throws IOException
     */
    public static byte[] bitmapToBytes(Rbm64Bitmap bitmap) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(bos)) {
            bitmap.serialize(dos);
        } catch (IOException e) {
            throw new IOException("Error serializing bitmap: ", e);
        }
        return bos.toByteArray();
    }

    /**
     * Bitmap 转换为 字符串
     * @return
     * @throws IOException
     */
    public String bitmapToString() throws IOException {
        final StringBuilder answer = new StringBuilder();
        LongIterator iterator = this.bitmap.getLongIterator();
        while (iterator.hasNext()) {
            long nextValue = iterator.next();
            if (answer.length() > 0) {
                answer.append(",");
            }
            answer.append(nextValue);
        }
        return answer.toString();
    }

    /**
     * Bitmap 转换为 List<Long>
     * @return
     * @throws IOException
     */
    public List<Long> bitmapToArray() throws IOException {
        List<Long> answer = new ArrayList<>();
        LongIterator iterator = this.bitmap.getLongIterator();
        while (iterator.hasNext()) {
            long nextValue = iterator.next();
            answer.add(nextValue);
        }
        return answer;
    }

    /**
     * List
     * @param values
     */
    public void fromArray(List<Long> values) {
        this.bitmap = this.bitmap == null ? new Roaring64NavigableMap() : this.bitmap;
        for (Long value : values) {
            this.bitmap.addLong(value);
        }
    }

    /**
     * 添加元素
     * @param value
     */
    public void add(Long value) {
        this.bitmap.addLong(value);
    }

    /**
     * 与操作
     * @param other
     */
    public void and(Rbm64Bitmap other) {
        this.bitmap.and(other.bitmap);
    }


    /**
     * 或操作
     * @param other
     */
    public void or(Rbm64Bitmap other) {
        this.bitmap.or(other.bitmap);
    }


    /**
     * 异或操作
     * @param other
     */
    public void xor(Rbm64Bitmap other) {
        this.bitmap.xor(other.bitmap);
    }

    /**
     * 差集
     * @param other
     */
    public void andNot(Rbm64Bitmap other) {
        this.bitmap.andNot(other.bitmap);
    }

    /**
     * 是否包含
     * @param value
     */
    public boolean contains(Long value) {
        return this.bitmap.contains(value);
    }

    /**
     * 是否包含公共元素
     * @param other
     */
    public boolean hasAny(Rbm64Bitmap other) {
        this.bitmap.and(other.bitmap);
        return this.bitmap.getLongCardinality() > 0 ? true :false;
    }

    /**
     * 基数
     */
    public Long getLongCardinality() {
        return this.bitmap.getLongCardinality();
    }

    /**
     * 序列化
     * @param output
     * @throws IOException
     */
    public void serialize(DataOutput output) throws IOException {
        bitmap.serialize(output);
    }

    /**
     * 反序列化
     * @param input
     * @throws IOException
     */
    public void deserialize(DataInput input) throws IOException {
        clear();
        bitmap = bitmap == null ? new Roaring64NavigableMap() : bitmap;
        bitmap.deserialize(input);
    }

    /**
     * 清空
     */
    public void clear() {
        this.bitmap = null;
    }
}

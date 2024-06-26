package com.data.market.market.function;

import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.*;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.OptionalLong;

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

    //------------------------------------------------------------------------------------------------------------------

    /**
     * bytes 转换为 Bitmap
     * @param bytes
     * @return
     * @throws IOException
     */
    public static Rbm64Bitmap fromBytes(byte[] bytes) throws IOException {
        Rbm64Bitmap bitmap = new Rbm64Bitmap();
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
            bitmap.deserialize(in);
        } catch (IOException e) {
            throw new IOException("Error deserializing bitmap: ", e);
        }
        return bitmap;
    }

    /**
     *  Base64 字符串转换为 Bitmap
     * @return
     * @throws IOException
     */
    public static Rbm64Bitmap fromBase64(String base64String) throws IOException {
        byte[] bytes = Base64.getDecoder().decode(base64String);
        Rbm64Bitmap bitmap = fromBytes(bytes);
        return bitmap;
    }

    /**
     *  字符串转换为 Bitmap
     * @return
     * @throws IOException
     */
    public static Rbm64Bitmap fromString(String string) {
        Rbm64Bitmap bitmap = new Rbm64Bitmap();
        String[] strArray = string.split(",");
        for (String s : strArray) {
            long v = Long.parseLong(s);
            bitmap.add(v);
        }
        return bitmap;
    }

    /**
     * 数组转换为 Bitmap
     * @param values
     */
    public static Rbm64Bitmap fromArray(List<Long> values) {
        Rbm64Bitmap bitmap = new Rbm64Bitmap();
        for (Long value : values) {
            bitmap.add(value);
        }
        return bitmap;
    }

    //------------------------------------------------------------------------------------------------------------------

    /**
     * Bitmap 转换为 bytes
     * @return
     * @throws IOException
     */
    public byte[] toBytes() throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(bos)) {
            this.bitmap.serialize(dos);
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
    public String toString() {
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
    public List<Long> toArray() throws IOException {
        List<Long> answer = new ArrayList<>();
        LongIterator iterator = this.bitmap.getLongIterator();
        while (iterator.hasNext()) {
            long nextValue = iterator.next();
            answer.add(nextValue);
        }
        return answer;
    }

    /**
     * Bitmap 转换为 Base64 字符串
     * @return
     * @throws IOException
     */
    public String toBase64() throws IOException {
        byte[] bytes = toBytes();
        String base64String = Base64.getEncoder().encodeToString(bytes);
        return base64String;
    }

    //------------------------------------------------------------------------------------------------------------------

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
     * 添加指定的数值
     * @param value
     */
    public void add(Long value) {
        this.bitmap.addLong(value);
    }

    /**
     * 删除指定的数值
     * @param value
     */
    public void remove(Long value) {
        this.bitmap.removeLong(value);
    }

    /**
     * 最大值
     * @return
     */
    public Long max() {
        OptionalLong maxOptional = this.bitmap.stream().max();
        if (maxOptional.isPresent()) {
            return maxOptional.getAsLong();
        } else {
            return 0L;
        }
    }

    /**
     * 最小值
     * @return
     */
    public Long min() {
        OptionalLong minOptional = this.bitmap.stream().min();
        if (minOptional.isPresent()) {
            return minOptional.getAsLong();
        } else {
            return -1L;
        }
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
     * 返回取值在 [start, end) 指定范围内的元素
     * @param start
     * @param end
     */
    public void subsetInRange(Long start, Long end) {
        this.bitmap = Roaring64NavigableMap.bitmapOf(
                this.bitmap.stream().filter(x -> x >= start && x < end).toArray()
        );
    }

    /**
     * 根据指定的起始值截取指定个数的元素
     * @param start
     * @param limit
     */
    public void subsetLimit(Long start, Long limit) {
        this.bitmap = Roaring64NavigableMap.bitmapOf(
                this.bitmap.stream().filter(x -> x >= start).limit(limit).toArray()
        );
    }

    /**
     * 根据 offset 指定的起始位置截取指定个数的元素
     * @param offset
     * @param limit
     */
    public void subsetOffset(Long offset, Long limit) {
        this.bitmap = Roaring64NavigableMap.bitmapOf(
                this.bitmap.stream().skip(offset).limit(limit).toArray()
        );
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

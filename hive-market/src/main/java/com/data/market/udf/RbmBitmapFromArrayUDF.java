package com.data.market.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.io.parquet.serde.ArrayWritableObjectInspector;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * 功能：Array 转换为 Bitmap
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2024/5/17 06:51
 */
public class RbmBitmapFromArrayUDF extends GenericUDF {
    private static String functionName = "rbm_bitmap_from_array";
    private transient ArrayWritableObjectInspector inspector;

    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        // 参数个数校验
        if (arguments.length != 1) {
            throw new UDFArgumentLengthException("The function 'rbm_bitmap_and' only accepts 1 argument, but got " + arguments.length);
        }

        // 参数类型校验
        ObjectInspector arg = arguments[0];
        if (!(arg instanceof ArrayWritableObjectInspector)) {
            throw new UDFArgumentException("Argument of rbm_bitmap_and should be binary type");
        }
        this.inspector = (ArrayWritableObjectInspector) arg;

        // 返回值类型
        return PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
    }

    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        if (deferredObjects[0].get() == null) {
            return null;
        }
        return null;
    }

    public String getDisplayString(String[] children) {
        // 这里返回函数及其参数的描述
        return "rbm_bitmap_and(bitmap, bitmap)";
    }
}
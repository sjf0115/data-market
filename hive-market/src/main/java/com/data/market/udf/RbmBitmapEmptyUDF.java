package com.data.market.udf;

import com.data.market.market.function.Rbm64Bitmap;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.io.IOException;

/**
 * 功能：返回一个空的 Bitmap
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2024/5/17 06:51
 */
public class RbmBitmapEmptyUDF extends GenericUDF {
    private static String functionName = "rbm_bitmap_empty";

    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        // 参数个数校验
        if (arguments.length != 0) {
            throw new UDFArgumentLengthException("The function '" +  functionName +"' no argument, but got " + arguments.length);
        }
        // 返回值类型
        return PrimitiveObjectInspectorFactory.javaByteObjectInspector;
    }

    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        try {
            Rbm64Bitmap bitmap = new Rbm64Bitmap();
            return bitmap.toBytes();
        } catch (IOException e) {
            throw new HiveException(e);
        }
    }

    public String getDisplayString(String[] children) {
        // 这里返回函数及其参数的描述
        return functionName + "()";
    }
}
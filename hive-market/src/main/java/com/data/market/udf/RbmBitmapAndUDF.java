package com.data.market.udf;

import com.data.market.market.function.Rbm64Bitmap;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableBinaryObjectInspector;
import org.apache.hadoop.io.BytesWritable;

import java.io.IOException;

/**
 * 功能：Bitmap 与
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2024/5/17 06:51
 */
@Description(name = "rbm_bitmap_and", value = "_FUNC_(bitmap1, bitmap2) - Returns the and of two bitmaps")
public class RbmBitmapAndUDF extends GenericUDF {
    private static String functionName = "rbm_bitmap_and";

    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        // 参数个数校验
        if (arguments.length != 2) {
            throw new UDFArgumentLengthException("The function '" + functionName + "' only accepts 2 argument, but got " + arguments.length);
        }

        // 参数类型校验
        for (int i = 0; i < arguments.length; i++) {
            if (!(arguments[i] instanceof WritableBinaryObjectInspector)) {
                throw new UDFArgumentException(functionName + " expects binary type for argument " + (i + 1));
            }
        }

        // 返回值类型
        return PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
    }

    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        if (deferredObjects[0].get() == null || deferredObjects[1].get() == null) {
            return null;
        }

        BytesWritable bw0 = (BytesWritable)(deferredObjects[0].get());
        BytesWritable bw1 = (BytesWritable)(deferredObjects[1].get());
        byte[] bytes0 = bw0.getBytes();
        byte[] bytes1 = bw1.getBytes();

        try {
            Rbm64Bitmap bitmap0 = Rbm64Bitmap.fromBytes(bytes0);
            Rbm64Bitmap bitmap1 = Rbm64Bitmap.fromBytes(bytes1);
            bitmap0.and(bitmap1);
            return new BytesWritable(bitmap0.toBytes());
        } catch (IOException e) {
            throw new HiveException(e);
        }
    }

    public String getDisplayString(String[] children) {
        // 这里返回函数及其参数的描述
        return functionName + "(bitmap, bitmap)";
    }
}
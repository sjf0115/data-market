package com.data.market.udf;

import com.data.market.market.function.Rbm64Bitmap;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

import java.io.IOException;

/**
 * 功能：Bitmap 与
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2024/5/17 06:51
 */
public class RbmBitmapAndUDF extends GenericUDF {
    private static String functionName = "rbm_bitmap_and";
    private transient BinaryObjectInspector inspector0;
    private transient BinaryObjectInspector inspector1;

    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        // 参数个数校验
        if (arguments.length != 2) {
            throw new UDFArgumentLengthException("The function '" + functionName + "' only accepts 2 argument, but got " + arguments.length);
        }

        // 参数类型校验
        ObjectInspector arg0 = arguments[0];
        ObjectInspector arg1 = arguments[1];
        if (!(arg0 instanceof BinaryObjectInspector) || !(arg1 instanceof BinaryObjectInspector)) {
            throw new UDFArgumentException("Argument of '" + functionName + "' should be binary type");
        }
        this.inspector0 = (BinaryObjectInspector) arg0;
        this.inspector1 = (BinaryObjectInspector) arg1;

        // 返回值类型
        return PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
    }

    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        if (deferredObjects[0].get() == null || deferredObjects[1].get() == null) {
            return null;
        }

        byte[] bytes0 = PrimitiveObjectInspectorUtils.getBinary(deferredObjects[0].get(), this.inspector0).getBytes();
        byte[] bytes1 = PrimitiveObjectInspectorUtils.getBinary(deferredObjects[1].get(), this.inspector1).getBytes();

        try {
            Rbm64Bitmap bitmap0 = Rbm64Bitmap.bytesToBitmap(bytes0);
            Rbm64Bitmap bitmap1 = Rbm64Bitmap.bytesToBitmap(bytes1);
            bitmap0.and(bitmap1);
            return Rbm64Bitmap.bitmapToBytes(bitmap0);
        } catch (IOException e) {
            throw new HiveException(e);
        }
    }

    public String getDisplayString(String[] children) {
        // 这里返回函数及其参数的描述
        return functionName + "(bitmap, bitmap)";
    }
}
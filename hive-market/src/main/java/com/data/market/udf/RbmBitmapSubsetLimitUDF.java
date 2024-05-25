package com.data.market.udf;

import com.data.market.market.function.Rbm64Bitmap;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

import java.io.IOException;

/**
 * 功能：计算位图的子集，返回元素根据指定的起始值，从 Bitmap 中截取指定个数的元素，并返回一个新的 Bitmap
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2024/5/25 11:54
 */
public class RbmBitmapSubsetLimitUDF extends GenericUDF {
    private static String functionName = "rbm_bitmap_subset_limit";

    private transient BinaryObjectInspector inspector0;
    private transient LongObjectInspector inspector1;
    private transient LongObjectInspector inspector2;

    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        // 参数个数校验
        if (arguments.length != 3) {
            throw new UDFArgumentLengthException("The function '" + functionName + "' only accepts 2 argument, but got " + arguments.length);
        }

        // 参数类型校验
        ObjectInspector arg0 = arguments[0];
        ObjectInspector arg1 = arguments[1];
        ObjectInspector arg2 = arguments[2];
        if (!(arg0 instanceof BinaryObjectInspector)) {
            throw new UDFArgumentException("The first argument of '" + functionName + "' should be binary type");
        }
        if (!(arg1 instanceof LongObjectInspector) || !(arg2 instanceof LongObjectInspector)) {
            throw new UDFArgumentException("Then second and third argument of '" + functionName + "' should be long type");
        }
        this.inspector0 = (BinaryObjectInspector) arg0;
        this.inspector1 = (LongObjectInspector) arg1;
        this.inspector2 = (LongObjectInspector) arg2;

        // 返回值类型
        return PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
    }

    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        if (deferredObjects[0].get() == null || deferredObjects[1].get() == null || deferredObjects[2].get() == null) {
            return null;
        }

        byte[] bytes = PrimitiveObjectInspectorUtils.getBinary(deferredObjects[0].get(), this.inspector0).getBytes();
        long start = PrimitiveObjectInspectorUtils.getLong(deferredObjects[1].get(), this.inspector1);
        long limit = PrimitiveObjectInspectorUtils.getLong(deferredObjects[2].get(), this.inspector2);

        try {
            Rbm64Bitmap bitmap = Rbm64Bitmap.bytesToBitmap(bytes);
            bitmap.subsetLimit(start, limit);
            return Rbm64Bitmap.bitmapToBytes(bitmap);
        } catch (IOException e) {
            throw new HiveException(e);
        }
    }

    public String getDisplayString(String[] children) {
        // 这里返回函数及其参数的描述
        return functionName + "(start, limit)";
    }
}

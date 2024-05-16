package com.data.market.udf;

import com.data.market.market.function.Rbm64Bitmap;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;

import java.io.IOException;

/**
 * 功能：Bitmap 中是否包含指定的 Value
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2024/5/17 06:51
 */
public class RbmBitmapContainsUDF extends GenericUDF {
    private transient BinaryObjectInspector inspector0;
    private transient LongObjectInspector inspector1;

    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        // 参数个数校验
        if (arguments.length != 2) {
            throw new UDFArgumentLengthException("The function 'rbm_bitmap_contains' only accepts 2 argument, but got " + arguments.length);
        }

        // 参数类型校验
        ObjectInspector arg0 = arguments[0];
        ObjectInspector arg1 = arguments[1];

        if (!(arg0 instanceof BinaryObjectInspector)) {
            throw new UDFArgumentException("First argument of rbm_bitmap_contains should be binary type.");
        }
        if (!(arg1 instanceof LongObjectInspector)) {
            throw new UDFArgumentException("Second argument of rbm_bitmap_contains should be long type.");
        }

        this.inspector0 = (BinaryObjectInspector) arg0;
        this.inspector1 = (LongObjectInspector) arg1;

        // 返回值类型
        return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
    }

    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        if (deferredObjects[0].get() == null || deferredObjects[1].get() == null) {
            return null;
        }

        byte[] bytes = PrimitiveObjectInspectorUtils.getBinary(deferredObjects[0].get(), this.inspector0).getBytes();
        Long value = PrimitiveObjectInspectorUtils.getLong(deferredObjects[1].get(), this.inspector1);

        try {
            Rbm64Bitmap bitmap = Rbm64Bitmap.bytesToBitmap(bytes);
            return bitmap.contains(value);
        } catch (IOException e) {
            throw new HiveException(e);
        }
    }

    public String getDisplayString(String[] children) {
        // 这里返回函数及其参数的描述
        return "rbm_bitmap_contains(bitmap, value)";
    }
}
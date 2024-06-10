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
import org.apache.hadoop.io.BytesWritable;

import java.io.IOException;

/**
 * 功能：从 Bitmap 中删除指定的数值
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2024/5/17 06:51
 */
public class RbmBitmapRemoveUDF extends GenericUDF {
    private static String functionName = "rbm_bitmap_remove";
    private transient BinaryObjectInspector inspector0;
    private transient LongObjectInspector inspector1;

    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        // 参数个数校验
        if (arguments.length != 2) {
            throw new UDFArgumentLengthException("The function '" + functionName + "' only accepts 2 argument, but got " + arguments.length);
        }

        // 参数类型校验
        ObjectInspector arg0 = arguments[0];
        ObjectInspector arg1 = arguments[1];

        if (!(arg0 instanceof BinaryObjectInspector)) {
            throw new UDFArgumentException("First argument of '" + functionName + "' should be binary type.");
        }
        if (!(arg1 instanceof LongObjectInspector)) {
            throw new UDFArgumentException("Second argument of '" + functionName + "' should be long type.");
        }

        this.inspector0 = (BinaryObjectInspector) arg0;
        this.inspector1 = (LongObjectInspector) arg1;

        // 返回值类型
        return PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
    }

    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        if (deferredObjects[0].get() == null || deferredObjects[1].get() == null) {
            return null;
        }

        byte[] bytes = PrimitiveObjectInspectorUtils.getBinary(deferredObjects[0].get(), this.inspector0).getBytes();
        Long value = PrimitiveObjectInspectorUtils.getLong(deferredObjects[1].get(), this.inspector1);

        try {
            Rbm64Bitmap bitmap = Rbm64Bitmap.fromBytes(bytes);
            bitmap.remove(value);
            return new BytesWritable(bitmap.toBytes());
        } catch (IOException e) {
            throw new HiveException(e);
        }
    }

    public String getDisplayString(String[] children) {
        // 这里返回函数及其参数的描述
        return functionName + "(bitmap, value)";
    }
}
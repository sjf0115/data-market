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
 * 功能：计算 Bitmap 中的最小值
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2024/5/17 06:51
 */
public class RbmBitmapMinUDF extends GenericUDF {
    private static String functionName = "rbm_bitmap_min";
    private transient BinaryObjectInspector inspector;

    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        // 参数个数校验
        if (arguments.length != 1) {
            throw new UDFArgumentLengthException("The function '" +  functionName +"' only accepts 1 argument, but got " + arguments.length);
        }

        // 参数类型校验
        ObjectInspector arg = arguments[0];
        if (!(arg instanceof BinaryObjectInspector)) {
            throw new UDFArgumentException("Argument of '" + functionName + "' should be binary type");
        }
        this.inspector = (BinaryObjectInspector) arg;

        // 返回值类型
        return PrimitiveObjectInspectorFactory.javaLongObjectInspector;
    }

    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        if (deferredObjects[0].get() == null || deferredObjects[1].get() == null) {
            return null;
        }

        byte[] bytes = PrimitiveObjectInspectorUtils.getBinary(deferredObjects[0].get(), this.inspector).getBytes();

        try {
            Rbm64Bitmap bitmap = Rbm64Bitmap.fromBytes(bytes);
            return bitmap.min();
        } catch (IOException e) {
            throw new HiveException(e);
        }
    }

    public String getDisplayString(String[] children) {
        // 这里返回函数及其参数的描述
        return functionName + "(bitmap)";
    }
}
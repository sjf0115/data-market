package com.data.market.udf;

import com.data.market.market.function.Rbm64Bitmap;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.io.IOException;

/**
 * 功能：将 bitmap 的 Base64 字符串转换为 bitmap
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2024/5/17 06:51
 */
public class RbmBitmapFromBase64UDF extends GenericUDF {
    private static String functionName = "rbm_bitmap_from_base64";
    private transient StringObjectInspector inspector;

    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        // 参数个数校验
        if (arguments.length != 1) {
            throw new UDFArgumentLengthException("The function '" + functionName + "' only accepts 1 argument, but got " + arguments.length);
        }

        // 参数类型校验
        ObjectInspector arg = arguments[0];
        if (!(arg instanceof StringObjectInspector)) {
            throw new UDFArgumentException("Argument of '" + functionName + "' should be string type");
        }
        this.inspector = (StringObjectInspector) arg;

        // 返回值类型
        return PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
    }

    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        if (deferredObjects[0].get() == null) {
            return null;
        }

        String str = PrimitiveObjectInspectorUtils.getString(deferredObjects[0].get(), this.inspector);
        Rbm64Bitmap bitmap = null;
        try {
            bitmap = Rbm64Bitmap.fromBase64(str);
            return bitmap.toBytes();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return bitmap;
    }

    public String getDisplayString(String[] children) {
        // 这里返回函数及其参数的描述
        return functionName + "(value)";
    }
}
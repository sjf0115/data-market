package com.data.market.udf;

import com.data.market.market.function.Rbm64Bitmap;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.io.parquet.serde.ArrayWritableObjectInspector;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.io.IOException;

/**
 * 功能：字符串 转换为 Bitmap
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2024/5/17 06:51
 */
public class RbmBitmapFromStringUDF extends GenericUDF {
    private transient StringObjectInspector inspector;

    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        // 参数个数校验
        if (arguments.length != 1) {
            throw new UDFArgumentLengthException("The function 'rbm_bitmap_from_str' only accepts 1 argument, but got " + arguments.length);
        }

        // 参数类型校验
        ObjectInspector arg = arguments[0];
        if (!(arg instanceof StringObjectInspector)) {
            throw new UDFArgumentException("Argument of rbm_bitmap_from_str should be string type");
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

        Rbm64Bitmap bitmap = new Rbm64Bitmap();
        try {
            String[] strArray = str.split(",");
            for (String s : strArray) {
                long v = Long.parseLong(s);
                bitmap.add(v);
            }
        } catch (NumberFormatException e) {
            throw new HiveException(e);
        }
        try {
            return Rbm64Bitmap.bitmapToBytes(bitmap);
        } catch (IOException e) {
            throw new HiveException(e);
        }
    }

    public String getDisplayString(String[] children) {
        // 这里返回函数及其参数的描述
        return "rbm_bitmap_from_str(value)";
    }
}
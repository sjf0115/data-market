package com.data.market.udf;

import com.data.market.market.function.Rbm64Bitmap;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.io.IOException;
import java.util.List;

/**
 * 功能：Array 转换为 Bitmap
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2024/5/17 06:51
 */
public class RbmBitmapFromArrayUDF extends GenericUDF {
    private static String functionName = "rbm_bitmap_from_array";
    private transient StandardListObjectInspector inspector;

    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        // 参数个数校验
        if (arguments.length != 1) {
            throw new UDFArgumentLengthException("The function '" + functionName + "' only accepts 1 argument, but got " + arguments.length);
        }

        // 参数类型校验
        ObjectInspector arg = arguments[0];
        if (!(arg instanceof StandardListObjectInspector)) {
            throw new UDFArgumentException("Argument of '" + functionName + "' should be array type");
        }
        this.inspector = (StandardListObjectInspector) arg;

        // 元素类型校验
        if (!(inspector.getListElementObjectInspector() instanceof LongObjectInspector)) {
            throw new UDFArgumentException("Argument of '" + functionName + "' should be array<long> type");
        }
        // 返回值类型
        return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
    }

    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        if (deferredObjects[0].get() == null) {
            return null;
        }
        List<Long> list = (List<Long>)inspector.getList(deferredObjects[0].get());
        try {
            Rbm64Bitmap bitmap = Rbm64Bitmap.fromArray(list);
            return bitmap.toBytes();
        } catch (IOException e) {
            throw new HiveException(e);
        }
    }

    public String getDisplayString(String[] children) {
        // 这里返回函数及其参数的描述
        return functionName + "(array)";
    }
}
package com.data.market.udf;

import com.data.market.market.function.Rbm64Bitmap;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 功能：Array 转换为 Bitmap
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2024/5/17 06:51
 */
@Description(name = "rbm_bitmap_from_array", value = "_FUNC_(array<value>) - Returns the bitmap of a array")
public class RbmBitmapFromArrayUDF extends GenericUDF {
    private static String functionName = "rbm_bitmap_from_array";
    private transient ListObjectInspector inspector;

    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        // 参数个数校验
        if (arguments.length != 1) {
            throw new UDFArgumentLengthException("The function '" + functionName + "' only accepts 1 argument, but got " + arguments.length);
        }

        // 参数类型校验
        ObjectInspector arg = arguments[0];
        if (!(arg instanceof ListObjectInspector)) {
            throw new UDFArgumentException("Argument of '" + functionName + "' should be array type");
        }
        this.inspector = (ListObjectInspector) arg;

        // 元素类型校验 Long 或者 Int
        if (!(inspector.getListElementObjectInspector() instanceof WritableLongObjectInspector) && !(inspector.getListElementObjectInspector() instanceof WritableIntObjectInspector)) {
            throw new UDFArgumentException("Argument of '" + functionName + "' should be long or int type, but got " + inspector.getTypeName());
        }
        // 返回值类型
        return PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
    }

    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        if (deferredObjects[0].get() == null) {
            return null;
        }
        List<Long> list = convertToLongList(deferredObjects);
        try {
            Rbm64Bitmap bitmap = Rbm64Bitmap.fromArray(list);
            return new BytesWritable(bitmap.toBytes());
        } catch (IOException e) {
            throw new HiveException(e);
        }
    }

    public String getDisplayString(String[] children) {
        // 这里返回函数及其参数的描述
        return functionName + "(array)";
    }

    private List<Long> convertToLongList(DeferredObject[] deferredObjects) throws HiveException {
        List<Long> longList = new ArrayList<>();
        List<?> list = inspector.getList(deferredObjects[0].get());

        for (int i = 0; i < list.size(); i++) {
            ObjectInspector eoi = inspector.getListElementObjectInspector();
            if (eoi instanceof WritableLongObjectInspector) {
                LongWritable value = (LongWritable)list.get(i);
                longList.add(value.get());
            } else {
                IntWritable value = (IntWritable)list.get(i);
                longList.add(Long.valueOf(value.get()));
            }
        }
        return longList;
    }
}
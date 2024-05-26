package com.data.market.udaf;

import com.data.market.market.function.Rbm64Bitmap;
import com.google.common.base.Objects;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.io.IOException;

/**
 * 功能：计算位图 Bitmap 列的交集(与操作)，并返回一个新的位图 Bitmap
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2024/5/23 22:11
 */
public class RbmGroupBitmapAndUDAF extends AbstractGenericUDAFResolver {
    private static String functionName = "rbm_group_bitmap_and";
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] arguments) throws SemanticException {
        // 参数个数校验
        if (arguments.length != 1) {
            throw new UDFArgumentLengthException("The function '" + functionName + "' only accepts 1 argument, but got " + arguments.length);
        }

        // 参数类型校验
        if (arguments[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0, "Only primitive type arguments are accepted but " + arguments[0].getTypeName() + " is passed.");
        }
        PrimitiveObjectInspector.PrimitiveCategory primitiveCategory = ((PrimitiveTypeInfo) arguments[0]).getPrimitiveCategory();
        if (primitiveCategory == PrimitiveObjectInspector.PrimitiveCategory.BINARY) {
            // 支持 BINARY 类型的聚合
            return new MergeEvaluator();
        } else {
            throw new UDFArgumentTypeException(0, "Only binary type arguments are accepted but " + arguments[0].getTypeName() + " is passed.");
        }
    }

    public static class MergeEvaluator extends GenericUDAFEvaluator {
        private BinaryObjectInspector inputOI;
        private BinaryObjectInspector outputOI;

        @AggregationType(estimable = true)
        static class BitmapAggBuffer extends AbstractAggregationBuffer {
            Rbm64Bitmap bitmap;
            public BitmapAggBuffer () {
                bitmap = new Rbm64Bitmap();
            }
        }

        // 返回类型。这里定义返回类型为 Binary
        @Override
        public ObjectInspector init(Mode mode, ObjectInspector[] parameters) throws HiveException {
            if (parameters.length != 1) {
                throw new UDFArgumentLengthException("The function '" + functionName + "' only accepts 1 argument, but got " + parameters.length);
            }
            super.init(mode, parameters);
            if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
                this.inputOI = (BinaryObjectInspector) parameters[0];
            } else {
                this.outputOI = (BinaryObjectInspector) parameters[0];
            }
            return PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
        }

        // 创建新的聚合计算需要的内存，用来存储 Mapper, Combiner, Reducer 运算过程中的聚合。
        @Override
        public AggregationBuffer getNewAggregationBuffer() {
            BitmapAggBuffer bitmapAggBuffer = new BitmapAggBuffer();
            reset(bitmapAggBuffer);
            return bitmapAggBuffer;
        }

        @Override
        public void reset(AggregationBuffer agg) {
            BitmapAggBuffer bitmapAggBuffer = (BitmapAggBuffer)agg;
            bitmapAggBuffer.bitmap = new Rbm64Bitmap();
        }

        // 遍历输入参数
        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) {
            Object param = parameters[0];
            if (Objects.equal(param, null)) {
                return;
            }
            BitmapAggBuffer bitmapAggBuffer = (BitmapAggBuffer) agg;
            try {
                byte[] bytes = PrimitiveObjectInspectorUtils.getBinary(param, inputOI).getBytes();
                Rbm64Bitmap bitmap = Rbm64Bitmap.fromBytes(bytes);
                bitmapAggBuffer.bitmap.and(bitmap);
            } catch (NumberFormatException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        // Mapper,Combiner 结束要返回的结果
        @Override
        public Object terminatePartial(AggregationBuffer agg) {
            return terminate(agg);
        }

        // 合并: Combiner 合并 Mapper 返回的结果, Reducer 合并 Mapper 或 Combiner 返回的结果
        @Override
        public void merge(AggregationBuffer agg, Object partial) {
            if (Objects.equal(partial, null)){
                return;
            }
            BitmapAggBuffer bitmapAggBuffer = (BitmapAggBuffer)agg;
            try {
                byte[] bytes = PrimitiveObjectInspectorUtils.getBinary(partial, outputOI).getBytes();
                Rbm64Bitmap bitmap = Rbm64Bitmap.fromBytes(bytes);
                bitmapAggBuffer.bitmap.and(bitmap);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        // 输出最终聚合结果
        @Override
        public Object terminate(AggregationBuffer agg) {
            BitmapAggBuffer bitmapAggBuffer = (BitmapAggBuffer) agg;
            byte[] bytes = null;
            try {
                bytes = bitmapAggBuffer.bitmap.toBytes();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return bytes;
        }
    }
}
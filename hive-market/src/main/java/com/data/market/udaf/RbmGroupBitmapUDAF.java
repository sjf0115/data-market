package com.data.market.udaf;

import com.data.market.market.function.Rbm64Bitmap;
import com.google.common.base.Objects;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableBinaryObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.BytesWritable;

import java.io.IOException;

/**
 * 功能：根据整数列聚合计算返回一个位图 Bitmap
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2024/5/23 22:11
 */
public class RbmGroupBitmapUDAF extends AbstractGenericUDAFResolver {
    private static String functionName = "rbm_group_bitmap";
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
        if (primitiveCategory == PrimitiveObjectInspector.PrimitiveCategory.LONG || primitiveCategory == PrimitiveObjectInspector.PrimitiveCategory.INT) {
            // 支持 Long 或者 Int 类型的聚合
            return new MergeEvaluator();
        } else {
            throw new UDFArgumentTypeException(0, "Only long or int type arguments are accepted but " + arguments[0].getTypeName() + " is passed.");
        }
    }

    public static class MergeEvaluator extends GenericUDAFEvaluator {
        private PrimitiveObjectInspector inputOI;
        private WritableBinaryObjectInspector outputOI;

        static class BitmapAggBuffer implements AggregationBuffer {
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
                this.inputOI = (PrimitiveObjectInspector) parameters[0];
            } else {
                this.outputOI = (WritableBinaryObjectInspector) parameters[0];
            }
            return PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
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

        // Map阶段：遍历输入参数
        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            Object param = parameters[0];
            if (Objects.equal(param, null)) {
                return;
            }
            BitmapAggBuffer bitmapAggBuffer = (BitmapAggBuffer) agg;
            try {
                Long value = PrimitiveObjectInspectorUtils.getLong(param, inputOI);
                bitmapAggBuffer.bitmap.add(value);
            } catch (NumberFormatException e) {
                throw new HiveException(e);
            }
        }

        // Mapper,Combiner 结束要返回的结果
        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            return terminate(agg);
        }

        // 合并: Combiner 合并 Mapper 返回的结果, Reducer 合并 Mapper 或 Combiner 返回的结果
        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            if (Objects.equal(partial, null)){
                return;
            }
            BitmapAggBuffer bitmapAggBuffer = (BitmapAggBuffer)agg;
            try {
                BytesWritable bw = (BytesWritable)partial;
                byte[] bytes = bw.getBytes();
                Rbm64Bitmap bitmap = Rbm64Bitmap.fromBytes(bytes);
                bitmapAggBuffer.bitmap.or(bitmap);
            } catch (IOException e) {
                throw new HiveException(e);
            }
        }

        // 输出最终聚合结果
        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            BitmapAggBuffer bitmapAggBuffer = (BitmapAggBuffer) agg;
            try {
                byte[] bytes = bitmapAggBuffer.bitmap.toBytes();
                return new BytesWritable(bytes);
            } catch (IOException e) {
                throw new HiveException(e);
            }
        }
    }
}
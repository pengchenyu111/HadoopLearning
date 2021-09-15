package com.pcy.hive;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * 自定义UDF函数：一进一出
 * 计算输入字符串的长度
 * <p>
 * 步骤：
 * 1.上传到服务器/opt/module/data/myudf.jar
 * 2.将jar包添加到hive的path
 * hive (default)> add jar /opt/module/data/myudf.jar;
 * 3.创建临时函数与开发好的 java class 关联
 * hive (default)> create temporary function my_len as "com.atguigu.hive. MyStringLengthUDF";
 * 4.即可在 hql 中使用自定义的函数
 * hive (default)> select ename,my_len(ename) ename_len from emp;
 *
 * @author PengChenyu
 * @since 2021-09-09 13:40:42
 */
public class MyStringLengthUDF extends GenericUDF {
    /**
     * 输入参数校验
     *
     * @param objectInspectors
     * @return
     * @throws UDFArgumentException
     */
    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        // 判断输入参数的长度
        if (objectInspectors.length != 1) {
            throw new UDFArgumentException("Input args err!");
        }
        //判断输入参数的类型
        if (!objectInspectors[0].getCategory().equals(ObjectInspector.Category.PRIMITIVE)) {
            throw new UDFArgumentTypeException(0, "type err!");
        }
        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        if (deferredObjects[0].get() == null) {
            return 0;
        }
        return deferredObjects[0].get().toString().length();
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "";
    }
}

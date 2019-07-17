package com.jing.text01;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * @author RanMoAnRan
 * @ClassName: TestUDF
 * @projectName hadoop-project
 * @description: TODO
 * @date 2019/7/9 15:07
 */
//自定义hive函数
public class TestUDF extends UDF {
    public Text evaluate(final Text s) {
        if (null == s) {
            return null;
        }
        //返回大写字母
        return new Text(s.toString().toUpperCase());
    }
}

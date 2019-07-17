package com.jing.text01;

import com.google.gson.Gson;
;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.*;

/**
 * @author RanMoAnRan
 * @ClassName: JsonUDF
 * @projectName hadoop-project
 * @description: TODO
 * @date 2019/7/9 16:56
 */
public class JsonUDF extends UDF {

    public NullWritable evaluate(final Text s) throws IOException {
        if (s!=null) {
            Gson gson = new Gson();

            JsonTest jsonTest = gson.fromJson(s.toString(), JsonTest.class);
            StringBuffer stringBuffer = new StringBuffer();
            stringBuffer.append(jsonTest.getMovie()).append("\t").append(jsonTest.getRate()).append("\t").append(jsonTest.getTimeStamp()).append("\t").append(jsonTest.getUid());
            System.out.println(stringBuffer);

            FileOutputStream fileOutputStream = new FileOutputStream(new File("/a.txt"));
            fileOutputStream.write(stringBuffer.toString().getBytes());
            fileOutputStream.write("\n".getBytes());

            fileOutputStream.close();
        }
        return null;
    }

}

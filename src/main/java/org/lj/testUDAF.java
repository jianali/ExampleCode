package org.lj;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

/**
 * Hello world!
 *测试udaf函数，求出每列的最大值
 */
public class testUDAF extends UDAF
{
//    public static void main( String[] args )
//    {
//        System.out.println( "Hello World!" );
//    }
public static class GroupByUDAFEvaluator implements UDAFEvaluator {
    public static class PartialResult {
        String returnValue1;
        String returnValue2;
    }

    private PartialResult partial;

    public void init() {
        partial = null;
    }

    public boolean iterate(String value1, String value2) {
        if (value1 == null && value2 == null) {
            return true;
        }

        if (partial == null) {
            partial = new PartialResult();
            partial.returnValue1 = "";
            partial.returnValue2 = "";
        }

        partial.returnValue1=value1;
        partial.returnValue2=value2;
        return true;
    }

    /*
     * 需要实现的第二个方法
     * 返回部分结果
     */
    public PartialResult terminatePartial() {
        return partial;
    }

    /*
     * 需要实现的第三个方法
     * 将两个部分结果合并
     */
    public boolean merge(PartialResult othe) {
        if (othe == null) {
            return true;
        }

        if (partial == null) {
            partial = new PartialResult();

            partial.returnValue1 = othe.returnValue1;
            partial.returnValue2 = othe.returnValue2;
        } else {
            //对比结果，返回字符串长度最长的字符串
            if (partial.returnValue1.length() <= othe.returnValue1.length())
                partial.returnValue1 = othe.returnValue1;

            //对比结果，返回字符串长度最短的字符串
            if (partial.returnValue2.length() >= othe.returnValue2.length())
                partial.returnValue2 = othe.returnValue2;
        }
        return true;
    }

    /*
     * 需要实现的第四个方法
     * 构造返回值类型，返回结果
     */
    public String terminate() {
        return "the max string is:" + partial.returnValue1 + " the min string is:" + partial.returnValue2;
    }
}

}

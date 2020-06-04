package flinkdemo;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.omg.CORBA.Environment;

/**
 * @author AlanZhang 1581404769@qq.com
 * @date 2020/6/4 15:18
 * @project quickstart
 * @@version 0.1
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        // 初始化flink运行上下文
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 创建DataSet
        DataSet<String> text = env.fromElements(
                "Who's there?",
                "I think I hear them. Stand, ho! Who's there?");

        DataSet<Tuple2<String, Integer>> wordCounts = text
                .flatMap(new LineSplitter())
                .groupBy(0)
                .sum(1);

        wordCounts.print();
    }
}

final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
    @Override
    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
        // 将文本分割
        String[] tokens = value.toLowerCase().split("\\W+");
        for (String token : tokens) {
            if (token.length() > 0) {
                out.collect(new Tuple2<String, Integer>(token, 1));
            }
        }
    }
}

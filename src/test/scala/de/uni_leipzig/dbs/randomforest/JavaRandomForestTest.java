package de.uni_leipzig.dbs.randomforest;

import de.uni_leipzig.dbs.api.java.RandomForest;
import de.uni_leipzig.dbs.api.java.RandomForestBuilder;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;

import java.net.URLDecoder;
import java.util.Arrays;
import java.util.Vector;

public class JavaRandomForestTest {

    public static void main(String[] args) throws Exception {

//        emulates local environment on java collections for improved performance
//        ExecutionEnvironment env = ExecutionEnvironment.createCollectionsEnvironment();
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String filepath = URLDecoder.decode(JavaRandomForestTest.class.getResource("/musicbrainz/training_musicbrainz_softTFIDF[1_2].csv").toURI().getPath(), "UTF-8");
        DataSet<Tuple4<Boolean, Double, Double, Double>> data = env.readCsvFile(filepath)
                .ignoreFirstLine()
                .fieldDelimiter(";")
                .includeFields(false, false, true, true, true, true)
                .types(Boolean.class, Double.class, Double.class, Double.class);


        DataSet<Tuple2<Double, Vector<Double>>> dataLV = data.map(new RawDataToInput());
        RandomForest model = new RandomForestBuilder()
                .setNumTrees(500)
                .build();
        model.fit(dataLV);
        model.save();
        model.load();
        DataSet<Vector<Double>> dataV = dataLV.map(new LabelVectorToVector());
        DataSet<Tuple2<Double, Vector<Double>>> predictedData = model.predict(dataV);

        System.out.println("(Accuracy, Precision, Recall): " + model.evaluateBinaryClassification(dataLV));
    }

    public static final class RawDataToInput implements MapFunction<Tuple4<Boolean, Double, Double, Double>, Tuple2<Double, Vector<Double>>> {
        @Override
        public Tuple2<Double, Vector<Double>> map(Tuple4<Boolean, Double, Double, Double> t) throws Exception {
            Double[] values = new Double[3];
            Double label;
            if (t.f0) label = 1.0;
            else label = -1.0;
            values[0] = t.f1;
            values[1] = t.f2;
            values[2] = t.f3;
            Vector<Double> vec = new Vector<Double>(Arrays.asList(values));
            return new Tuple2(label, vec);
        }
    }

    public static final class LabelVectorToVector implements MapFunction<Tuple2<Double, Vector<Double>>, Vector<Double>> {
        @Override
        public Vector<Double> map(Tuple2<Double, Vector<Double>> value) throws Exception {
            return value.f1;
        }
    }

}

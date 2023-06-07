package demo.app;

import org.apache.flink.api.common.functions.MapFunction;

import java.util.List;

public class LongToDemoRecord implements MapFunction<Long, DemoRecord> {
    @Override
    public DemoRecord map(Long value) throws Exception {
        if (value == null) {
            return null;
        }
        int intValue = value.intValue();
        if (intValue >= 0 && intValue < testData.size()) {
            return testData.get(intValue);
        }
        return null;
    }

    // T, X, M, J, G, B, F, H, N, K, Y, A, R, V, S, L, Q, E, U, D
    // 46, 89, 15, 72, 142, 57, 121, 162, 38, 1, 105, 83, 94, 23, 157, 52, 116, 9, 61, 134
    // 0.692, -2.876, 1.345, 0.104, -3.921, 5.209, 2.678, -0.375, 4.821, -1.092, 3.569, -4.321, 1.988, 0.824, -2.187, 6.453, -0.759, 3.217, -5.432, 2.104
    public static List<DemoRecord> testData = List.of(
            new DemoRecord("T", 46, 0.692),
            new DemoRecord("X", 89, -2.876),
            new DemoRecord("M", 15, 1.345),
            new DemoRecord("J", 72, 0.104),
            new DemoRecord("G", 142, -3.921),
            new DemoRecord("B", 57, 5.209),
            new DemoRecord("F", 121, 2.678),
            new DemoRecord("H", 162, -0.375),
            new DemoRecord("N", 38, 4.821),
            new DemoRecord("K", 1, -1.092),
            new DemoRecord("Y", 105, 3.569),
            new DemoRecord("A", 83, -4.321),
            new DemoRecord("R", 94, 1.988),
            new DemoRecord("V", 23, 0.824),
            new DemoRecord("S", 157, -2.187),
            new DemoRecord("L", 52, 6.453),
            new DemoRecord("Q", 116, -0.759),
            new DemoRecord("E", 9, 3.217),
            new DemoRecord("U", 61, -5.432),
            new DemoRecord("D", 134, 2.104)
    );
}

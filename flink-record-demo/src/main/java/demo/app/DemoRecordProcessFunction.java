package demo.app;

import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class DemoRecordProcessFunction extends KeyedProcessFunction<String, DemoRecord, Double> {
    private transient ReducingState<Double> reducingState;

    @Override
    public void open(Configuration parameters) {
        reducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<>(
                "reducing-state", Double::sum, Double.class));
    }

    @Override
    public void processElement(DemoRecord value, KeyedProcessFunction<String, DemoRecord, Double>.Context ctx, Collector<Double> out) throws Exception {
        reducingState.add(value.c());
        Double next = reducingState.get();
        out.collect(next);
    }
}

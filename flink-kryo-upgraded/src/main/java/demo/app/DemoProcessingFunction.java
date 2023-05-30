package demo.app;

import demo.data.IntOpaqueWrapper;
import demo.data.IntOpaqueWrapperAggregateFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class DemoProcessingFunction extends KeyedProcessFunction<IntOpaqueWrapper, IntOpaqueWrapper, IntOpaqueWrapper> {
    private transient ValueState<IntOpaqueWrapper> valueState;
    private transient ListState<IntOpaqueWrapper> listState;
    private transient ReducingState<IntOpaqueWrapper> reducingState;
    private transient AggregatingState<IntOpaqueWrapper, IntOpaqueWrapper> aggregatingState;
    private transient MapState<IntOpaqueWrapper, IntOpaqueWrapper> mapState;

    @Override
    public void open(Configuration parameters) {
        final TypeInformation<IntOpaqueWrapper> intOpaqueWrapperTypeInformation = new GenericTypeInfo<>(IntOpaqueWrapper.class);

        valueState = getRuntimeContext().getState(new ValueStateDescriptor<>(
                "value-state",
                intOpaqueWrapperTypeInformation));

        listState = getRuntimeContext().getListState(new ListStateDescriptor<>(
                "list-state",
                intOpaqueWrapperTypeInformation));

        reducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<>(
                "reducing-state",
                IntOpaqueWrapper::sum,
                intOpaqueWrapperTypeInformation));

        aggregatingState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<>(
                "aggregating-state",
                new IntOpaqueWrapperAggregateFunction(),
                intOpaqueWrapperTypeInformation));

        mapState = getRuntimeContext().getMapState(new MapStateDescriptor<>(
                "map-state",
                intOpaqueWrapperTypeInformation,
                intOpaqueWrapperTypeInformation));

        final Logger logger = LoggerFactory.getLogger(DemoProcessingFunction.class);
        logger.info("opened");
    }

    @Override
    public void processElement(IntOpaqueWrapper v, KeyedProcessFunction<IntOpaqueWrapper, IntOpaqueWrapper, IntOpaqueWrapper>.Context ctx, Collector<IntOpaqueWrapper> out) throws Exception {
        final StringBuilder sb = new StringBuilder();
        sb.append(String.format("process. v=%s.", v));

        processValueState(valueState, v, sb);
        processListState(listState, v, sb);
        processReducingState(reducingState, v, sb);
        processAggregatingState(aggregatingState, v, sb);
        processMapState(mapState, v, sb);

        final Logger logger = LoggerFactory.getLogger(DemoProcessingFunction.class);
        logger.info(sb.toString());

        out.collect(valueState.value());
    }

    static void processValueState(ValueState<IntOpaqueWrapper> state, IntOpaqueWrapper v, StringBuilder sb) throws IOException {
        IntOpaqueWrapper current = state.value();
        String currentDesc = (current == null) ? "<null>" : current.toString();
        IntOpaqueWrapper currentNotNull = (current == null) ? IntOpaqueWrapper.create(0) : current;
        IntOpaqueWrapper next = IntOpaqueWrapper.sum(currentNotNull, v);
        state.update(next);
        sb.append(String.format(" value:%s->%s.", currentDesc, next));
    }
    static void processListState(ListState<IntOpaqueWrapper> state, IntOpaqueWrapper v, StringBuilder sb) throws Exception {
        state.add(v);
        sb.append(" list:");
        boolean isFirst = true;
        for (IntOpaqueWrapper wrapped : state.get()) {
            if (isFirst) {
                isFirst = false;
            } else {
                sb.append(",");
            }
            sb.append(wrapped.toString());
        }
        sb.append(". ");
    }
    static void processReducingState(ReducingState<IntOpaqueWrapper> state, IntOpaqueWrapper v, StringBuilder sb) throws Exception {
        IntOpaqueWrapper current = state.get();
        state.add(v);
        IntOpaqueWrapper next = state.get();
        sb.append(String.format(" reducing:%s->%s.", current, next));
    }
    static void processAggregatingState(AggregatingState<IntOpaqueWrapper, IntOpaqueWrapper> state, IntOpaqueWrapper v, StringBuilder sb) throws Exception {
        IntOpaqueWrapper current = state.get();
        String currentDesc = (current == null) ? "<null>" : current.toString();
        state.add(v);
        IntOpaqueWrapper next = state.get();
        sb.append(String.format(" aggregating:%s->%s.", currentDesc, next));
    }
    static void processMapState(MapState<IntOpaqueWrapper, IntOpaqueWrapper> state, IntOpaqueWrapper v, StringBuilder sb) throws Exception {
        IntOpaqueWrapper current = state.get(v);
        String currentDesc = (current == null) ? "<null>" : current.toString();
        IntOpaqueWrapper currentNotNull = (current == null) ? IntOpaqueWrapper.create(0) : current;
        IntOpaqueWrapper next = IntOpaqueWrapper.sum(currentNotNull, v);
        state.put(v, next);
        sb.append(String.format(" map:%s->%s.", currentDesc, next));
    }
}

package demo.app;

import demo.data.IntOpaqueWrapper;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.IntegerTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DemoProcessingFunction extends KeyedProcessFunction<IntOpaqueWrapper, Long, Long> {
    private transient ValueState<Long> state;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Long> descriptor =
                new ValueStateDescriptor<>(
                        "processing-state",
                        IntegerTypeInfo.of(Long.class));
        state = getRuntimeContext().getState(descriptor);

        final Logger logger = LoggerFactory.getLogger(DemoProcessingFunction.class);
        logger.info("opened");
    }

    @Override
    public void processElement(Long value, KeyedProcessFunction<IntOpaqueWrapper, Long, Long>.Context ctx, Collector<Long> out) throws Exception {
        Long current = state.value();
        String currentDesc = (current == null) ? "<null>" : Long.toString(current);
        long currentValue = (current == null) ? 0L : current;
        long newValue = currentValue + value;
        state.update(newValue);
        IntOpaqueWrapper key = ctx.getCurrentKey();

        final Logger logger = LoggerFactory.getLogger(DemoProcessingFunction.class);
        logger.info(String.format("process. key=%s. current=%s. v=%d. out=%d", key, currentDesc, value, newValue));

        out.collect(newValue);
    }
}

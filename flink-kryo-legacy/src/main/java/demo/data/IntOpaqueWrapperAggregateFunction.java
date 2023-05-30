package demo.data;

import org.apache.flink.api.common.functions.AggregateFunction;

public class IntOpaqueWrapperAggregateFunction implements
        AggregateFunction<IntOpaqueWrapper, IntOpaqueWrapper, IntOpaqueWrapper> {
    @Override
    public IntOpaqueWrapper createAccumulator() {
        return IntOpaqueWrapper.create(0);
    }

    @Override
    public IntOpaqueWrapper add(IntOpaqueWrapper value, IntOpaqueWrapper accumulator) {
        return IntOpaqueWrapper.sum(value, accumulator);
    }

    @Override
    public IntOpaqueWrapper getResult(IntOpaqueWrapper accumulator) {
        return accumulator;
    }

    @Override
    public IntOpaqueWrapper merge(IntOpaqueWrapper a, IntOpaqueWrapper b) {
        return IntOpaqueWrapper.sum(a, b);
    }
}

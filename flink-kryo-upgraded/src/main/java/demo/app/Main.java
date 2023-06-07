package demo.app;

import demo.data.IntOpaqueWrapper;
import demo.data.IntOpaqueWrapperKryo2Serializer;
import demo.data.IntOpaqueWrapperKryo5Serializer;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.client.cli.CliFrontend;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.runtime.jobgraph.RestoreMode;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
//    public final static String KRYO_V2_NATIVE_SAVEPOINT_PATH = "../native-savepoints/savepoint-0bed3c-7f053b08f084";
//    public final static String KRYO_V5_NATIVE_SAVEPOINT_PATH = "../native-savepoints/savepoint-51d0d3-da9ea6535060";

    @SuppressWarnings("deprecation")
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(Main.class);
        try {
            try {
                logger.info("Starting up.");

                final Configuration flinkConfiguration = new Configuration();
//                flinkConfiguration.set(DeploymentOptions.ATTACHED, false);
                // Uncomment to debug restoring from savepoint.
//                flinkConfiguration.set(SavepointConfigOptions.SAVEPOINT_PATH, KRYO_V5_NATIVE_SAVEPOINT_PATH);
//                flinkConfiguration.set(SavepointConfigOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE, false);
//                flinkConfiguration.set(SavepointConfigOptions.RESTORE_MODE, RestoreMode.NO_CLAIM);
//                logger.info("SAVEPOINT_PATH={}", flinkConfiguration.get(SavepointConfigOptions.SAVEPOINT_PATH));
                final StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment(flinkConfiguration);
                final ExecutionConfig executionConfig = streamEnv.getConfig();
                executionConfig.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());
                streamEnv.setParallelism(1);
                streamEnv.setStateBackend(new HashMapStateBackend());

                executionConfig.enableForceKryo();

                executionConfig.addDefaultKryoSerializer(IntOpaqueWrapper.class, IntOpaqueWrapperKryo2Serializer.class);
                executionConfig.registerTypeWithKryoSerializer(IntOpaqueWrapper.class, IntOpaqueWrapperKryo2Serializer.class);
                executionConfig.registerTypeWithKryoSerializer(IntOpaqueWrapper.class, new IntOpaqueWrapperKryo2Serializer());
                streamEnv.addDefaultKryoSerializer(IntOpaqueWrapper.class, IntOpaqueWrapperKryo2Serializer.class);
                streamEnv.registerTypeWithKryoSerializer(IntOpaqueWrapper.class, IntOpaqueWrapperKryo2Serializer.class);
                streamEnv.registerTypeWithKryoSerializer(IntOpaqueWrapper.class, new IntOpaqueWrapperKryo2Serializer());

                executionConfig.addDefaultKryo5Serializer(IntOpaqueWrapper.class, IntOpaqueWrapperKryo5Serializer.class);
                executionConfig.registerTypeWithKryo5Serializer(IntOpaqueWrapper.class, IntOpaqueWrapperKryo5Serializer.class);
                executionConfig.registerTypeWithKryo5Serializer(IntOpaqueWrapper.class, new IntOpaqueWrapperKryo5Serializer());
                streamEnv.addDefaultKryo5Serializer(IntOpaqueWrapper.class, IntOpaqueWrapperKryo5Serializer.class);
                streamEnv.registerTypeWithKryo5Serializer(IntOpaqueWrapper.class, IntOpaqueWrapperKryo5Serializer.class);
                streamEnv.registerTypeWithKryo5Serializer(IntOpaqueWrapper.class, new IntOpaqueWrapperKryo5Serializer());

                final NumberSequenceSource source = new NumberSequenceSource(1, 20);
                final DataStreamSource<Long> stream = streamEnv.fromSource(source, WatermarkStrategy.noWatermarks(), "number-sequence-source");
                stream.name("number-sequence-stream");
                stream.setParallelism(1);

                final TypeInformation<IntOpaqueWrapper> intOpaqueWrapperTypeInformation = new GenericTypeInfo<>(IntOpaqueWrapper.class);
                final SingleOutputStreamOperator<IntOpaqueWrapper> wrappedIntegerStream = stream.map(
                        l -> IntOpaqueWrapper.create((int) ((long) l)), intOpaqueWrapperTypeInformation);
                wrappedIntegerStream.name("wrapped-integer-stream");
                wrappedIntegerStream.setParallelism(1);

                final SingleOutputStreamOperator<IntOpaqueWrapper> pausedStream = wrappedIntegerStream.map(l -> { Thread.sleep(5000); return l; });
                pausedStream.name("paused-stream");
                pausedStream.setParallelism(1);

                final KeyedStream<IntOpaqueWrapper, IntOpaqueWrapper> keyedStream = pausedStream.keyBy(
                        IntOpaqueWrapper::getModulus5, intOpaqueWrapperTypeInformation);

                final SingleOutputStreamOperator<IntOpaqueWrapper> processedStream = keyedStream.process(new DemoProcessingFunction());
                processedStream.name("processed-stream");
                processedStream.setParallelism(1);

                Sink<IntOpaqueWrapper> sink = new LogSink();
                DataStreamSink<IntOpaqueWrapper> dataStreamSink = processedStream.sinkTo(sink);
                dataStreamSink.name("log-sink");
                dataStreamSink.setParallelism(1);

                logger.info("Executing streaming app.");
                JobExecutionResult jobExecutionResult = streamEnv.execute("flink-kryo-demo");
                JobID jobID = jobExecutionResult.getJobID();
                logger.info("Flink execute() complete. jobID={}", jobID);
            } catch (Exception e) {
                System.out.printf("println top level exception. %s: %s%n",
                        e.getClass().getSimpleName(), e.getMessage());
                logger.error("top level application exception", e);
            }
        } catch (RuntimeException e) {
            System.out.printf("println top level RuntimeException. %s: %s%n",
                    e.getClass().getSimpleName(), e.getMessage());
            logger.error("top level application RuntimeException", e);
        }

        logger.info("exiting...");
    }

    public static class LogSink implements Sink<IntOpaqueWrapper> {
        @Override
        public SinkWriter<IntOpaqueWrapper> createWriter(InitContext context) {
            return new LogSinkWriter();
        }
    }

    public static class LogSinkWriter implements SinkWriter<IntOpaqueWrapper> {
        final Logger logger = LoggerFactory.getLogger(LogSinkWriter.class);

        @Override
        public void write(IntOpaqueWrapper element, Context context) {
            logger.info("write {}", element);
        }

        @Override
        public void flush(boolean endOfInput) {
            logger.info("flush endOfInput={}", endOfInput);
        }

        @Override
        public void close() {
            logger.info("close");
        }
    }
}

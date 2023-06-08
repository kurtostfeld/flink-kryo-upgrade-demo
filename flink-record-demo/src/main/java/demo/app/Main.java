package demo.app;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    @SuppressWarnings("deprecation")
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(Main.class);
        try {
            try {
                logger.info("Starting up.");

                final Configuration flinkConfiguration = new Configuration();
                final StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment(flinkConfiguration);
                final ExecutionConfig executionConfig = streamEnv.getConfig();
                executionConfig.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());
                streamEnv.setParallelism(1);
                streamEnv.setStateBackend(new HashMapStateBackend());

                executionConfig.enableForceKryo();

                final NumberSequenceSource source = new NumberSequenceSource(0, 19);
                final DataStreamSource<Long> stream = streamEnv.fromSource(source, WatermarkStrategy.noWatermarks(), "number-sequence-source");
                stream.name("number-sequence-stream");
                stream.setParallelism(1);

                final SingleOutputStreamOperator<DemoRecord> mappedStream = stream.map(new LongToDemoRecord());
                mappedStream.name("record-stream");
                mappedStream.setParallelism(1);

                final KeyedStream<DemoRecord, String> keyedStream = mappedStream.keyBy(DemoRecord::a);

                final SingleOutputStreamOperator<Double> processedStream = keyedStream.process(new DemoRecordProcessFunction());
                processedStream.name("processed-stream");
                processedStream.setParallelism(1);

                Sink<Double> sink = new LogSink();
                DataStreamSink<Double> dataStreamSink = processedStream.sinkTo(sink);
                dataStreamSink.name("log-sink");
                dataStreamSink.setParallelism(1);

                logger.info("Executing streaming app.");
                JobExecutionResult jobExecutionResult = streamEnv.execute("flink-record-demo");
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

    public static class LogSink implements Sink<Double> {
        @Override
        public SinkWriter<Double> createWriter(InitContext context) {
            return new LogSinkWriter();
        }
    }

    public static class LogSinkWriter implements SinkWriter<Double> {
        final Logger logger = LoggerFactory.getLogger(LogSinkWriter.class);

        @Override
        public void write(Double element, Context context) {
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

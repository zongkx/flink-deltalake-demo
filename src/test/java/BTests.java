/**
 * @author zongkx
 */

import io.delta.flink.sink.DeltaSink;
import io.delta.flink.source.DeltaSource;
import lombok.SneakyThrows;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.CloseableIterator;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Arrays;

/**
 * @author zongkx
 */

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BTests {


    public static final RowType TEST_ROW_TYPE = new RowType(Arrays.asList(
            new RowType.RowField("id", new IntType()),
            new RowType.RowField("name", new VarCharType(VarCharType.MAX_LENGTH))
    ));

    private static DeltaSink<RowData> createDeltaSink(String deltaTablePath) {
        return DeltaSink.forRowData(new Path(deltaTablePath), new Configuration(), TEST_ROW_TYPE)
                .withMergeSchema(true)
                .withPartitionColumns("id").build();
    }

    @BeforeAll
    public void before() {


    }

    public DataStream<RowData> createBoundedDeltaSourceAllColumns(
            StreamExecutionEnvironment env,
            String deltaTablePath) {

        DeltaSource<RowData> deltaSource = DeltaSource
                .forBoundedRowData(
                        new Path(deltaTablePath),
                        new Configuration())
                .build();

        return env.fromSource(deltaSource, WatermarkStrategy.noWatermarks(), "delta-test-data");
    }

    @Test
    @SneakyThrows
    void write() {
        String tablePath = "delta-test-data";
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.disableOperatorChaining();
        env.addSource(new RichParallelSourceFunction<RowData>() {
                    @Override
                    public void run(SourceContext<RowData> ctx) throws Exception {
                        GenericRowData genericRowData = GenericRowData.of(1, StringData.fromString("1111"));
                        ctx.collect(genericRowData);
                    }

                    @Override
                    public void cancel() {

                    }
                }).setParallelism(1)
                .sinkTo(createDeltaSink(tablePath)).setParallelism(1);
        env.execute("Issue 362 - Delta");
        env.close();
    }

    @Test
    @SneakyThrows
    void read() {
        String tablePath = "delta-test-data";
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.disableOperatorChaining();
        DataStream<RowData> boundedDeltaSourceAllColumns = createBoundedDeltaSourceAllColumns(env, tablePath);
        CloseableIterator<RowData> rowDataCloseableIterator = boundedDeltaSourceAllColumns.executeAndCollect();
        while (rowDataCloseableIterator.hasNext()) {
            RowData next = rowDataCloseableIterator.next();
            int anInt = next.getInt(0);
            System.out.println(anInt);
        }
        env.execute("Issue 362 - Delta");
        env.close();
    }

}
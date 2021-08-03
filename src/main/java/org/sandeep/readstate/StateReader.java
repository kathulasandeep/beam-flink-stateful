package org.sandeep.readstate;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.state.api.ExistingSavepoint;
import org.apache.flink.state.api.Savepoint;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.state.api.functions.KeyedStateReaderFunction;
import org.apache.flink.util.Collector;

public class StateReader {
    public static void main(String s[]) throws Exception {
        ExecutionEnvironment bEnv   = ExecutionEnvironment.getExecutionEnvironment();
        bEnv.setParallelism(1);

        ExistingSavepoint savepoint = Savepoint.load(bEnv,
                "file:///tmp/savepoint/savepoint-5f87d9-a4e9d94b825a",
                new MemoryStateBackend());


        DataSet<KeyedState> keyedState = savepoint.readKeyedState("Word count/ParMultiDo(Stateful)",
                new ReaderFunction());

        System.out.println("######getting count    "+keyedState.count());


    }

    static class ReaderFunction extends KeyedStateReaderFunction<String, KeyedState> {

        ValueState<String> wordCounts;

        @Override
        public void open(Configuration parameters) {
            RuntimeContext context = getRuntimeContext();
            ValueStateDescriptor<String> stateDescriptor =
                    new ValueStateDescriptor<>("word_counts", Types.STRING);

            wordCounts = context.getState(stateDescriptor);
        }

        @Override
        public void readKey(
                String key,
                Context ctx,
                Collector<KeyedState> out) throws Exception {

            KeyedState data = new KeyedState();
            data.key    = key;
            data.value  = wordCounts.value();
            out.collect(data);
        }
    }

    static class KeyedState{
        public String key;
        public String value;

    }


}

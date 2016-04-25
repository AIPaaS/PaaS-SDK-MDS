package test.com.ai.paas.ipaas.mds;

import com.ai.paas.ipaas.mds.IMessageProcessor;
import com.ai.paas.ipaas.mds.IMsgProcessorHandler;

import java.util.ArrayList;
import java.util.List;

public class ProcessorClass implements IMsgProcessorHandler {


    @Override
    public IMessageProcessor[] createInstances(int partitionNum) {
        List<IMessageProcessor> processors = new ArrayList<>();
        IMessageProcessor processor = null;
        for (int i = 0; i < partitionNum; i++) {
            processor = new MessageProcessor();
            processors.add(processor);
        }
        return processors.toArray(new IMessageProcessor[processors.size()]);
    }
}

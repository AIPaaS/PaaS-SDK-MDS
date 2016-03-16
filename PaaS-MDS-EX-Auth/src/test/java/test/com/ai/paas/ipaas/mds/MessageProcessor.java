package test.com.ai.paas.ipaas.mds;

import com.ai.paas.ipaas.mds.IMessageProcessor;
import com.ai.paas.ipaas.mds.vo.MessageAndMetadata;

/**
 * Created by xin on 16-3-16.
 */
public class MessageProcessor implements IMessageProcessor {
    @Override
    public void process(MessageAndMetadata message) throws Exception {
        System.out.println(String.valueOf(message.getMessage()));
    }
}

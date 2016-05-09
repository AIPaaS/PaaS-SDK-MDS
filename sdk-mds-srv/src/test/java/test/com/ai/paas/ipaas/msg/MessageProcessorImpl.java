package test.com.ai.paas.ipaas.msg;

import com.ai.paas.ipaas.mds.IMessageProcessor;
import com.ai.paas.ipaas.mds.vo.MessageAndMetadata;

public class MessageProcessorImpl implements IMessageProcessor {

	@Override
	public void process(MessageAndMetadata message) throws Exception {
		if (null != message) {
			String key = new String(message.getKey(), "UTF-8");
			// if (null!=key && (Integer.parseInt(key)) % 7 == 0) {
			// throw new Exception("error!................" + key);
			// }
			System.out.println("------Topic:" + message.getTopic() + ",key:"
					+ key + ",content:"
					+ new String(message.getMessage(), "UTF-8"));
		}
	}

}

package test.com.ai.paas.ipaas.msg;

import org.junit.Before;
import org.junit.Test;

import com.ai.paas.ipaas.mds.IMessageConsumer;
import com.ai.paas.ipaas.mds.IMsgProcessorHandler;
import com.ai.paas.ipaas.mds.MsgConsumerFactory;
import com.ai.paas.ipaas.uac.vo.AuthDescriptor;

public class MsgConsumerTest {
	private IMessageConsumer msgConsumer = null;

	@Before
	public void setUp() throws Exception {
		String srvId = "MDS001";
		String authAddr = "http://10.1.228.198:14821/iPaas-Auth/service/auth";
		AuthDescriptor ad = new AuthDescriptor(authAddr, "94CB9953B0AB4EFF8377355042F07805", "123456",srvId);
		String topic = "EACAE677521D46D5A0A5E60A3522AB9B_MDS001_324128206";
		IMsgProcessorHandler msgProcessorHandler = new MsgProcessorHandlerImpl();
		msgConsumer = MsgConsumerFactory.getClient(ad, topic,
				msgProcessorHandler);
	}

	@Test
	public void consumeMsg() {
		msgConsumer.start();
		while (true) {
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}

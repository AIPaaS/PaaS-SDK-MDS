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
		String authAddr = "http://10.1.228.200:14105/service-portal-uac-web/service/auth";
		AuthDescriptor ad = new AuthDescriptor(authAddr, "C82D5E2C2F23414896616F3F4840EB48", "123456",srvId);
		String topic = "6C4F4DBA96294DDCBC5DBBF2CAD442B5_MDS001_1910465998";
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

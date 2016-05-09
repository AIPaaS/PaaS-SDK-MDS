package test.com.ai.paas.ipaas.msg;

import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

import com.ai.paas.ipaas.mds.MsgSenderFactory;
import com.ai.paas.ipaas.uac.vo.AuthDescriptor;

public class MsgSenderFactoryTest {
	@Before
	public void setUp() throws Exception {

	}

	@Test(expected = IllegalArgumentException.class)
	public void getSecNullParamterClient() throws Exception {
		AuthDescriptor ad = new AuthDescriptor("aaa", "lll", "kkk", "sss");
		ad = new AuthDescriptor("", "", "", "");
		MsgSenderFactory.getClient(ad, "test");
		ad = new AuthDescriptor(null, null, null, null);
		MsgSenderFactory.getClient(ad, "test");
	}

	@Test(expected = IllegalArgumentException.class)
	public void getThdNullParamterClient() throws Exception {
		AuthDescriptor ad = new AuthDescriptor("aaa", "lll", "kkk", "sss");
		MsgSenderFactory.getClient(ad, null);
	}

	// 这里应该补充完整
	@Test(expected = IllegalArgumentException.class)
	public void getNullParamterClient() throws Exception {
		Properties kafaProps = null;
		String userId = null;
		String topic = null;

		MsgSenderFactory.getClient(kafaProps, userId, topic);
	}

}

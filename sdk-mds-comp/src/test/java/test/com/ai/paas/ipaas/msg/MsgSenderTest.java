package test.com.ai.paas.ipaas.msg;

import org.apache.jmeter.protocol.java.sampler.JUnitSampler;
import org.apache.jmeter.threads.JMeterVariables;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.ai.paas.ipaas.PaasException;
import com.ai.paas.ipaas.mds.IMessageSender;
import com.ai.paas.ipaas.mds.MsgSenderFactory;
import com.ai.paas.ipaas.uac.vo.AuthDescriptor;

public class MsgSenderTest {
	private static transient final Logger log = LogManager
			.getLogger(MsgSenderTest.class);
	private static IMessageSender msgSender = null;

	public MsgSenderTest() {

	}

	static {
		try {
			setUp();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@BeforeClass
	public static void setUp() throws Exception {
		try {
			String srvId, authAddr, authPid, authPasswd, topic;
			JUnitSampler sampler = new JUnitSampler();
			JMeterVariables vars = sampler.getThreadContext().getVariables();
			if (null != vars) {
				srvId = sampler.getThreadContext().getVariables()
						.get("serviceId");
				authAddr = sampler.getThreadContext().getVariables()
						.get("authAddr");
				authPid = sampler.getThreadContext().getVariables()
						.get("authPid");
				authPasswd = sampler.getThreadContext().getVariables()
						.get("authPasswd");
				topic = sampler.getThreadContext().getVariables().get("topic");
			} else {
				// 取一下系统环境变量
				srvId = System.getProperty("msg.serviceId");
				authAddr = System.getProperty("msg.authAddr");
				authPid = System.getProperty("msg.authPid");
				authPasswd = System.getProperty("msg.authPasswd");
				topic = System.getProperty("msg.topic");
			}
			// 这里需要判断一下
			srvId = "MDS001";
			authAddr = "http://10.1.228.200:14105/service-portal-uac-web/service/auth";
			authPid = "C82D5E2C2F23414896616F3F4840EB48";
			authPasswd = "123456";
			topic = "6C4F4DBA96294DDCBC5DBBF2CAD442B5_MDS001_1910465998";
			AuthDescriptor ad = new AuthDescriptor(authAddr, authPid,
					authPasswd, srvId);
			msgSender = MsgSenderFactory.getClient(ad, topic);
		} catch (Exception e) {
			log.error(e);
			throw e;
		}
	}

	@AfterClass
	public static void tearDown() throws Exception {
		msgSender = null;
	}

	@Test(expected = IllegalArgumentException.class)
	public void sendBlankStrMsg() throws Exception {
		if (null == msgSender) {
			log.error("Message sender is null!");
			throw new PaasException("90000", "Message sender is null!");
		}
		msgSender.send("", 0);
	}

	@Test(expected = IllegalArgumentException.class)
	public void sendBlankByteMsg() throws Exception {
		if (null == msgSender) {
			log.error("Message sender is null!");
			throw new PaasException("90000", "Message sender is null!");
		}
		byte[] bytes = null;
		msgSender.send(bytes, 0);
	}

	@Test
	public void sendLongStrMsg() throws Exception {
		if (null == msgSender) {
			log.error("Message sender is null!");
			throw new PaasException("90000", "Message sender is null!");
		}
		msgSender
				.send("adsajddddddddddsadsadsa"
						+ "dddddddddddddddddddasdsadsadsadd"
						+ "ddddddddddddddddddddddddasdsadsadsadsdasdsadddddddddddddddddddddasdsadsadsadsd"
						+ "dfgfdgggggdgfdgdfgfdgfdgfdgfdgfdgfdgdddddddddddddddddddddasdsadsadsadsd"
						+ "dfsdfdsferytertertretrretretedddddddddddddddddddddasdsadsadsadsd"
						+ "fsdfdsfdsfdsfsdfsdfsfsdfsdfsddddddddddddddddddddddasdsadsadsadsd"
						+ "fdsfsdfsfdsfdsfsdfdsfsdfsdfsddddddddddddddddddddddasdsadsadsadsd"
						+ "fdsfsfsdfdsfsdtryrtyryryrtyrytrdddddddddddddddddddddasdsadsadsadsd"
						+ "retetertretretretetretretertertedddddddddddddddddddddasdsadsadsadsd"
						+ "retertertetretertrefdgdgdfgdddddddddddddddddddddasdsadsadsadsd"
						+ "dgdfgggggggggggggggggggggfdgdfgdfdddddddddddddddddddddasdsadsadsadsd"
						+ "gdfggggggggggggggggggggggggggggfhdddddddddddddddddddddasdsadsadsadsd"
						+ "hgwllllllllllllllllllllllllldddddddddddddddddddddasdsadsadsadsd"
						+ "asdddddddddddddddddddddddddddddddddddddddddasdsadsadsadsd"
						+ "kkkkkkkkkkkkkkkkkkkkkdsa", 0);
	}

	@Test
	public void sendByteMsg() throws Exception {
		if (null == msgSender) {
			log.error("Message sender is null!");
			throw new PaasException("90000", "Message sender is null!");
		}
		msgSender.send("Byte message00".getBytes(), 0, "0");
		msgSender.send("Byte message01".getBytes(), 1, "1");
		msgSender.send("Byte message02".getBytes(), 2, "2");
		msgSender.send("Byte message03".getBytes(), 3, "3");
		msgSender.send("Byte message04".getBytes(), 4, "4");
		msgSender.send("Byte message05".getBytes(), 5, "5");
		msgSender.send("Byte message06".getBytes(), 6, "6");
		msgSender.send("Byte message07".getBytes(), 7, "7");
		msgSender.send("Byte message08".getBytes(), 8, "8");
		msgSender.send("Byte message09".getBytes(), 9, "9");
	}

	@Test
	public void sendStringMsg() throws Exception {
		if (null == msgSender) {
			log.error("Message sender is null!");
			throw new PaasException("90000", "Message sender is null!");
		}
		try {
			long t1 = System.nanoTime();
			for (int i = 0; i < 1; i++) {
				msgSender.send("This is a test message00" + i, 0);
				// t1=System.nanoTime();
				msgSender.send("This is a test message10" + i, 1);
				// System.out.println((System.nanoTime()-t1)/1000000);
//				msgSender.send("This is a test message20" + i, 2);
//				msgSender.send("This is a test message30" + i, 3);
//				msgSender.send("This is a test message40" + i, 4);
//				msgSender.send("This is a test message50" + i, 5);
//				msgSender.send("This is a test message60" + i, 6);
//				msgSender.send("This is a test message70" + i, 7);
//				msgSender.send("This is a test message80" + i, 8);
//				msgSender.send("This is a test message90" + i, 9);
			}
			System.out.println((System.nanoTime() - t1) / 1000000);
		} catch (Exception e) {
			log.error(e);
			throw e;
		}
	}

	@Test
	public void sendKeyByteMsg() throws Exception {
		if (null == msgSender) {
			log.error("Message sender is null!");
			throw new PaasException("90000", "Message sender is null!");
		}
		msgSender.send("Byte message00".getBytes(), 0, "0");
		msgSender.send("Byte message01".getBytes(), 1, "1");
		msgSender.send("Byte message02".getBytes(), 2, "2");
		msgSender.send("Byte message03".getBytes(), 3, "3");
		msgSender.send("Byte message04".getBytes(), 4, "4");
		msgSender.send("Byte message05".getBytes(), 5, "5");
		msgSender.send("Byte message06".getBytes(), 6, "6");
		msgSender.send("Byte message07".getBytes(), 7, "7");
		msgSender.send("Byte message08".getBytes(), 8, "8");
		msgSender.send("Byte message09".getBytes(), 9, "9");
	}

	@Test
	public void sendStringKeyMsg() throws Exception {
		if (null == msgSender) {
			log.error("Message sender is null!");
			throw new PaasException("90000", "Message sender is null!");
		}
		try {
			long t1 = System.nanoTime();
			for (int i = 0; i < 1; i++) {
				msgSender.send("This is a test message00" + i, 0, "0");
				msgSender.send("This is a test message10" + i, 1, "1");
				msgSender.send("This is a test message20" + i, 2, "2");
				msgSender.send("This is a test message30" + i, 3, "3");
				msgSender.send("This is a test message40" + i, 4, "4");
				msgSender.send("This is a test message50" + i, 5, "5");
				msgSender.send("This is a test message60" + i, 6, "6");
				msgSender.send("This is a test message70" + i, 7, "7");
				msgSender.send("This is a test message80" + i, 8, "8");
				msgSender.send("This is a test message90" + i, 9, "9");
			}
			System.out.println((System.nanoTime() - t1) / 1000000);
		} catch (Exception e) {
			log.error(e);
			throw e;
		}
	}

}

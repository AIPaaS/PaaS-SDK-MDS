/*
 *   This file is based on the source code of the Kafka spout of the Apache Storm project.
 *   (https://github.com/apache/storm/tree/master/external/storm-kafka)
 */

package com.ai.paas.ipaas.mds.impl.consumer.client;

import java.nio.ByteBuffer;

public class Utils {

	public static Integer getInt(Object o) {
		if (o instanceof Long) {
			return ((Long) o).intValue();
		} else if (o instanceof Integer) {
			return (Integer) o;
		} else if (o instanceof Short) {
			return ((Short) o).intValue();
		} else {
			throw new IllegalArgumentException("Don't know how to convert " + o
					+ " + to int");
		}
	}

	public static byte[] toByteArray(ByteBuffer buffer) {
		byte[] ret = new byte[buffer.remaining()];
		buffer.get(ret, 0, ret.length);
		return ret;
	}

}

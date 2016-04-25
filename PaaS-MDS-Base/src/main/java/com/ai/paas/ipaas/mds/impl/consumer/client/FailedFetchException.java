
package com.ai.paas.ipaas.mds.impl.consumer.client;

public class FailedFetchException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6119901545390768997L;

	public FailedFetchException(String message) {
		super(message);
	}

	public FailedFetchException(Exception e) {
		super(e);
	}
}

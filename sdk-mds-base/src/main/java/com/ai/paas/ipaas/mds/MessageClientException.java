package com.ai.paas.ipaas.mds;

import com.ai.paas.ipaas.PaasRuntimeException;

public class MessageClientException extends PaasRuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6091662080764173798L;
	private String errCode;
	private String errDetail;

	public MessageClientException(String errDetail) {
		super(errDetail);
		this.errDetail = errDetail;
	}

	public MessageClientException(String errCode, String errDetail) {
		super(errCode + ":" + errDetail);
		this.errCode = errCode;
		this.errDetail = errDetail;
	}

	public MessageClientException(String errCode, Exception ex) {
		super(errCode, ex);
		this.errCode = errCode;
	}

	public MessageClientException(String errCode, String errDetail, Exception ex) {
		super(errCode+":"+errDetail, ex);
		this.errCode = errCode;
		this.errDetail = errDetail;
	}

	public String getErrCode() {
		return errCode;
	}

	public void setErrCode(String errCode) {
		this.errCode = errCode;
	}

	public String getErrDetail() {
		return errDetail;
	}

	public void setErrDetail(String errDetail) {
		this.errDetail = errDetail;
	}
}

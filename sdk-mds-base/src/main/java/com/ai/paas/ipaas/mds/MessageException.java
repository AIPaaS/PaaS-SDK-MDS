package com.ai.paas.ipaas.mds;

import com.ai.paas.GeneralRuntimeException;

public class MessageException extends GeneralRuntimeException {

    /**
     * 
     */
    private static final long serialVersionUID = -6091662080764173798L;
    private String errCode;
    private String errDetail;

    public MessageException(String errDetail) {
        super(errDetail);
        this.errDetail = errDetail;
    }

    public MessageException(String errCode, String errDetail) {
        super(errCode + ":" + errDetail);
        this.errCode = errCode;
        this.errDetail = errDetail;
    }

    public MessageException(String errCode, Exception ex) {
        super(errCode, ex);
        this.errCode = errCode;
    }

    public MessageException(String errCode, String errDetail, Exception ex) {
        super(errCode + ":" + errDetail, ex);
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

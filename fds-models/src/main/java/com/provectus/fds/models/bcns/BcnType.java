package com.provectus.fds.models.bcns;

public enum BcnType {
    BID("bid"),
    CLICK("click"),
    IMPRESSION("imp");

    private String code;

    BcnType(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }
}
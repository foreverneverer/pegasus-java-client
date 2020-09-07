package com.xiaomi.infra.pegasus.client.request;

public class Get  {
    public final byte[] hashKey;
    public final byte[] sortKey;

    public Get(byte[] hashKey) {
        this(hashKey,null);
    }

    public Get(byte[] hashKey, byte[] sortKey) {
        this.hashKey = hashKey;
        this.sortKey = sortKey;
    }

}

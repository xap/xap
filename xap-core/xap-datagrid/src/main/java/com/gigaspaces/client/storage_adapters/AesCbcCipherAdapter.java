package com.gigaspaces.client.storage_adapters;

import javax.crypto.SecretKey;
import java.security.GeneralSecurityException;

public class AesCbcCipherAdapter extends AbstractCipherAdapter {
    @Override
    protected String initTransformation() {
        return "AES/CBC/PKCS7Padding"; // todo: configurable padding
    }

    @Override
    protected String initAlgorithm() {
        return "AES";
    }

    @Override
    protected byte[] encrypt(byte[] data, SecretKey key) throws GeneralSecurityException {
        return new byte[0];
    }

    @Override
    protected byte[] decrypt(byte[] data, SecretKey key) throws GeneralSecurityException {
        return new byte[0];
    }
}

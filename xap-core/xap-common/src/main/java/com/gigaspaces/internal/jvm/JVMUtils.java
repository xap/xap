package com.gigaspaces.internal.jvm;


public class JVMUtils {

    public static JVMDiagnosticWrapper getJVMDiagnosticWrapper() {
        try {
            return new HotSpotDiagnosticWrapper();
        } catch (Exception e){
            try {
                return new J9DiagnosticWrapper();
            } catch (Exception ex){
                return new DummyDiagnosticWrapper();
            }
        }
    }
}


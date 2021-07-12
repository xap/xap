package com.gigaspaces.internal.server.space.recovery.direct_persistency;


import com.gigaspaces.start.SystemLocations;
import com.j_spaces.core.sadapter.SAException;

import java.io.*;
import java.util.Scanner;

public class ConsistencyFile implements IStorageConsistency{
    private final File consistFile;

    public ConsistencyFile(String spaceName, String fullMemberName) throws SAException {
        File folder = SystemLocations.singleton().work("tiered-storage/" + spaceName).toFile();
        if (!folder.exists()) {
            if (!folder.mkdirs()) {
                throw new SAException("Failed to mkdir " + folder.getAbsolutePath());
            }
        }
        consistFile = new File(folder.getPath() + "/consist_" + fullMemberName + ".txt");
        if(!consistFile.exists()) {
            try {
                if (!consistFile.createNewFile())
                    throw new IllegalStateException("Failed to create consistency file " + consistFile.getAbsolutePath());
            } catch (IOException e) {
                throw new IllegalStateException("Failed to create consistency file " + consistFile.getAbsolutePath(), e);
            }
        }
    }

    public ConsistencyFile(String path){
        consistFile = new File(path);
        if(!consistFile.exists()) {
            try {
                if (!consistFile.createNewFile())
                    throw new IllegalStateException("Failed to create consistency file " + consistFile.getAbsolutePath());
            } catch (IOException e) {
                throw new IllegalStateException("Failed to create consistency file " + consistFile.getAbsolutePath(), e);
            }
        }
    }

    @Override
    public StorageConsistencyModes getStorageState() {
        if(consistFile !=null){
            try {
                Scanner reader = new Scanner(consistFile);
                if(reader.hasNextLine()) {
                    String data = reader.nextLine();
                    if(data.equals(StorageConsistencyModes.Consistent.toString())){
                        return StorageConsistencyModes.Consistent;
                    }
                    else if(data.equals(StorageConsistencyModes.Inconsistent.toString())){
                        return StorageConsistencyModes.Inconsistent;
                    }
                    else{
                        return StorageConsistencyModes.Unknown;
                    }
                }
                reader.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
        return StorageConsistencyModes.Unknown;
    }

    @Override
    public void setStorageState(StorageConsistencyModes s) {
        if(consistFile != null){
            try{
                FileWriter fileWriter = new FileWriter(consistFile);
                fileWriter.append(s.toString());
                fileWriter.flush();
                fileWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public boolean isPerInstancePersistency() {
        return true;
    }

}

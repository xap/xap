package com.gigaspaces.lrmi.classloading;

import java.io.Serializable;
/**
 * Request for a class definition, includes class name, classloader id and type of resource to retrieve (CLASS or RESOURCE file)
 *
 * @author alon shoham
 * @since 15.0
 */
public class ClassDefinitionRequest implements Serializable {
    private static final long serialVersionUID = 1L;
    private long classLoaderId;
    private String className;
    private FileType fileType;

    public ClassDefinitionRequest() {
    }

    public ClassDefinitionRequest(long classLoaderId, String className, FileType fileType) {
        this.classLoaderId = classLoaderId;
        this.className = className;
        this.fileType = fileType;
    }

    public String getClassName() {
        return className;
    }

    public long getClassLoaderId() {
        return classLoaderId;
    }

    public FileType getFileType() {
        return fileType;
    }

    public String getResourceName(){
        return className;
    }
}

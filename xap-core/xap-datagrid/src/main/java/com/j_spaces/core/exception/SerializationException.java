package com.j_spaces.core.exception;


 /**
 * Such exception thrown when the serialization fails
 *
 * @since 15.8.0
 */
public class SerializationException extends RuntimeException{
     private static final long serialVersionUID = 1986718821543448112L;
    public SerializationException( Throwable cause ){
        super( cause );
    }
}

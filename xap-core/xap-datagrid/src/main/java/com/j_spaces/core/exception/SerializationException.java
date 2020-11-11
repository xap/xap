package com.j_spaces.core.exception;


 /**
 * Such exception thrown when the serialization fails
 *
 * @since 15.8.0
 */
public class SerializationException extends RuntimeException{

    public SerializationException( Throwable cause ){
        super( cause );
    }
}

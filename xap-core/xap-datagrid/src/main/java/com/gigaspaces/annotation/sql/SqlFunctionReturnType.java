package com.gigaspaces.annotation.sql;

import com.gigaspaces.query.sql.functions.SqlFunction;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/***
 * @since 15.0
 * @author yael nahon
 *
 *
 * Use this annotation to indicate the return type of a {@link com.gigaspaces.query.sql.functions.SqlFunction}
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface SqlFunctionReturnType {
    Class<?> type() default Object.class ;
}

package org.openspaces.core.space.suspend.anntations;

import org.openspaces.core.space.suspend.SpaceChangeEvent;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/**
 * Allows a bean's method to be invoked when space suspend type changes
 * The target invocation method may have a single parameter of type {@link SpaceChangeEvent}.
 *
 * @author Elad Gur
 * @since 14.0.1
 */

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface SuspendTypeChanged {
}
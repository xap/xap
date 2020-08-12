package org.openspaces.core.space.status;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/**
 * Allows a bean's method to be invoked when space suspend type or space mode changes
 * The target invocation method may have a single parameter of type {@link SpaceStatusChangedEvent}.
 *
 * @author Elad Gur
 * @since 14.0.1
 */

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface SpaceStatusChanged {}
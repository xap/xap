/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.openspaces.core.space.suspend.anntations.registery;

import org.openspaces.core.space.AbstractAnnotationRegistry;
import org.openspaces.core.space.suspend.SuspendTypeChangedEvent;
import org.openspaces.core.space.suspend.SuspendTypeChangedListener;
import org.openspaces.core.space.suspend.anntations.SuspendTypeChanged;

import java.lang.reflect.Method;
import java.text.MessageFormat;

/**
 * Receives space mode change events and routs them to beans that use annotations to register as
 * listeners on those events.
 * <p>
 * When the application starts beans that has one or more of the annotation {@link SuspendTypeChanged}
 * are registered in this bean, and when
 * events arrive they are routed to the registered beans' methods.
 *
 * @author Elad Gur
 */
public class SuspendTypeAnnotationRegistry extends AbstractAnnotationRegistry
        implements SuspendTypeChangedListener {

    @Override
    protected void validateMethod(Class<?> annotation, Method method) {
        Class<?>[] methodParametersTypes = method.getParameterTypes();
        validateAnnotationsType(annotation);
        validateMethodParameters(annotation, methodParametersTypes);
    }

    private void validateAnnotationsType(Class<?> annotationClass) {
        if (!annotationClass.equals(SuspendTypeChanged.class)) {
            throw new IllegalArgumentException("The specified annotation is not a space suspend type annotation: " + annotationClass);
        }
    }

    private void validateMethodParameters(Class<?> annotation, Class<?>[] methodParametersTypes) {
        final int expectedNumOfParameters = 1;

        if (methodParametersTypes.length != expectedNumOfParameters) {
            throw new IllegalArgumentException("The specified method has invalid number of parameters, A valid method may have a single parameter of type " +
                    SuspendTypeChangedEvent.class.getName());
        } else if (!methodParametersTypes[0].equals(SuspendTypeChangedEvent.class)) {
            String errorMsg = MessageFormat.format("Illegal target invocation method parameter type: {0}. A valid target invocation method for annotation {1} may have a single parameter of type {2}",
                    methodParametersTypes[0].getName(), annotation.getSimpleName(), SuspendTypeChangedEvent.class.getName());
            throw new IllegalArgumentException(errorMsg);
        }
    }

    @Override
    public void onSuspendTypeChanged(SuspendTypeChangedEvent event) {
        fireEvent(SuspendTypeChanged.class, event);
    }

}

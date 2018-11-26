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

package org.openspaces.core.space.mode.registry;

import com.gigaspaces.cluster.activeelection.SpaceMode;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openspaces.core.space.AbstractAnnotationRegistry;
import org.openspaces.core.space.mode.AfterSpaceModeChangeEvent;
import org.openspaces.core.space.mode.BeforeSpaceModeChangeEvent;
import org.openspaces.core.space.mode.PostBackup;
import org.openspaces.core.space.mode.PostPrimary;
import org.openspaces.core.space.mode.PreBackup;
import org.openspaces.core.space.mode.PrePrimary;
import org.openspaces.core.space.mode.SpaceAfterBackupListener;
import org.openspaces.core.space.mode.SpaceAfterPrimaryListener;
import org.openspaces.core.space.mode.SpaceBeforeBackupListener;
import org.openspaces.core.space.mode.SpaceBeforePrimaryListener;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Hashtable;

/**
 * Receives space mode change events and routs them to beans that use annotations to register as
 * listeners on those events.
 * <p>
 * When the application starts beans that has one or more of the anotnation {@link PreBackup},
 * {@link PostBackup}, {@link PrePrimary}, {@link PostPrimary} are registered in this bean, and when
 * events arrive they are routed to the registered beans' methods.
 *
 * @author shaiw
 */
public class ModeAnnotationRegistry extends AbstractAnnotationRegistry implements SpaceBeforePrimaryListener,
        SpaceAfterPrimaryListener,
        SpaceBeforeBackupListener,
        SpaceAfterBackupListener {

    @Override
    protected void validateMethod(Class<?> annotation, Method method) {
        // check that the specified method has no more than one parameter
        Class<?>[] types = method.getParameterTypes();
        if (types.length > 1) {
            throw new IllegalArgumentException("The specified method has more than one parameter. A valid" +
                    " method may have no parameters or a single parameter of type " + BeforeSpaceModeChangeEvent.class.getName() +
                    " or " + AfterSpaceModeChangeEvent.class.getName());
        }

        // checks that the annotation is legal and that the method parameter is valid for the annotation type.
        if (annotation.equals(PreBackup.class) || annotation.equals(PrePrimary.class)) {
            if (types.length == 1 && !types[0].equals(BeforeSpaceModeChangeEvent.class)) {
                throw new IllegalArgumentException("Illegal target invocation method parameter type: " + types[0].getName() +
                        ". A valid target invocation method for annotation " + annotation.getSimpleName() + " may have no parameters" +
                        " or a single parameter of type " + BeforeSpaceModeChangeEvent.class.getName());
            }
        } else if (annotation.equals(PostBackup.class) || annotation.equals(PostPrimary.class)) {
            if (types.length == 1 && !types[0].equals(AfterSpaceModeChangeEvent.class)) {
                throw new IllegalArgumentException("Illegal target invocation method parameter type: " + types[0].getName() +
                        ". A valid target invocation method for annotation " + annotation.getSimpleName() + " may have no parameters" +
                        " or a single parameter of type " + AfterSpaceModeChangeEvent.class.getName());
            }
        } else {
            throw new IllegalArgumentException("The specified annotation is not a space mode annotation: " + annotation);
        }
    }

    /**
     * Invoked before a space changes its mode to {@link SpaceMode#PRIMARY}.
     */
    public void onBeforePrimary(BeforeSpaceModeChangeEvent event) {
        fireEvent(PrePrimary.class, event);
    }

    /**
     * Invoked after a space changes its mode to {@link SpaceMode#PRIMARY}.
     */
    public void onAfterPrimary(AfterSpaceModeChangeEvent event) {
        fireEvent(PostPrimary.class, event);
    }

    /**
     * Invoked before a space changes its mode to {@link SpaceMode#BACKUP}.
     */
    public void onBeforeBackup(BeforeSpaceModeChangeEvent event) {
        fireEvent(PreBackup.class, event);
    }

    /**
     * Invoked after a space changes its mode to {@link SpaceMode#BACKUP}.
     */
    public void onAfterBackup(AfterSpaceModeChangeEvent event) {
        fireEvent(PostBackup.class, event);
    }

}

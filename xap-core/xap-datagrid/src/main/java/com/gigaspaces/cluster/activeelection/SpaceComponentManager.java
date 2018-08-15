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


package com.gigaspaces.cluster.activeelection;

import com.gigaspaces.internal.server.space.SpaceImpl;
import com.sun.jini.admin.DestroyAdmin;

import java.rmi.RemoteException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * SpaceComponentManager handles SpaceInternal components.<br> The manager handles all space mode
 * transitions and changes the components state according to the space state. <p> Space states:
 * -------------------------------------------------------------<br> <ul> <li> NONE - the initial
 * state of the space before election.<br> <li> PRIMARY - the state of the space after is was
 * selected as primary.<br> <li> BACKUP - the state of the space after it was selected as
 * backup.<br> </ul> <p> Space components types:<br> --------------------------------------------------------------<br>
 * backup components - components that can run on backup spaces.<br> primary only       - components
 * that can run only on primary spaces. <p> The following state transitions are handled:<br>
 * --------------------------------------------------<br> NONE-->PRIMARY - space becomes primary
 * after the first election. All components are started. <p> NONE-->BACKUP  - space becomes backup
 * after the first election. Only the backup components are started.<br> <p> BACKUP-->PRIMARY -
 * space becomes primary after previously being backup. Only the primary-only components are
 * started, since backup components are already up. <p> PRIMARY-->BACKUP - in case of a
 * "split-brain" where two spaces are primary at the same time, one of them returns to backup. In
 * this case all the space components are restarted.
 *
 * @author anna
 * @version 1.0
 * @since 6.0
 */
@com.gigaspaces.api.InternalApi
public class SpaceComponentManager
        implements ISpaceModeListener {
    private SpaceImpl _space;

    // Indicates space availability mode
    private SpaceMode _spaceMode = SpaceMode.NONE;

    // Space components handlers
    private LinkedList<ISpaceComponentsHandler> _componentsHandlers = new LinkedList<ISpaceComponentsHandler>();

    // logger
    private static final Logger _logger = Logger.getLogger(com.gigaspaces.logger.Constants.LOGGER_CLUSTER_ACTIVE_ELECTION);

    /**
     * Constructor.
     */
    public SpaceComponentManager(SpaceImpl space) throws SpaceComponentsInitializeException {
        _space = space;

        init();
    }

    /**
     * Initialize the component manager with space components. Initialize any component that can be
     * run during recovery. Only components that can run on backup are initialized, Primary-only
     * components are initialized later, since they can't run during recovery.
     */
    public void init() throws SpaceComponentsInitializeException {

        _componentsHandlers = _space.getSpaceComponentHandlers();

        try {
            // set space initializer , so the components can access the space when it is still backup
            SpaceInitializationIndicator.setInitializer();

            // Initialize any components that can run during recovery
            for (Iterator<ISpaceComponentsHandler> iterator = _componentsHandlers.iterator(); iterator.hasNext(); ) {
                ISpaceComponentsHandler handler = iterator.next();

                if (handler.isRecoverySupported())
                    handler.initComponents(false);

            }

        } finally {
            SpaceInitializationIndicator.unsetInitializer();

        }

        _space.addInternalSpaceModeListener(this);

    }

    /*
     * @see com.j_spaces.core.ISpaceModeListener#beforeSpaceModeChange(com.gigaspaces.cluster.init.Mode)
     */
    @Override
    public void beforeSpaceModeChange(SpaceMode newMode) {
        try {
            // set space initializer , so the components can access the space when it is still backup
            SpaceInitializationIndicator.setInitializer();

            switch (newMode) {
                case BACKUP:

                    beforeMovingToBackup();
                    break;

                case PRIMARY:

                    beforeMovingToPrimary();
                    break;
            }
        } catch (SpaceComponentsInitializeException e) {
            // If this exception was delegated from
            //  one of the components
            // Shutdown the space
            try {
                if (_logger.isLoggable(Level.SEVERE)) {
                    _logger.log(Level.SEVERE,
                            "Failed to initialize space components, shutting down the space.",
                            e);
                }
                ((DestroyAdmin) _space.getAdmin()).destroy();

            } catch (RemoteException e1) {
                if (_logger.isLoggable(Level.SEVERE)) {
                    _logger.log(Level.SEVERE, "Failed to shutdown space.", e1);
                }

            }
        } finally {
            SpaceInitializationIndicator.unsetInitializer();

        }


    }// beforeSpaceModeChange

    private void closePrimaryOnlyComponents() {
        SpaceInitializationIndicator.setInitializer();

        for (Iterator<ISpaceComponentsHandler> iterator = _componentsHandlers.iterator(); iterator.hasNext(); ) {
            ISpaceComponentsHandler handler = iterator.next();
            handler.close(true);
        }

        SpaceInitializationIndicator.unsetInitializer();

    }
    /*
     * @see com.j_spaces.core.ISpaceModeListener#afterSpaceModeChange(com.gigaspaces.cluster.init.Mode)
     */
    @Override
    public void afterSpaceModeChange(SpaceMode newMode) {
        switch (newMode) {
            case BACKUP:

                afterMovingToBackup();
                break;

            case PRIMARY:

                afterMovingToPrimary();
                break;
        }

        _spaceMode = newMode;
    }

    /**
     * Called after space becomes backup. Any space component that can run on backup is started.
     * (active-when-backup=true)
     */
    public void afterMovingToBackup() {
        switch (_spaceMode) {
            // NONE --> BACKUP
            case NONE:
                // Start backup components
                startComponents(false);
                break;

            // PRIMARY -- > BACKUP
            case PRIMARY:
                closePrimaryOnlyComponents();
//                // Restart space when switching from primary to backup
//                try {
//                    _space.stopInternal();
//                    _space.startInternal();
//                } catch (RemoteException e) {
//                    if (_logger.isLoggable(Level.WARNING)) {
//                        _logger.log(Level.WARNING, "Failed moving to backup", e);
//                    }
//                }

                break;
        }
    }

    /**
     * Called after space becomes primary. All space components tare started.
     */
    private void afterMovingToPrimary() {
        switch (_spaceMode) {
            // NONE -->PRIMARY
            case NONE:

                // Start primary-only components
                startComponents(true);

                // Start backup components
                startComponents(false);
                break;
            // BACKUP -->PRIMARY
            case BACKUP:
                // Start primary-only components
                startComponents(true);
                break;
        }
    }


    /**
     * Called before space becomes backup. Any space component that can run on backup is
     * initialized. (active-when-backup=true)
     */
    public void beforeMovingToBackup()
            throws SpaceComponentsInitializeException {
        switch (_spaceMode) {
            // NONE --> BACKUP
            case NONE:
                // Init backup components
                initBackupComponents();
                break;

            // PRIMARY -- > BACKUP
            case PRIMARY:

                break;
        }
    }

    /**
     * Called before space becomes primary. All space components that are initialized.
     */
    private void beforeMovingToPrimary()
            throws SpaceComponentsInitializeException {
        switch (_spaceMode) {
            // NONE -->PRIMARY
            case NONE:

                // Init primary-only components
                initPrimaryOnlyComponents();

                // Init backup components
                initBackupComponents();
                break;
            // BACKUP -->PRIMARY
            case BACKUP:
                // Init primary-only components
                initPrimaryOnlyComponents();
                break;
        }
    }

    /**
     * Initialize primary-only space components
     */
    private void initPrimaryOnlyComponents()
            throws SpaceComponentsInitializeException {

        for (Iterator<ISpaceComponentsHandler> iterator = _componentsHandlers.iterator(); iterator.hasNext(); ) {
            ISpaceComponentsHandler handler = iterator.next();
            handler.initComponents(true);
        }
    }

    /**
     * Initialize backup component that can't be run during recovery.
     */
    private void initBackupComponents()
            throws SpaceComponentsInitializeException {

        for (Iterator<ISpaceComponentsHandler> iterator = _componentsHandlers.iterator(); iterator.hasNext(); ) {
            ISpaceComponentsHandler handler = iterator.next();

            if (!handler.isRecoverySupported())
                handler.initComponents(false);
        }
    }

    /**
     * Initialize all space components
     */
    public void initComponents() throws SpaceComponentsInitializeException {
        try {
            // set space initializer , so the components can access the space when it is still backup
            SpaceInitializationIndicator.setInitializer();


            // Initialize primary-only components
            initPrimaryOnlyComponents();

            // Initialize backup components
            initBackupComponents();
        } finally {
            SpaceInitializationIndicator.unsetInitializer();

        }

    }

    /**
     * Start space components
     */
    private void startComponents(boolean spaceIsPrimary) {
        for (Iterator<ISpaceComponentsHandler> iterator = _componentsHandlers.iterator(); iterator.hasNext(); ) {
            ISpaceComponentsHandler handler = iterator.next();
            handler.startComponents(spaceIsPrimary);
        }
    }

    /**
     * Start all space components
     */
    public void startComponents() {
        startComponents(true);
        startComponents(false);

        _spaceMode = SpaceMode.PRIMARY;
    }


    /**
     * Restart space components. Space state doesn't change
     */
    public void restartSpaceComponents() throws SpaceComponentsInitializeException {
        init();

        SpaceMode newMode = _spaceMode;
        _spaceMode = SpaceMode.NONE;

        beforeSpaceModeChange(newMode);

        afterSpaceModeChange(newMode);
    }

    /**
     * @return Returns the componentsHandlers.
     */
    public LinkedList<ISpaceComponentsHandler> getComponentsHandlers() {
        return _componentsHandlers;
    }

    /**
     * @param componentsHandlers The componentsHandlers to set.
     */
    public void setComponentsHandlers(
            LinkedList<ISpaceComponentsHandler> componentsHandlers) {
        _componentsHandlers = componentsHandlers;
    }

    /**
     * @return Returns the space.
     */
    public SpaceImpl getSpace() {
        return _space;
    }

    /**
     * @param space The space to set.
     */
    public void setSpace(SpaceImpl space) {
        _space = space;
    }

    /**
     * @return Returns the spaceMode.
     */
    public SpaceMode getSpaceMode() {
        return _spaceMode;
    }

    /**
     * @param spaceMode The spaceMode to set.
     */
    public void setSpaceMode(SpaceMode spaceMode) {
        _spaceMode = spaceMode;
    }

    public void clear() {
        _componentsHandlers.clear();
        _space.removeInternalSpaceModeListener(this);

    }
}

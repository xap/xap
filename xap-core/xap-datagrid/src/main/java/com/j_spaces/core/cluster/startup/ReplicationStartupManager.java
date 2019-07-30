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


package com.j_spaces.core.cluster.startup;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * ReplicationStartupManager handles the space startup state in persistent replicated cluster. Space
 * startup state is needed to keep track of the last space in cluster that was up, since it holds
 * the most updated cluster state. The ReplicationStartupManager monitors all the target spaces in
 * cluster. Initial spaces state is updated at space startup in {@link SpaceImpl}. Connection events
 * are received from the {@link AbstractReplicationChannel} and disconnect events from {@link
 * GigaSpacesFaultDetectionHandler}.
 *
 * Default ReplicationStartupManager implementation stores space state to file. This behavior can be
 * changed by overriding the saveState and loadState methods.
 *
 * @author anna
 * @version 1.0
 * @since 5.1
 */
@com.gigaspaces.api.InternalApi
public class ReplicationStartupManager
        implements SpaceConnectionListener {

    public final static String DIR_NAME = "secure-startup";
    public final static String FILENAME_SUFFIX = "-startup-state.txt";
    // logger
    private static Logger _logger = Logger.getLogger(com.gigaspaces.logger.Constants.LOGGER_REPLICATION);

    public static enum StartupState {

        UNINITIALIZED, WAITING, LAST, NOT_LAST
    }

    ;

    // Managed state
    protected final String _spaceName;

    // Space startup state
    protected StartupState _currentSpaceState = StartupState.UNINITIALIZED;

    // The file that keeps space state
    protected File _stateFile;

    // Contains all the spaces that are currently active
    private final HashSet<Object> _activeSpaces = new HashSet<Object>();

    /**
     * @param spaceName
     */
    public ReplicationStartupManager(String spaceName) {
        // Load space state
        _spaceName = spaceName;

        try {
            _currentSpaceState = loadState();
        } catch (IOException e) {
            if (_logger.isLoggable(Level.SEVERE)) {
                _logger.log(Level.SEVERE, " Failed to load space [" + spaceName
                        + "] state.", e);
            }
        }
    }

    /**
     * @return space startup state
     */
    public StartupState getSpaceState() {
        return _currentSpaceState;
    }

    /**
     * Change space state and store it
     */
    public synchronized void changeState(StartupState newState) {

        if (_currentSpaceState == newState)
            return;


        if (_logger.isLoggable(Level.FINE)) {
            _logger.log(Level.FINE, "Changing space [" + _spaceName + "] state from "
                    + _currentSpaceState + " to " + newState);
        }

        // Save the state to file
        try {
            saveState(newState);
        } catch (IOException e) {
            if (_logger.isLoggable(Level.WARNING)) {
                _logger.log(Level.WARNING, " Error on space connect [" + _spaceName
                        + "] state.", e);
            }
        }
        _currentSpaceState = newState;

    }

    /**
     * @return Loaded space state
     */
    protected StartupState loadState() throws IOException {
        createStateFileIfNotExists();

        StartupState state = null;

        String stateString;
        try (BufferedReader reader = new BufferedReader(new FileReader(_stateFile))) {
            stateString = reader.readLine();
        }

        try {
            // Create an enum from string
            // in case of an empty/null string or illegal - non-enum string
            // state is set to UNINITIALIZED
            if (stateString != null) {
                state = Enum.valueOf(StartupState.class, stateString);

                return state;
            }
        } catch (IllegalArgumentException e) {
        }

        // No valid state was defined - UNINITIALIZED
        state = StartupState.UNINITIALIZED;

        return state;
    }

    /**
     * @throws IOException
     */
    private void createStateFileIfNotExists() throws IOException {
        if (_stateFile == null) {
            _stateFile = new File(DIR_NAME + File.separator + _spaceName.replace(':', '-') + FILENAME_SUFFIX);
        }

        // Check if the file already exists
        if (!_stateFile.exists()) {
            File dir = new File(DIR_NAME);
            dir.mkdir();
            _stateFile.createNewFile();
        }

    }

    /**
     * Save space state
     */
    protected void saveState(StartupState state) throws IOException {
        createStateFileIfNotExists();

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(_stateFile))) {
            writer.write(state.toString());
            writer.flush();
        }
    }

    /**
     * Wait for the last space to start
     */
    public synchronized void waitForLastSpace() throws InterruptedException {
        // If space is not last - don't wait
        if (!_activeSpaces.isEmpty())
            return;

        // Change the state to waiting(no need to save this state to file)
        _currentSpaceState = StartupState.WAITING;

        wait();

        // If got here - it means that the last space is already up
        changeState(StartupState.NOT_LAST);

    }

    /**
     * Check if this space should wait for other space in cluster to start
     */
    public synchronized boolean shouldWait(List<String> targetNames)
            throws InterruptedException {

        // If this space was the last - no need to wait
        if (_currentSpaceState == StartupState.LAST) {
            return false;
        }

        // If last space is already up - nor need to wait
        if (!_activeSpaces.isEmpty())
            return false;

        // If this is the first time this space is starting
        // only the first space in group is allowed to start first
        // to avoid inconsistency.
        if (_currentSpaceState == StartupState.UNINITIALIZED) {
            if (_spaceName.equals(targetNames.get(0)))
                return false;
        }

        return true;

    }

    /**
     * Called when one of the spaces in cluster connects to this space
     *
     * @param event connection event
     */
    public synchronized void onSpaceConnect(SpaceConnectionEvent event) {
        try {
            // Add this space to the list of active spaces
            _activeSpaces.add(event.getId());

            // If the space is waiting - notify it so it can join the cluster
            if (_currentSpaceState == StartupState.WAITING) {
                notify();
            } else {
                changeState(StartupState.NOT_LAST);
            }

        } catch (Exception e) {
            if (_logger.isLoggable(Level.WARNING)) {
                _logger.log(Level.WARNING, " Error on space connect [" + _spaceName
                        + "] state.", e);
            }
        }
    }


    /*
     * @see com.j_spaces.core.cluster.startup.SpaceConnectionListener#onSpaceDisconnect(com.j_spaces.core.cluster.startup.SpaceConnectionEvent)
     */
    public synchronized void onSpaceDisconnect(SpaceConnectionEvent event) {
        _activeSpaces.remove(event.getId());

        if (_activeSpaces.isEmpty()) {
            // No active spaces in cluster - mark this space as the last
            changeState(StartupState.LAST);
        }
    }

}

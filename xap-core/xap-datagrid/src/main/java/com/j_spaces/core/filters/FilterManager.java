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

package com.j_spaces.core.filters;

import com.gigaspaces.client.mutators.SpaceEntryMutator;
import com.gigaspaces.cluster.activeelection.ISpaceComponentsHandler;
import com.gigaspaces.cluster.activeelection.SpaceComponentsInitializeException;
import com.gigaspaces.executor.SpaceTaskWrapper;
import com.gigaspaces.internal.collections.CollectionsFactory;
import com.gigaspaces.internal.collections.IntegerList;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.space.SpaceConfigReader;
import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.server.space.events.NotifyContext;
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.server.filter.NotifyEvent;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.SpaceContext;
import com.j_spaces.core.filters.entry.ExecutionFilterEntry;
import com.j_spaces.core.filters.entry.ISpaceFilterEntry;
import com.j_spaces.core.filters.entry.SpaceAfterChangeFilterEntryImpl;
import com.j_spaces.core.filters.entry.SpaceBeforeChangeFilterEntryImpl;
import com.j_spaces.core.filters.entry.SpaceFilterEntryImpl;
import com.j_spaces.core.filters.entry.SpaceUpdateFilterEntryImpl;
import com.j_spaces.kernel.ClassLoaderHelper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.j_spaces.core.Constants.Filter.DEFAULT_ACTIVE_WHEN_BACKUP;
import static com.j_spaces.core.Constants.Filter.DEFAULT_FILTER_ENABLE_VALUE;
import static com.j_spaces.core.Constants.Filter.DEFAULT_OPERATION_CODE_VALUE;
import static com.j_spaces.core.Constants.Filter.DEFAULT_PASS_ENTRY;
import static com.j_spaces.core.Constants.Filter.DEFAULT_SHUTDOWN_ON_INIT_FAILURE;
import static com.j_spaces.core.Constants.Filter.FILTERS_TAG_NAME;
import static com.j_spaces.core.Constants.Filter.FILTER_ACTIVE_WHEN_BACKUP;
import static com.j_spaces.core.Constants.Filter.FILTER_CLASS_TAG_NAME;
import static com.j_spaces.core.Constants.Filter.FILTER_ENABLED_TAG_NAME;
import static com.j_spaces.core.Constants.Filter.FILTER_NAMES_PROP;
import static com.j_spaces.core.Constants.Filter.FILTER_OPERATION_CODE_TAG_NAME;
import static com.j_spaces.core.Constants.Filter.FILTER_PASS_ENTRY;
import static com.j_spaces.core.Constants.Filter.FILTER_PRIORITY_TAG_NAME;
import static com.j_spaces.core.Constants.Filter.FILTER_PROVIDERS;
import static com.j_spaces.core.Constants.Filter.FILTER_SHUTDOWN_ON_INIT_FAILURE;
import static com.j_spaces.core.Constants.Filter.FILTER_URL_TAG_NAME;

@com.gigaspaces.api.InternalApi
public class FilterManager implements ISpaceComponentsHandler {
    private static final Logger _logger = Logger.getLogger(com.gigaspaces.logger.Constants.LOGGER_FILTERS);

    private static final int MAX_FILTER_PRIORITIES = 5;
    private static final String DEFAULT_NAME = "default";

    public final boolean[] _isFilter;

    private final IJSpace _space;
    private final PrioritySpaceFiltersHolder[] _filters;
    private final Map<String, FilterHolder> _filtersRepository;
    private final SpaceTypeManager _typeManager;

    public FilterManager(SpaceTypeManager typeManager, IJSpace space, SpaceEngine engine) {
        _filters = new PrioritySpaceFiltersHolder[FilterOperationCodes.MAX_FILTER_OPERATION_CODES];
        _isFilter = new boolean[FilterOperationCodes.MAX_FILTER_OPERATION_CODES];
        _filtersRepository = new Hashtable<String, FilterHolder>();
        _typeManager = typeManager;
        _space = space;
        init(engine);
    }

    /**
     * init the filter manager.
     */
    public void init(SpaceEngine engine) {
        ArrayList<FilterHolder>[][] tempFilterConstruction = new ArrayList[FilterOperationCodes.MAX_FILTER_OPERATION_CODES][];
        // Extract Internal Filters:
        List<FilterProvider> internalFilterProviders = engine.getInternalFilters();
        if (internalFilterProviders != null)
            for (FilterProvider filterProvider : internalFilterProviders)
                add(filterProvider, tempFilterConstruction);

        extract_filters(engine.getConfigReader(), tempFilterConstruction);

        // Extract URL Filter Providers:
        if (_space.getURL() != null) {
            FilterProvider[] urlFilterProviders = (FilterProvider[]) _space.getURL().getCustomProperties().get(FILTER_PROVIDERS);
            if (urlFilterProviders != null)
                for (FilterProvider filterProvider : urlFilterProviders)
                    add(filterProvider, tempFilterConstruction);
        }

        for (int i = 0; i < tempFilterConstruction.length; i++) {
            final ArrayList<FilterHolder>[] allPrioritiesFilters = tempFilterConstruction[i];
            if (allPrioritiesFilters != null) {
                final FilterHolder[][] filterHolders = new FilterHolder[MAX_FILTER_PRIORITIES][];
                for (int j = 0; j < allPrioritiesFilters.length; j++) {
                    final ArrayList<FilterHolder> specificPriorityFilter = allPrioritiesFilters[j];
                    if (specificPriorityFilter != null)
                        filterHolders[j] = specificPriorityFilter.toArray(new FilterHolder[0]);
                }
                _filters[i] = new PrioritySpaceFiltersHolder(filterHolders);
            }
        }

    }


    //has AfterTakeMultiple filter other then jSpaceStatistics
    private boolean initNonStatisticsAfterTakeMultipleOpFilter() {
        if(!_isFilter[FilterOperationCodes.AFTER_TAKE_MULTIPLE]){
            return false;
        }

        PrioritySpaceFiltersHolder prioritySpaceFiltersHolder = _filters[FilterOperationCodes.AFTER_TAKE_MULTIPLE];

        if(prioritySpaceFiltersHolder.isSingleFilterHolder){
            return !(prioritySpaceFiltersHolder.singleFilterHolder.getFilter() instanceof JSpaceStatistics);
        } else {
            for (FilterHolder[] prioritizedFilterHolder : prioritySpaceFiltersHolder.prioritizedFilterHolders) {
                for (FilterHolder filterHolder : prioritizedFilterHolder) {
                    if(!(filterHolder.getFilter() instanceof JSpaceStatistics)){
                        return true;
                    }
                }
            }
        }

        return false;
    }

    public ISpaceFilter getFilterObject(String filterId) {
        FilterHolder holder = _filtersRepository.get(filterId);
        if (holder == null)
            return null;

        return holder.getFilter();
    }

    private void extract_filters(SpaceConfigReader configReader, ArrayList<FilterHolder>[][] tempFilterConstruction) {
        // using space properties- get filters from xml file
        List<String> filtersNames = configReader.getListSpaceProperty(FILTER_NAMES_PROP, DEFAULT_NAME, ",");
        if (filtersNames == null)
            return; // no filter

        for (String filterName : filtersNames) {
            final String filterNamePrefix = FILTERS_TAG_NAME + "." + filterName + ".";

            // is this filter enabled?? (default = true)
            boolean isEnabled = configReader.getBooleanSpaceProperty(
                    filterNamePrefix + FILTER_ENABLED_TAG_NAME, DEFAULT_FILTER_ENABLE_VALUE);
            if (!isEnabled)
                continue;

            // is this filter active when space is backup
            final boolean isActiveWhenBackup = configReader.getBooleanSpaceProperty(
                    FILTERS_TAG_NAME + "." + filterName + '.' + FILTER_ACTIVE_WHEN_BACKUP, DEFAULT_ACTIVE_WHEN_BACKUP);

            final boolean shutdownSpaceOnInitFailure = configReader.getBooleanSpaceProperty(
                    filterNamePrefix + FILTER_SHUTDOWN_ON_INIT_FAILURE, DEFAULT_SHUTDOWN_ON_INIT_FAILURE);

            final boolean passFilterEntry = configReader.getBooleanSpaceProperty(
                    filterNamePrefix + FILTER_PASS_ENTRY, DEFAULT_PASS_ENTRY);

            List<String> op_codes = configReader.getListSpaceProperty(
                    filterNamePrefix + FILTER_OPERATION_CODE_TAG_NAME, DEFAULT_OPERATION_CODE_VALUE, ",");

            /* init a specific filter */
            String classname = configReader.getSpaceProperty(
                    filterNamePrefix + FILTER_CLASS_TAG_NAME, DEFAULT_NAME);
            if (classname.equals(DEFAULT_NAME)) {
                // invalid class name
                if (_logger.isLoggable(Level.SEVERE))
                    _logger.severe("Failed to add filter " + filterName + ": " + "no class name specified");
                continue;
            }
            int priority;
            try {
                priority = configReader.getIntSpaceProperty(filterNamePrefix + FILTER_PRIORITY_TAG_NAME, "0");
            } catch (Exception e) {
                if (_logger.isLoggable(Level.SEVERE))
                    _logger.log(Level.SEVERE, "Failed to add filter " + filterName + ": " + "invalid priority specified", e);
                continue;
            }
            if (priority < 0 || priority >= MAX_FILTER_PRIORITIES) {
                if (_logger.isLoggable(Level.SEVERE))
                    _logger.severe("Failed to add filter " + filterName + ": " + "invalid priority specified: ");
                continue;
            }
            String url = configReader.getSpaceProperty(filterNamePrefix + FILTER_URL_TAG_NAME, null);

            IntegerList op_arr = CollectionsFactory.getInstance().createIntegerList();
            for (String op : op_codes) {
                int operation_code;
                try {
                    operation_code = Integer.parseInt(op);
                } catch (Exception e) {
                    if (_logger.isLoggable(Level.SEVERE))
                        _logger.log(Level.SEVERE, "Failed to add filter " + filterName + ": " + "invalid operation-code specified: " + op, e);
                    continue;
                }
                if (operation_code < 0 || operation_code >= FilterOperationCodes.MAX_FILTER_OPERATION_CODES) {
                    if (_logger.isLoggable(Level.SEVERE))
                        _logger.severe("Failed to add filter " + filterName + ": " + "invalid operation-code specified: " + operation_code);
                    continue;
                }

                op_arr.add(operation_code);
            }
            if (op_arr.isEmpty()) {
                if (_logger.isLoggable(Level.SEVERE))
                    _logger.severe("Failed to add filter " + filterName + ": " + "no operation-code specified.");
                continue;
            }

            try {
                add(filterName, op_arr.toNativeArray(), priority, url, classname, isActiveWhenBackup, shutdownSpaceOnInitFailure, passFilterEntry, tempFilterConstruction);
            } catch (Exception e) {
                // continue to next filter
                if (_logger.isLoggable(Level.SEVERE))
                    _logger.log(Level.SEVERE, "Failed to add filter " + filterName, e);
                continue;
            }
        }
    }

    private void add(String filterId, int[] opCodes, int priority, String url, String classname,
                     boolean isActiveWhenBackup, boolean shutdownSpaceOnInitFailure, boolean passFilterEntry, ArrayList<FilterHolder>[][] tempFilterConstruction) throws RuntimeException {
        ISpaceFilter filterObject = null;
        // create the filter object
        try {
            filterObject = (ISpaceFilter) ClassLoaderHelper.loadClass(classname).newInstance();
        } catch (Exception e) {
            if (_logger.isLoggable(Level.SEVERE)) {
                _logger.log(Level.SEVERE, "Failed to add filter " + filterId + ": " + "newInstance() aborted", e);
            }

            return;
        }

        FilterProvider filterProvider = new FilterProvider(filterId, filterObject);
        filterProvider.setEnabled(true);
        filterProvider.setFilterParam(url);
        filterProvider.setActiveWhenBackup(isActiveWhenBackup);
        filterProvider.setShutdownSpaceOnInitFailure(shutdownSpaceOnInitFailure);
        filterProvider.setPriority(priority);
        filterProvider.setOpCodes(opCodes);
        filterProvider.setPassFilterEntry(passFilterEntry);

        add(filterProvider, tempFilterConstruction);
    }

    private void add(final FilterProvider filterProvider, ArrayList<FilterHolder>[][] tempFilterConstruction) throws RuntimeException {
        if (!filterProvider.isEnabled())
            return;

        final FilterHolder filterHolder = new FilterHolder(filterProvider);

        _filtersRepository.put(filterProvider.getName(), filterHolder);

        for (int i = 0; i < filterProvider.getOpCodes().length; i++) {
            int operationCode = filterProvider.getOpCodes()[i];

            if (tempFilterConstruction[operationCode] == null) {
                tempFilterConstruction[operationCode] = new ArrayList[MAX_FILTER_PRIORITIES];
            }

            if (tempFilterConstruction[operationCode][filterProvider.getPriority()] == null) {
                tempFilterConstruction[operationCode][filterProvider.getPriority()] = new ArrayList<FilterHolder>();
            }

            tempFilterConstruction[operationCode][filterProvider.getPriority()].add(filterHolder);
            _isFilter[operationCode] = true;
        }
    }

    public void close() {

        for (FilterHolder filterHolder : _filtersRepository.values()) {
            try {
                filterHolder.getFilter().close(); // could throw RuntimeException
            } catch (RuntimeException ex) {
                if (_logger.isLoggable(Level.SEVERE)) {
                    _logger.log(Level.SEVERE, "Failed to close filter " + filterHolder.getName(), ex);
                }
            }
        }
    }

    public void invokeFilters(int operationCode, SpaceContext sc, Object subject) throws RuntimeException {
        PrioritySpaceFiltersHolder prioritySpaceFiltersHolder = _filters[operationCode];
        if (prioritySpaceFiltersHolder == null)
            return;

        if (prioritySpaceFiltersHolder.isSingleFilterHolder)
            execute_filter(prioritySpaceFiltersHolder.singleFilterHolder, operationCode, sc, subject);
        else {
            FilterHolder[][] filters_by_priority = prioritySpaceFiltersHolder.prioritizedFilterHolders;
    
            /* operate the filters according to desired priority */
            for (int priority = MAX_FILTER_PRIORITIES - 1; priority >= 0; priority--) {
                FilterHolder[] filters_per_priority = filters_by_priority[priority];

                if (filters_per_priority == null)
                    continue;

                invoke_same_priority_filters(operationCode, sc, subject, filters_per_priority);
            }
        }
    }

    private void invoke_same_priority_filters(int operationCode, SpaceContext sc, Object subject,
                                              FilterHolder[] filters_per_priority) throws RuntimeException {
        for (int i = 0; i < filters_per_priority.length; ++i) {
            FilterHolder filterHolder = filters_per_priority[i];
            if (filterHolder == null)
                continue;

            /* execute the filter process method */
            execute_filter(filterHolder, operationCode, sc, subject);
        } // for
    }

    /**
     * execute the filter process method.
     */
    private void execute_filter(FilterHolder filterHolder, int operationCode, SpaceContext sc, Object subject) {
        if (!filterHolder.isInitialized())
            return;

        if (subject instanceof Object[]) {
            ISpaceFilterEntry[] entries = null;
            if (filterHolder.isPassFilterEntry()) {
                Object[] packets = (Object[]) subject;
                entries = new ISpaceFilterEntry[packets.length];
                for (int i = 0; i < packets.length; ++i)
                    entries[i] = createFilterEntry(packets[i]);
            }
            // execute the filter
            filterHolder.getFilter().process(sc, entries, operationCode);
        } else {
            ISpaceFilterEntry filterEntry = filterHolder.isPassFilterEntry() ? createFilterEntry(subject) : null;

            // execute the filter
            filterHolder.getFilter().process(sc, filterEntry, operationCode);
        }
    }

    /**
     * Creates a Filter Entry out of this 'subject'.
     *
     * @param subject passed to filter - can be: null, EntryPacket, IEntryHolder.
     * @return object implementing ISpaceFilterEntry (or null is 'subject' was null).
     */
    private ISpaceFilterEntry createFilterEntry(Object subject) {
        ISpaceFilterEntry result;

        if (subject == null)
            result = null;
        else if (subject instanceof ISpaceFilterEntry)
            result = (ISpaceFilterEntry) subject;
        else if (subject instanceof IEntryPacket) {
            IEntryPacket packet = (IEntryPacket) subject;
            ITypeDesc typeDesc = packet.getTypeDescriptor();
            if (typeDesc == null)
                typeDesc = _typeManager.getTypeDesc(packet.getTypeName());
            result = new SpaceUpdateFilterEntryImpl(packet, typeDesc);
        } else if (subject instanceof ITemplateHolder) {
            ITemplateHolder templateHolder = (ITemplateHolder) subject;
            ITypeDesc typeDesc = _typeManager.getTypeDesc(templateHolder.getClassName());
            if (!templateHolder.isChange())
                result = new SpaceFilterEntryImpl(templateHolder, typeDesc);
            else {
                Collection<SpaceEntryMutator> mutators = templateHolder.getMutators();
                if (templateHolder.getAnswerHolder() != null
                        && templateHolder.getAnswerHolder()
                        .getAnswerPacket() != null
                        && templateHolder.getAnswerHolder()
                        .getAnswerPacket().m_EntryPacket != null) {

                    IEntryPacket entryPacket = templateHolder.getAnswerHolder()
                            .getAnswerPacket().m_EntryPacket;

                    result = new SpaceAfterChangeFilterEntryImpl(templateHolder, entryPacket, typeDesc, mutators);
                } else
                    result = new SpaceBeforeChangeFilterEntryImpl(templateHolder, typeDesc, mutators);
            }

        } else if (subject instanceof IEntryHolder) {
            IEntryHolder entryHolder = (IEntryHolder) subject;
            ITypeDesc typeDesc = _typeManager.getTypeDesc(entryHolder.getClassName());
            result = new SpaceFilterEntryImpl(entryHolder, typeDesc);
        } else if (subject instanceof NotifyContext) {
            NotifyContext notifyContext = (NotifyContext) subject;
            IEntryHolder entryHolder = notifyContext.getEntry();
            ITypeDesc typeDesc = _typeManager.getTypeDesc(entryHolder.getClassName());
            result = new NotifyEvent(entryHolder, typeDesc, notifyContext);
        } else if (subject instanceof SpaceTaskWrapper)
            result = new ExecutionFilterEntry(((SpaceTaskWrapper) subject).getWrappedTask());
        else
            result = new ExecutionFilterEntry(subject);

        return result;
    }

    /*
     * @see com.j_spaces.core.ISpaceComponentsHandler#initComponents(boolean)
     */
    public void initComponents(boolean primaryOnly) throws SpaceComponentsInitializeException {
        for (FilterHolder filterHolder : _filtersRepository.values()) {
            if (primaryOnly != filterHolder.isPrimaryOnly())
                continue;

            ISpaceFilter filterObject = filterHolder.getFilter();

            try {
                if (filterHolder.isInitialized()) {
                    if (_logger.isLoggable(Level.FINE)) {
                        _logger.log(Level.FINE, "Failed to initialize filter - filter already initialized "
                                + filterHolder.getName());
                    }
                    continue;
                }
                filterObject.init(_space, filterHolder.getName(), filterHolder.getFilterParam(),
                        filterHolder.getPriority());
                filterHolder.setInitialized();
            } catch (RuntimeException ex) {
                if (_logger.isLoggable(Level.SEVERE)) {
                    _logger.log(Level.SEVERE, "Failed to initialize filter " + filterHolder.getName(), ex);
                }

                // Check if space needs to be closed
                if (filterHolder.isShutdownSpaceOnInitFailure()) {
                    throw new SpaceComponentsInitializeException("Failed to initialize filter - "
                            + filterHolder.getName(), ex);
                } else {

                    // If Filter failed to init - close it
                    try {
                        filterObject.close();

                        if (_logger.isLoggable(Level.FINE)) {
                            _logger.fine("FilterManager: Filter: " + filterHolder.getName() + " closed successfully.");
                        }
                    } catch (Exception e) {
                        if (_logger.isLoggable(Level.FINE)) {
                            _logger.log(Level.FINE, "FilterManager:  Failed to close " + filterHolder.getName(), ex);
                        }
                    }
                }
            }
        }

        // Invoke space filters by FilterOperationCodes.ON_INIT
        // deprecated - should be removed in next release
        if (_isFilter[FilterOperationCodes.ON_INIT])
            invokeFilters(FilterOperationCodes.ON_INIT, null, null);
    }

    /*
     * @see com.j_spaces.core.ISpaceComponentsHandler#startComponents(boolean)
     */
    public void startComponents(boolean primaryOnly) {
    }

    /*
     * @see com.gigaspaces.cluster.activeelection.ISpaceComponentsHandler#isRecoverySupported()
     */
    public boolean isRecoverySupported() {
        return true;
    }

    @Override
    public void close(boolean primaryOnly) {
        for (FilterHolder filterHolder : _filtersRepository.values()) {
            if (primaryOnly != filterHolder.isPrimaryOnly())
                continue;

            try {
                filterHolder.close(); // could throw RuntimeException
            } catch (RuntimeException ex) {
                if (_logger.isLoggable(Level.SEVERE)) {
                    _logger.log(Level.SEVERE, "Failed to close filter " + filterHolder.getName(), ex);
                }
            }
        }
    }

    public boolean hasFilterRequiresFullSpaceFilterEntry(int operationCode) {
        PrioritySpaceFiltersHolder prioritySpaceFiltersHolder = _filters[operationCode];
        if (prioritySpaceFiltersHolder == null)
            return false;
        return prioritySpaceFiltersHolder.hasFilterRequiresFullSpaceFilterEntry;
    }

}
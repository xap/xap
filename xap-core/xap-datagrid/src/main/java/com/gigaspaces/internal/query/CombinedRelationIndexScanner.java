package com.gigaspaces.internal.query;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.j_spaces.core.cache.QueryExtensionIndexManagerWrapper;
import com.j_spaces.core.cache.TypeData;
import com.j_spaces.core.cache.TypeDataIndex;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.kernel.list.IObjectsList;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

public class CombinedRelationIndexScanner extends AbstractQueryIndex {

    private static final long serialVersionUID = 1L;
    private List<IQueryIndexScanner> relationIndexScannerList;

    public CombinedRelationIndexScanner() {
        super();
    }

    public CombinedRelationIndexScanner(List<IQueryIndexScanner> queryIndexes) {
        this.relationIndexScannerList = queryIndexes;
    }

    @Override
    protected boolean hasIndexValue() {
        return true;
    }

    @Override
    public IObjectsList getIndexedEntriesByType(Context context, TypeData typeData,
                                                ITemplateHolder template, int latestIndexToConsider) {
        return getEntriesByIndex(context, typeData, null /*index*/, false /*fifoGroupsScan*/);
    }


    @Override
    protected IObjectsList getEntriesByIndex(Context context, TypeData typeData, TypeDataIndex<Object> index, boolean fifoGroupsScan) {
        String typeName = typeData.getClassName();
        if (!relationIndexScannerList.isEmpty()) {
            String namespace = ((RelationIndexScanner) relationIndexScannerList.get(0)).getNamespace();
            QueryExtensionIndexManagerWrapper handler = typeData.getCacheManager().getQueryExtensionManager(namespace);
            if (handler == null) return IQueryIndexScanner.RESULT_IGNORE_INDEX;
            return handler.scanIndex(typeName, relationIndexScannerList);
        }
        return IQueryIndexScanner.RESULT_IGNORE_INDEX;
    }

    @Override
    public Object getIndexValue() {
        return null;
    }

    @Override
    public boolean requiresOrderedIndex() {
        return false;
    }

    @Override
    public boolean supportsTemplateIndex() {
        return false;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readExternal(in);
        relationIndexScannerList = IOUtils.readList(in);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        IOUtils.writeList(out, relationIndexScannerList);
    }
}

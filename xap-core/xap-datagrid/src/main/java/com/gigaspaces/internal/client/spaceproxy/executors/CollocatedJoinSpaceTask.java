package com.gigaspaces.internal.client.spaceproxy.executors;

import com.gigaspaces.async.AsyncResult;
import com.gigaspaces.internal.space.requests.CollocatedJoinSpaceRequestInfo;
import com.gigaspaces.internal.space.requests.SpaceRequestInfo;
import com.gigaspaces.internal.space.responses.CollocatedJoinSpaceResponseInfo;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.j_spaces.jdbc.*;
import com.j_spaces.jdbc.query.IQueryResultSet;
import com.j_spaces.jdbc.query.JoinedQueryResult;
import net.jini.core.transaction.Transaction;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.SQLException;
import java.util.List;

/**
 * @author yohanakh
 * @since 15.8.0
 */
@com.gigaspaces.api.InternalApi
public class CollocatedJoinSpaceTask extends SystemDistributedTask<CollocatedJoinSpaceResponseInfo> {
    private static final long serialVersionUID = 1L;

    private CollocatedJoinSpaceRequestInfo _collocatedJoinSpaceRequestInfo;

    public CollocatedJoinSpaceTask() {
    }

    public CollocatedJoinSpaceTask(AbstractDMLQuery query, Transaction txn, int readModifier, int max) {
        this._collocatedJoinSpaceRequestInfo = new CollocatedJoinSpaceRequestInfo(query, txn, readModifier, max);
    }

    @Override
    public SpaceRequestInfo getSpaceRequestInfo() {
        return _collocatedJoinSpaceRequestInfo;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(_collocatedJoinSpaceRequestInfo);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        this._collocatedJoinSpaceRequestInfo = (CollocatedJoinSpaceRequestInfo) in.readObject();
    }

    @Override
    public CollocatedJoinSpaceResponseInfo reduce(List<AsyncResult<CollocatedJoinSpaceResponseInfo>> asyncResults) throws Exception {
        JoinedQueryResult res = new JoinedQueryResult();
        for (AsyncResult<CollocatedJoinSpaceResponseInfo> asyncResult : asyncResults) {
            if (asyncResult.getException() != null) {
                throw new RuntimeException(asyncResult.getException());
            }
            res.addAll(asyncResult.getResult().getResult());
        }

//        applyOrderByIfNeeded(res);
        res = applyLimitIfNeeded(res);
        return new CollocatedJoinSpaceResponseInfo(res);
    }

    private JoinedQueryResult applyLimitIfNeeded(JoinedQueryResult res) throws SQLException {
        SelectQuery query = ((SelectQuery) _collocatedJoinSpaceRequestInfo.getQuery());
        if (query.getJoins() == null) return res;
        for (Join join : query.getJoins()) {
            if (join.getSubQuery() != null && ((SelectQuery) join.getSubQuery()).getLimit() != 0) {
                SelectQuery subQuery = ((SelectQuery) join.getSubQuery());
                for (SelectColumn groupByColumn : subQuery.getGroupColumn()) {
                    groupByColumn.getColumnTableData().setTableIndexUnsafe(1);
                    for (SelectColumn queryColumn : subQuery.getQueryColumns()) {
                        if (groupByColumn.getName().equals(queryColumn.getName()) || (queryColumn.hasAlias() && groupByColumn.getName().equals(queryColumn.getAlias()))) {
                            groupByColumn.setProjectedIndex(queryColumn.getProjectedIndex());
                            groupByColumn.getColumnData().setColumnIndexInTableUnsafe(queryColumn.getProjectedIndex());
                        }
                    }
                }
                for (SelectColumn queryColumn : subQuery.getQueryColumns()) {
                    queryColumn.getColumnData().setColumnIndexInTableUnsafe(queryColumn.getProjectedIndex());
                }
                for (OrderColumn orderColumn : subQuery.getOrderColumns()) {
                    orderColumn.getColumnTableData().setTableIndexUnsafe(1); //the table is now the second one = 1
                }
                IQueryResultSet<IEntryPacket> res2 = subQuery.getExecutor().groupByAndKeepOrder(res, subQuery.getGroupColumn(), subQuery.getOrderColumns(), subQuery.getLimit());
                res = new JoinedQueryResult(res2);
            }
        }
        return res;
    }

    private void applyOrderByIfNeeded(JoinedQueryResult res) throws SQLException {
        SelectQuery query = ((SelectQuery) _collocatedJoinSpaceRequestInfo.getQuery());
        if (query.getJoins() == null) return;

        if (query.getJoins().size() == 1 && query.getJoins().get(0).getSubQuery() != null && ((SelectQuery) query.getJoins().get(0).getSubQuery()).getOrderColumns().size() > 0) {
            SelectQuery subQuery = ((SelectQuery) query.getJoins().get(0).getSubQuery());
            for (OrderColumn orderColumn : subQuery.getOrderColumns()) {
                orderColumn.getColumnTableData().setTableIndexUnsafe(1); //the table is now the second one = 1
            }
            subQuery.getExecutor().orderBy(res, subQuery.getOrderColumns());
        } else if (query.getJoins().size() > 1) {
            for (Join join : query.getJoins()) {
                if (join.getSubQuery() != null && ((SelectQuery) join.getSubQuery()).getOrderColumns().size() > 0) {
                    throw new SQLException("Unsupported join query - detected more than one join with group by");
                }
            }
        }
    }
}

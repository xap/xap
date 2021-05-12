package com.gigaspaces.jdbc;

public interface PhysicalPlanHandler<T> {
    QueryExecutor prepareForExecution(T t);
}

package com.gigaspaces.internal.server.space.schema_evolution;

import com.gigaspaces.datasource.*;
import com.gigaspaces.metadata.SpaceTypeDescriptor;

public class SchemaAdapterSpaceDataSource extends SpaceDataSource {
    private final SpaceDataSource spaceDataSource;
    private final SpaceTypeSchemaAdapterContainer spaceTypeSchemaAdapterContainer;

    public SchemaAdapterSpaceDataSource(SpaceDataSource spaceDataSource, SpaceTypeSchemaAdapterContainer spaceTypeSchemaAdapterContainer) {
        this.spaceDataSource = spaceDataSource;
        this.spaceTypeSchemaAdapterContainer = spaceTypeSchemaAdapterContainer;
    }

    @Override
    public DataIterator<SpaceTypeDescriptor> initialMetadataLoad() {
        return new SchemaAdapterDataIterator<>(spaceDataSource.initialMetadataLoad());
    }

    @Override
    public DataIterator<Object> initialDataLoad() {
        return new SchemaAdapterDataIterator<>(spaceDataSource.initialDataLoad());
    }

    @Override
    public DataIterator<Object> getDataIterator(DataSourceQuery query) {
        return new SchemaAdapterDataIterator<>(spaceDataSource.getDataIterator(query));
    }

    @Override
    public Object getById(DataSourceIdQuery idQuery) {
        return spaceTypeSchemaAdapterContainer.adapt(spaceDataSource.getById(idQuery));
    }

    @Override
    public DataIterator<Object> getDataIteratorByIds(DataSourceIdsQuery idsQuery) {
        return new SchemaAdapterDataIterator<>(spaceDataSource.getDataIteratorByIds(idsQuery));
    }

    @Override
    public boolean supportsInheritance() {
        return spaceDataSource.supportsInheritance();
    }

    class SchemaAdapterDataIterator<T> implements DataIterator<T>{
        DataIterator<T> dataIterator;

        public SchemaAdapterDataIterator(DataIterator<T> dataIterator) {
            this.dataIterator = dataIterator;
        }

        @Override
        public void close() {
            if(dataIterator != null)
                dataIterator.close();
        }

        @Override
        public boolean hasNext() {
            if(dataIterator != null)
                return dataIterator.hasNext();
            return false;
        }

        @Override
        public T next() {
            if(dataIterator == null)
                return null;
            return (T) spaceTypeSchemaAdapterContainer.adapt(dataIterator.next()); }
    }
}

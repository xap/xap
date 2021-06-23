package org.openspaces.core.config;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.server.space.tiered_storage.TieredStorageConfig;
import com.gigaspaces.internal.server.space.tiered_storage.TieredStorageTableConfig;
import com.gigaspaces.serialization.SmartExternalizable;
import com.gigaspaces.server.SpaceCustomComponent;
import org.openspaces.core.extension.SpaceCustomComponentFactoryBean;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Yael Nahon
 * @since 16.0
 */
public class TieredStorageConfigurer implements SpaceCustomComponentFactoryBean, SmartExternalizable {
    static final long serialVersionUID = 8399147971664655711L;

    private Map<String, TieredStorageTableConfig> tables = new HashMap<>();

    @Override
    public SpaceCustomComponent createSpaceComponent() {
        return new TieredStorageConfig().setTables(tables);
    }

    public TieredStorageConfigurer addTable(TieredStorageTableConfig tableConfig) {
        TieredStorageTableConfig config = tables.putIfAbsent(tableConfig.getName(), tableConfig);
        if (config != null) {
            throw new IllegalArgumentException("cannot define a table twice (" + config.getName() + " already defined)");
        }
        return this;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeMapStringT(out,tables);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        tables = IOUtils.readMapStringT(in);

    }
}

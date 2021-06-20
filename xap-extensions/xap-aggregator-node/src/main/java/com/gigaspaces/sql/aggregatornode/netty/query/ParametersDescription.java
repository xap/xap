package com.gigaspaces.sql.aggregatornode.netty.query;

import com.gigaspaces.sql.aggregatornode.netty.utils.PgType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableList;

public class ParametersDescription {
    private final List<ParameterDescription> parameters;

    public ParametersDescription() {
        this.parameters = Collections.emptyList();
    }

    public ParametersDescription(List<ParameterDescription> parameters) {
        this.parameters = parameters;
    }

    public ParametersDescription(int[] types) {
        this.parameters = Arrays.stream(types)
                .mapToObj(PgType::getType)
                .map(ParameterDescription::new)
                .collect(Collectors.toList());
    }

    public int getParametersCount() {
        return parameters.size();
    }
    public List<ParameterDescription> getParameters() {
        return unmodifiableList(parameters);
    }
}

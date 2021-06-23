package com.gigaspaces.sql.aggregatornode.netty.query;

import com.gigaspaces.sql.aggregatornode.netty.utils.TypeUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableList;

public class ParametersDescription {
    public static ParametersDescription EMPTY = new ParametersDescription(Collections.emptyList());

    private final List<ParameterDescription> parameters;

    public ParametersDescription(List<ParameterDescription> parameters) {
        this.parameters = parameters;
    }

    public ParametersDescription(int[] types) {
        this.parameters = Arrays.stream(types)
                .mapToObj(TypeUtils::getType)
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

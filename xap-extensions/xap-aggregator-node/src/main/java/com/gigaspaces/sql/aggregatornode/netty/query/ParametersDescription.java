package com.gigaspaces.sql.aggregatornode.netty.query;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableList;

public class ParametersDescription {
    private List<ParameterDescription> parameters;

    public ParametersDescription(int[] types) {
        this.parameters = Arrays.stream(types).mapToObj(ParameterDescription::new).collect(Collectors.toList());
    }

    public ParametersDescription(List<ParameterDescription> parameters) {
        this.parameters = parameters;
    }

    public int getParametersCount() {
        return parameters.size();
    }
    public List<ParameterDescription> getParameters() {
        return unmodifiableList(parameters);
    }
}

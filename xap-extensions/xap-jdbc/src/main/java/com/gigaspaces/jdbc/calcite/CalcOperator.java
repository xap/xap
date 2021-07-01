package com.gigaspaces.jdbc.calcite;

import com.gigaspaces.internal.metadata.TypeDesc;
import com.gigaspaces.internal.transport.ProjectionTemplate;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;

import java.util.List;

public class CalcOperator implements ISQLOperator {

    private final QueryTemplatePacketsHolder queryTemplatePacketsHolder;
    private final List<String> projection;
//    private final PropertyInfo[] allColumnInfo;

    public CalcOperator(QueryTemplatePacketsHolder queryTemplatePacketsHolder, List<String> projection) {
        this.queryTemplatePacketsHolder = queryTemplatePacketsHolder;
        this.projection = projection;


//        int idPropertyIndex = typeDesc.getIdentifierPropertyId();
//        int index = 0;
//        int nonIdPropertyIndex = 1;
//        allColumnInfo = new PropertyInfo[typeDesc.getNumOfFixedProperties()];
//        while (index < allColumnInfo.length) { // put the SPACE ID as the first column.
//            if (index == idPropertyIndex) {
//                allColumnInfo[0] = typeDesc.getFixedProperty(index);
//            } else {
//                allColumnInfo[nonIdPropertyIndex++] = typeDesc.getFixedProperty(index);
//            }
//            index++;
//        }
    }

    @Override
    public QueryTemplatePacketsHolder build() {

        for (QueryTemplatePacket queryTemplatePacket : queryTemplatePacketsHolder.getQueryTemplatePackets().values()) {
            //Projects
            String[] projects =
                    projection.stream().filter(filedName -> queryTemplatePacket.getTypeDescriptor().getFixedPropertyPosition(filedName) != TypeDesc.NO_SUCH_PROPERTY).toArray(String[]::new);
            ProjectionTemplate _projectionTemplate = ProjectionTemplate.create(projects, queryTemplatePacket.getTypeDescriptor());
            queryTemplatePacket.setProjectionTemplate(_projectionTemplate);
        }

        return queryTemplatePacketsHolder;
    }
}
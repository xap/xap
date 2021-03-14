package com.gigaspaces.jdbc.output;

import java.util.List;

/**
 * Created by moran on 6/8/18.
 */
public class PlainTextOutput {
    private final static String TABS = "";
    private final static String NEWLINE = System.lineSeparator();
    private final static char SPACE = ' ';
    private final Output output;

    public PlainTextOutput(Output output) {
        this.output = output;
    }

    public String getOutput() {
        StringBuilder sb = new StringBuilder();
        final List<String> columns = output.getColumns();
        sb.append("| ");
        for (int i=0; i<columns.size(); i++) {
            final String column = columns.get(i);
            sb.append(column);
            addPadding(sb, i, column.length());
            sb.append(TABS);
            if (i != columns.size() -1) sb.append(" | ");

        }
        sb.append(" |");
        if( !columns.isEmpty() ) {
            sb.append(NEWLINE);
            addL(sb, columns);
            sb.append(NEWLINE);
        }

        final List<List<String>> rows = output.getRows();
        for (List<String> row : rows) {
            sb.append("| ");
            for (int i=0; i<row.size(); i++) {
                final String data = row.get(i);
                sb.append(data);
                addPadding(sb, i, data.length());
                sb.append(TABS);
                if (i != row.size() -1) sb.append(" | ");
            }
            sb.append(" |");
            sb.append(NEWLINE);
        }

        return sb.toString();
    }

    private void addPadding(StringBuilder sb, int index, int length) {
        final List<Integer> columnWidth = output.getColumnWidth();
        final int padding = columnWidth.get(index) - length;
        for (int p=0; p<padding; p++) {
            sb.append(SPACE);
        }
    }

    private void addL(StringBuilder sb, List<String> columns) {
        sb.append("| ");
        final List<Integer> columnWidth = output.getColumnWidth();
        for (int i = 0; i < columns.size(); i++) {
            for (int p=0; p<columnWidth.get(i); p++) {
                sb.append("-");
            }
//            sb.append("--");
            if (i != columns.size() -1) sb.append(" | ");
        }
        sb.append(" |");
    }
}

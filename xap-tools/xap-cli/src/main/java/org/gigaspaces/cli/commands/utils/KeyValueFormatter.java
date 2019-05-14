package org.gigaspaces.cli.commands.utils;

/**
 * @author Niv Ingberg
 * @since 14.5
 */
public class KeyValueFormatter {
    private final StringBuilder sb = new StringBuilder();
    private final String format;

    private KeyValueFormatter(Builder builder) {
        this.format = "%-" + builder.width + "s" + builder.separator + "%s%n";
    }

    public KeyValueFormatter append(String key, Object value) {
        sb.append(String.format(format, key, value));
        return this;
    }

    public KeyValueFormatter append(String s) {
        sb.append(s);
        return this;
    }

    public KeyValueFormatter appendLineSeparator() {
        return append(System.lineSeparator());
    }

    public String get() {
        return sb.toString();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private int width = 16;
        private String separator = " ";

        public Builder width(int width) {
            this.width = width;
            return this;
        }

        public Builder separator(String separator) {
            this.separator = separator;
            return this;
        }

        public KeyValueFormatter build() {
            return new KeyValueFormatter(this);
        }
    }
}

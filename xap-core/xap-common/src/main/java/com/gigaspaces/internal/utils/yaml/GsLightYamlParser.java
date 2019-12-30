package com.gigaspaces.internal.utils.yaml;

import com.gigaspaces.api.InternalApi;
import com.gigaspaces.internal.io.BootIOUtils;

import java.io.*;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A light yaml parser, providing common yaml functionality with no external dependencies.
 * For internal GigaSpaces usage only - API and functionality might change between versions.
 *
 * @author Niv Ingberg
 * @since 15.2
 */
@InternalApi
public class GsLightYamlParser implements YamlParser {
    @Override
    public Map<String, Object> parse(InputStream inputStream) throws IOException {
        Context context = new Context();
        try (Stream<String> lines = BootIOUtils.lines(inputStream)) {
            lines.map(context::stripComment).forEach(context::processLine);
        }
        return context.latestNode.findRoot().generateValuesTree();
    }

    private static class Context {
        private static final String LIST_PREFIX = "- ";
        private static final String LIST_PREFIX_REPLACEMENT = stringWithSameChar(' ', LIST_PREFIX.length());

        private int currLine = 0;
        private Node latestNode = new Node();
        private final Function<String, Object> scalarAdapter = this::guessType;

        private void processLine(String line) {
            currLine++;
            String trimmedLine = line.trim();
            // Ignore empty lines or lines starting with # (comments)
            if (trimmedLine.isEmpty())
                return;

            if (trimmedLine.startsWith(LIST_PREFIX)) {
                boolean isTuple = line.contains(": ");
                latestNode = newListItemNode(line, isTuple);
                if (isTuple) {
                    latestNode = newKeyValueNode(line.replaceFirst(LIST_PREFIX, LIST_PREFIX_REPLACEMENT));
                }
            } else {
                latestNode = newKeyValueNode(line);
            }
        }

        private Node newKeyValueNode(String s) {
            int colonPos = s.indexOf(':');
            if (colonPos == -1)
                throw new IllegalStateException("Line #" + currLine + " does not contain ':'");

            String key = s.substring(0, colonPos).trim();
            int indent = colonPos - key.length();
            Object value = toValue(s.substring(colonPos + 1));
            return new Node(key, value, indent, latestNode.findParent(indent));
        }

        private Node newListItemNode(String s, boolean isTuple) {
            int indent = s.indexOf(LIST_PREFIX);
            Object value = isTuple ? null : toValue(s.substring(indent + LIST_PREFIX.length()));
            return new Node(null, value, indent, latestNode.findParent(indent));
        }

        private Object toValue(String s) {
            return scalarAdapter.apply(s.trim());
        }

        private String stripComment(String s) {
            if (s == null || s.length() == 0)
                return s;
            if (s.startsWith("#"))
                return "";

            boolean inSingleComment = false;
            boolean inDoubleComment = false;
            for (int i = 0 ; i < s.length() ; i++) {
                char c = s.charAt(i);
                switch (c) {
                    case '\'':
                        if (!inDoubleComment)
                            inSingleComment = !inSingleComment;
                        break;
                    case '"':
                        if (!inSingleComment)
                            inDoubleComment = !inDoubleComment;
                        break;
                    case '#':
                        if (!inSingleComment && !inDoubleComment) {
                            boolean escaped = i != 0 && s.charAt(i-1) == '\\';
                            if (!escaped)
                                return s.substring(0, i);
                        }
                        break;
                    default:
                        break;
                }
            }
            return s;
        }

        private Object guessType(String s) {
            // Boolean:
            if (s.equals(Boolean.TRUE.toString()))
                return Boolean.TRUE;
            if (s.equals(Boolean.FALSE.toString()))
                return Boolean.FALSE;
            // Integer:
            try {
                return Integer.parseInt(s);
            } catch (NumberFormatException e) {
                // Ignore
            }
            // Double:
            try {
                return Double.parseDouble(s);
            } catch (NumberFormatException e) {
                // Ignore
            }
            if (s.isEmpty())
                return null;
            // String:
            return unquoteIfNeeded(s);
        }

        private static String unquoteIfNeeded(String s) {
            if ((s.startsWith("\"") && s.endsWith("\"")) ||
                    (s.startsWith("'") && s.endsWith("'")))
                return s.substring(1, s.length()-1);
            return s;
        }

        private static String stringWithSameChar(char c, int length) {
            char[] charArray = new char[length];
            Arrays.fill(charArray, c);
            return new String(charArray);
        }
    }

    private static class Node {
        private final String key;
        private final Object value;
        private final int indent;
        private final Node parent;
        private final List<Node> children = new ArrayList<>();

        private Node() {
            this(null, null, -1, null);
        }

        private Node(String key, Object value, int indent, Node parent) {
            this.key = key;
            this.value = value;
            this.indent = indent;
            this.parent = parent;
            if (parent != null)
                this.parent.children.add(this);
        }

        private Node findParent(int indent) {
            Node node = this;
            while (indent <= node.indent)
                node = node.parent;
            return node;
        }

        Node findRoot() {
            Node node = this;
            while (node.parent != null)
                node = node.parent;
            return node;
        }

        Map<String, Object> generateValuesTree() {
            Map<String, Object> result = new LinkedHashMap<>();
            for (Node child : children) {
                result.put(child.key, child.getValue());
            }

            return result;
        }

        Object getValue() {
            if (children.isEmpty())
                return value;
            if (children.get(0).key == null) {
                return children.stream().map(Node::getValue).collect(Collectors.toList());
            }
            return generateValuesTree();
        }
    }
}

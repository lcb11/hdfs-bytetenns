package com.ruyuan.dfs.client.utils;


import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 打印工具
 *
 * @author Sun Dasheng
 */
public class ConsoleTable {

    private Header header;
    private Body body;
    String lineSep = "\n";
    String verticalSep = "|";
    String horizontalSep = "-";
    String joinSep = "+";
    int[] columnWidths;
    NullPolicy nullPolicy = NullPolicy.EMPTY_STRING;
    boolean restrict = false;

    private ConsoleTable() {
    }

    public void print() {
        System.out.println(getContent());
    }

    String getContent() {
        return toString();
    }

    List<String> getLines() {
        List<String> lines = new ArrayList<>();
        if ((header != null && !header.isEmpty()) || (body != null && !body.isEmpty())) {
            lines.addAll(header.print(columnWidths, horizontalSep, verticalSep, joinSep));
            lines.addAll(body.print(columnWidths, horizontalSep, verticalSep, joinSep));
        }
        return lines;
    }

    @Override
    public String toString() {
        return StringUtils.join(getLines(), lineSep);
    }

    public static class ConsoleTableBuilder {

        ConsoleTable consoleTable = new ConsoleTable();

        public ConsoleTableBuilder() {
            consoleTable.header = new Header();
            consoleTable.body = new Body();
        }

        public ConsoleTableBuilder addHead(Cell cell) {
            consoleTable.header.addHead(cell);
            return this;
        }

        public ConsoleTableBuilder addRow(List<Cell> row) {
            consoleTable.body.addRow(row);
            return this;
        }

        public ConsoleTableBuilder addHeaders(List<Cell> headers) {
            consoleTable.header.addHeads(headers);
            return this;
        }

        public ConsoleTableBuilder addRows(List<List<Cell>> rows) {
            consoleTable.body.addRows(rows);
            return this;
        }

        public ConsoleTableBuilder lineSep(String lineSep) {
            consoleTable.lineSep = lineSep;
            return this;
        }

        public ConsoleTableBuilder verticalSep(String verticalSep) {
            consoleTable.verticalSep = verticalSep;
            return this;
        }

        public ConsoleTableBuilder horizontalSep(String horizontalSep) {
            consoleTable.horizontalSep = horizontalSep;
            return this;
        }

        public ConsoleTableBuilder joinSep(String joinSep) {
            consoleTable.joinSep = joinSep;
            return this;
        }

        public ConsoleTableBuilder nullPolicy(NullPolicy nullPolicy) {
            consoleTable.nullPolicy = nullPolicy;
            return this;
        }

        public ConsoleTableBuilder restrict(boolean restrict) {
            consoleTable.restrict = restrict;
            return this;
        }

        public ConsoleTable build() {
            //compute max column widths
            if (!consoleTable.header.isEmpty() || !consoleTable.body.isEmpty()) {
                List<List<Cell>> allRows = new ArrayList<>();
                allRows.add(consoleTable.header.cells);
                allRows.addAll(consoleTable.body.rows);
                int maxColumn = allRows.stream().map(List::size).mapToInt(size -> size).max().getAsInt();
                int minColumn = allRows.stream().map(List::size).mapToInt(size -> size).min().getAsInt();
                if (maxColumn != minColumn && consoleTable.restrict) {
                    throw new IllegalArgumentException("number of columns for each row must be the same when strict mode used.");
                }
                consoleTable.columnWidths = new int[maxColumn];
                for (List<Cell> row : allRows) {
                    for (int i = 0; i < row.size(); i++) {
                        Cell cell = row.get(i);
                        if (cell == null || cell.getValue() == null) {
                            cell = consoleTable.nullPolicy.getCell(cell);
                            row.set(i, cell);
                        }
                        int length = strLength(cell.getValue());
                        if (consoleTable.columnWidths[i] < length) {
                            consoleTable.columnWidths[i] = length;
                        }
                    }
                }
            }
            return consoleTable;
        }
    }

    private static class Header {
        public List<Cell> cells;

        public Header() {
            this.cells = new ArrayList<>();
        }

        public void addHead(Cell cell) {
            cells.add(cell);
        }

        public void addHeads(List<Cell> headers) {
            cells.addAll(headers);
        }

        public boolean isEmpty() {
            return cells == null || cells.isEmpty();
        }

        public List<String> print(int[] columnWidths, String horizontalSep, String verticalSep, String joinSep) {
            List<String> result = new ArrayList<>();
            if (!isEmpty()) {
                //top horizontal sep line
                result.addAll(printLineSep(columnWidths, horizontalSep, verticalSep, joinSep));
                //header row
                result.addAll(printRows(Collections.singletonList(cells), columnWidths, verticalSep));
            }
            return result;
        }
    }

    public enum Align {
        LEFT, RIGHT, CENTER
    }

    private enum NullPolicy {
        THROW {
            @Override
            public Cell getCell(Cell cell) {
                throw new IllegalArgumentException("cell or value is null: " + cell);
            }
        },
        NULL_STRING {
            @Override
            public Cell getCell(Cell cell) {
                if (cell == null) {
                    return new Cell("null");
                }
                cell.setValue("null");
                return cell;
            }
        },
        EMPTY_STRING {
            @Override
            public Cell getCell(Cell cell) {
                if (cell == null) {
                    return new Cell("");
                }
                cell.setValue("");
                return cell;
            }
        };

        public abstract Cell getCell(Cell cell);

    }

    @Data
    public static class Cell {
        private Align align;
        private String value;
        public Cell(Align align, String value) {
            this.align = align;
            this.value = value;
        }
        public Cell(String value) {
            this.align = Align.LEFT;
            this.value = value;
        }
    }

    private static class Body {
        public List<List<Cell>> rows;

        public Body() {
            rows = new ArrayList<>();
        }

        public void addRow(List<Cell> row) {
            this.rows.add(row);
        }

        public void addRows(List<List<Cell>> rows) {
            this.rows.addAll(rows);
        }

        public boolean isEmpty() {
            return rows == null || rows.isEmpty();
        }

        public List<String> print(int[] columnWidths, String horizontalSep, String verticalSep, String joinSep) {
            List<String> result = new ArrayList<>();
            if (!isEmpty()) {
                //top horizontal sep line
                result.addAll(printLineSep(columnWidths, horizontalSep, verticalSep, joinSep));
                //rows
                result.addAll(printRows(rows, columnWidths, verticalSep));
                //bottom horizontal sep line
                result.addAll(printLineSep(columnWidths, horizontalSep, verticalSep, joinSep));
            }
            return result;
        }
    }

    public static List<String> printRows(List<List<Cell>> rows, int[] columnWidths, String verticalSep) {
        List<String> result = new ArrayList<>();
        for (List<Cell> row : rows) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < row.size(); i++) {
                Cell cell = row.get(i);
                if (cell == null) {
                    cell = new Cell("");
                }
                //add v-sep after last column
                String verStrTemp = i == row.size() - 1 ? verticalSep : "";
                Align align = cell.getAlign();
                switch (align) {
                    case LEFT:
                        sb.append(String.format("%s %s %s", verticalSep, rightPad(cell.getValue(), columnWidths[i]), verStrTemp));
                        break;
                    case RIGHT:
                        sb.append(String.format("%s %s %s", verticalSep, leftPad(cell.getValue(), columnWidths[i]), verStrTemp));
                        break;
                    case CENTER:
                        sb.append(String.format("%s %s %s", verticalSep, center(cell.getValue(), columnWidths[i]), verStrTemp));
                        break;
                    default:
                        throw new IllegalArgumentException("wrong align : " + align.name());
                }
            }
            result.add(sb.toString());
        }
        return result;
    }

    public static List<String> printLineSep(int[] columnWidths, String horizontalSep, String verticalSep, String joinSep) {
        StringBuilder line = new StringBuilder();
        for (int i = 0; i < columnWidths.length; i++) {
            String l = String.join("", Collections.nCopies(columnWidths[i] +
                    strLength(verticalSep) + 1, horizontalSep));
            line.append(joinSep).append(l).append(i == columnWidths.length - 1 ? joinSep : "");
        }
        return Collections.singletonList(line.toString());
    }

    public static String leftPad(String str, int size, char c) {
        if (str == null) {
            return null;
        }

        int strLength = strLength(str);
        if (size <= 0 || size <= strLength) {
            return str;
        }
        return repeat(size - strLength, c).concat(str);
    }

    public static String rightPad(String str, int size, char c) {
        if (str == null) {
            return null;
        }

        int strLength = strLength(str);
        if (size <= 0 || size <= strLength) {
            return str;
        }
        return str.concat(repeat(size - strLength, c));
    }

    public static String center(String str, int size, char c) {
        if (str == null) {
            return null;
        }

        int strLength = strLength(str);
        if (size <= 0 || size <= strLength) {
            return str;
        }
        str = leftPad(str, strLength + (size - strLength) / 2, c);
        str = rightPad(str, size, c);
        return str;
    }

    public static String leftPad(String str, int size) {
        return leftPad(str, size, ' ');
    }

    public static String rightPad(String str, int size) {
        return rightPad(str, size, ' ');
    }

    public static String center(String str, int size) {
        return center(str, size, ' ');
    }

    private static String repeat(int size, char c) {
        StringBuilder s = new StringBuilder();
        for (int index = 0; index < size; index++) {
            s.append(c);
        }
        return s.toString();
    }

    public static int strLength(String str) {
        return strLength(str, "UTF-8");
    }

    public static int strLength(String str, String charset) {
        int len = 0;
        int j = 0;
        byte[] bytes = str.getBytes(Charset.forName(charset));
        while (bytes.length > 0) {
            short tmpst = (short) (bytes[j] & 0xF0);
            if (tmpst >= 0xB0) {
                if (tmpst < 0xC0) {
                    j += 2;
                    len += 2;
                } else if ((tmpst == 0xC0) || (tmpst == 0xD0)) {
                    j += 2;
                    len += 2;
                } else if (tmpst == 0xE0) {
                    j += 3;
                    len += 2;
                } else if (tmpst == 0xF0) {
                    short tmpst0 = (short) (((short) bytes[j]) & 0x0F);
                    if (tmpst0 == 0) {
                        j += 4;
                        len += 2;
                    } else if ((tmpst0 > 0) && (tmpst0 < 12)) {
                        j += 5;
                        len += 2;
                    } else if (tmpst0 > 11) {
                        j += 6;
                        len += 2;
                    }
                }
            } else {
                j += 1;
                len += 1;
            }
            if (j > bytes.length - 1) {
                break;
            }
        }
        return len;
    }
}

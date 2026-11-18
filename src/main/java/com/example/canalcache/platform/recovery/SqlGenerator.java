package com.example.canalcache.platform.recovery;

import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.example.canalcache.model.BinlogEvent;
import com.example.canalcache.platform.history.TableSchema;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.*;

public class SqlGenerator {
    public static final Set<Integer> SUPPORTED_TYPES = Set.of(Types.BIGINT, Types.INTEGER, Types.SMALLINT, Types.TINYINT,
            Types.DECIMAL, Types.NUMERIC, Types.VARCHAR, Types.CHAR, Types.LONGVARCHAR, Types.DATE, Types.TIMESTAMP,
            Types.BIT, Types.BOOLEAN, Types.FLOAT, Types.REAL, Types.DOUBLE, Types.TIME,
            Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY, Types.BLOB, Types.CLOB,
            Types.NCHAR, Types.NVARCHAR, Types.LONGNVARCHAR);
    private static final ObjectMapper JSON = new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
            .enable(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS);
    private static final java.util.regex.Pattern TIME = java.util.regex.Pattern.compile("(-?)(\\d{1,3}):([0-5]\\d):([0-5]\\d)(?:\\.(\\d{1,6}))?");
    public record Eligibility(boolean eligible, String reason, List<String> unsupportedColumns) { }
    public record GeneratedSql(String sql, String reverseType, String explanation) { }

    public static Eligibility checkEligibility(TableSchema schema) {
        if (schema == null) return new Eligibility(false, "Table schema snapshot not found", List.of());
        if (schema.primaryKey() == null || schema.primaryKey().isBlank()) return new Eligibility(false, "Table has no single primary key", List.of());
        if (schema.recoveryRestriction() != null) return new Eligibility(false, schema.recoveryRestriction(), List.of());
        List<String> unsupported = new ArrayList<>();
        for (var col : schema.columns()) {
            if (col.generated()) unsupported.add(col.name() + " (GENERATED COLUMN)");
            else if ((!SUPPORTED_TYPES.contains(col.jdbcType()) && !isJson(col))
                    || col.typeName().toUpperCase(Locale.ROOT).matches("^(GEOMETRY|POINT|LINESTRING|POLYGON|MULTIPOINT|MULTILINESTRING|MULTIPOLYGON|GEOMCOLLECTION).*")) {
                unsupported.add(col.name() + " (" + col.typeName() + ")");
            }
        }
        return unsupported.isEmpty() ? new Eligibility(true, "OK", List.of())
                : new Eligibility(false, "Table contains unsupported column types: " + String.join(", ", unsupported), unsupported);
    }

    public static void validateEvent(BinlogEvent event, TableSchema schema, ZoneId binlogTimeZone) {
        Eligibility eligibility = checkEligibility(schema);
        if (!eligibility.eligible()) throw new IllegalArgumentException(eligibility.reason());
        if (!schema.table().equals(event.getTableName()) || !schema.database().equals(event.getDatabaseName())) {
            throw new IllegalArgumentException("Event does not match its schema snapshot");
        }
        if (event.getEventType() == null || event.isDdl()) throw new IllegalArgumentException("Not a recoverable row event");
        if (event.getEventType() != EventType.UPDATE
                && schema.columns().stream().anyMatch(TableSchema.ColumnDefinition::foreignKey)) {
            throw new IllegalArgumentException("INSERT/DELETE recovery is not supported for tables with foreign keys");
        }
        switch (event.getEventType()) {
            case INSERT -> requireFull(event.getAfterData(), schema, "after", binlogTimeZone);
            case DELETE -> requireFull(event.getBeforeData(), schema, "before", binlogTimeZone);
            case UPDATE -> {
                if (event.getBeforeData() != null && event.getAfterData() != null
                        && !Objects.equals(event.getBeforeData().get(schema.primaryKey()), event.getAfterData().get(schema.primaryKey()))) {
                    throw new IllegalArgumentException("Primary key modification is not supported for recovery");
                }
                requireFull(event.getBeforeData(), schema, "before", binlogTimeZone);
                requireFull(event.getAfterData(), schema, "after", binlogTimeZone);
                for (var col : schema.columns()) {
                    if (col.foreignKey() && !valuesEqual(event.getBeforeData().get(col.name()), event.getAfterData().get(col.name()), col, binlogTimeZone)) {
                        throw new IllegalArgumentException("Foreign key column modification is not supported for recovery: " + col.name());
                    }
                }
            }
            default -> throw new IllegalArgumentException("Unsupported event type for recovery");
        }
    }

    private static void requireFull(Map<String, Object> data, TableSchema schema, String side, ZoneId binlogTimeZone) {
        Set<String> expected = new LinkedHashSet<>();
        schema.columns().forEach(col -> expected.add(col.name()));
        if (data == null || !data.keySet().equals(expected)) throw new IllegalArgumentException("Incomplete " + side + " image; FULL columns required");
        for (var col : schema.columns()) {
            Object value = data.get(col.name());
            if (value == null && !col.nullable()) throw new IllegalArgumentException("NULL in required column " + col.name());
            if (value != null && isJson(col)) {
                validateJsonNumbers(json(value), col.name());
            } else if (value instanceof String && isTimestamp(col)) {
                LocalDateTime time = dateTime(value);
                if (binlogTimeZone.getRules().getValidOffsets(time).size() != 1) {
                    throw new DateTimeException("TIMESTAMP has no unique UTC value in Canal time zone " + binlogTimeZone
                            + ": " + col.name() + " = " + value);
                }
            } else {
                formatValue(value, col, binlogTimeZone);
            }
        }
    }

    public static GeneratedSql generate(BinlogEvent event, TableSchema schema) {
        return generate(event, schema, ZoneId.systemDefault());
    }

    public static GeneratedSql generate(BinlogEvent event, TableSchema schema, ZoneId binlogTimeZone) {
        validateEvent(event, schema, binlogTimeZone);
        String table = quoteIdentifier(schema.database()) + "." + quoteIdentifier(schema.table());
        String sql;
        String reverse;
        switch (event.getEventType()) {
            case INSERT -> {
                reverse = "DELETE";
                sql = "DELETE FROM " + table + " WHERE " + guard(event.getAfterData(), schema, binlogTimeZone) + " LIMIT 1;";
            }
            case UPDATE -> {
                reverse = "UPDATE";
                List<String> sets = new ArrayList<>();
                List<String> preserve = new ArrayList<>();
                for (var col : schema.columns()) {
                    if (col.name().equals(schema.primaryKey())) continue;
                    Object before = event.getBeforeData().get(col.name());
                    String name = quoteIdentifier(col.name());
                    if (!valuesEqual(before, event.getAfterData().get(col.name()), col, binlogTimeZone)) {
                        sets.add(name + " = " + formatValue(before, col, binlogTimeZone));
                    } else if (schema.autoUpdatedColumns().contains(col.name())) {
                        preserve.add(name + " = " + name);
                    }
                }
                if (sets.isEmpty()) throw new IllegalArgumentException("No column changes detected in UPDATE event");
                sets.addAll(preserve);
                sql = "UPDATE " + table + " SET " + String.join(", ", sets) + " WHERE " + guard(event.getAfterData(), schema, binlogTimeZone) + " LIMIT 1;";
            }
            case DELETE -> {
                reverse = "INSERT";
                List<String> names = new ArrayList<>(), values = new ArrayList<>();
                for (var col : schema.columns()) {
                    names.add(quoteIdentifier(col.name())); values.add(formatValue(event.getBeforeData().get(col.name()), col, binlogTimeZone));
                }
                sql = "INSERT INTO " + table + " (" + String.join(", ", names) + ") VALUES (" + String.join(", ", values) + ");";
            }
            default -> throw new IllegalArgumentException("Unsupported event type");
        }
        String header = "-- Recovery preview only. Review constraints and current schema before execution.\n"
                + "-- Expected affected rows: 1; zero rows or a key conflict means recovery did not succeed.\n"
                + "-- Text literals use UTF-8 hex; guards compare text bytes, including case and spaces.\n";
        return new GeneratedSql(header + sql, reverse, "Reverse one row event; no transaction or business rollback guarantee");
    }

    private static String guard(Map<String, Object> data, TableSchema schema, ZoneId binlogTimeZone) {
        List<String> conditions = new ArrayList<>();
        for (var col : schema.columns()) {
            Object value = data.get(col.name());
            String name = quoteIdentifier(col.name());
            if (value == null) conditions.add(name + " IS NULL");
            else if (isJson(col)) conditions.add(jsonGuard(name, value));
            else if (isTimestamp(col)) conditions.add("UNIX_TIMESTAMP(" + name + ") = " + epochSeconds(timestamp(value, binlogTimeZone)).toPlainString());
            else if (isOrdinal(col) || isYear(col)) conditions.add("CAST(" + name + " AS UNSIGNED) = " + formatValue(value, col));
            else if (col.jdbcType() == Types.TIME) conditions.add(name + " = CAST(" + formatValue(value, col) + " AS TIME(6))");
            else if (isText(col)) conditions.add("CAST(CONVERT(" + name + " USING utf8mb4) AS BINARY) = X'" + hex(value.toString()) + "'");
            else conditions.add(name + " = " + formatValue(value, col, binlogTimeZone));
        }
        return String.join(" AND ", conditions);
    }

    public static String quoteIdentifier(String name) { return "`" + name.replace("`", "``") + "`"; }
    public static boolean isText(TableSchema.ColumnDefinition col) {
        return !isJson(col) && !isOrdinal(col) && !isYear(col)
                && Set.of(Types.VARCHAR, Types.CHAR, Types.LONGVARCHAR, Types.CLOB, Types.NCHAR, Types.NVARCHAR, Types.LONGNVARCHAR).contains(col.jdbcType());
    }
    public static String formatValue(Object value, TableSchema.ColumnDefinition col) {
        return formatValue(value, col, ZoneId.systemDefault());
    }
    public static String formatValue(Object value, TableSchema.ColumnDefinition col, ZoneId binlogTimeZone) {
        if (value == null) return "NULL";
        String str = value.toString();
        if (isJson(col)) {
            json(value);
            return jsonLiteral(value);
        }
        if (isOrdinal(col)) return ordinal(value, col).toString();
        if (isYear(col)) {
            int year = Integer.parseInt(str);
            if (year != 0 && (year < 1901 || year > 2155)) throw new IllegalArgumentException("YEAR outside column range: " + col.name());
            return Integer.toString(year);
        }
        return switch (col.jdbcType()) {
            case Types.BIT, Types.BOOLEAN -> bitValue(value, col).toString();
            case Types.FLOAT, Types.REAL, Types.DOUBLE -> {
                boolean single = singlePrecision(col);
                double number = single ? Float.parseFloat(str) : Double.parseDouble(str);
                if (!Double.isFinite(number)) throw new IllegalArgumentException("Non-finite floating-point value: " + col.name());
                yield "CAST(" + (single ? Float.toString((float) number) : Double.toString(number)) + " AS " + (single ? "FLOAT" : "DOUBLE") + ")";
            }
            case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY, Types.BLOB -> "X'" + HexFormat.of().formatHex(binary(value)) + "'";
            case Types.TIME -> {
                long micros = timeSeconds(value).movePointRight(6).longValueExact();
                long absolute = Math.abs(micros);
                yield String.format(Locale.ROOT, "'%s%02d:%02d:%02d.%06d'", micros < 0 ? "-" : "", absolute / 3600000000L,
                        absolute / 60000000 % 60, absolute / 1000000 % 60, absolute % 1000000);
            }
            case Types.BIGINT, Types.INTEGER, Types.SMALLINT, Types.TINYINT -> {
                BigInteger number = new BigInteger(str);
                int bits = switch (col.jdbcType()) { case Types.BIGINT -> 64; case Types.INTEGER -> 32; case Types.SMALLINT -> 16; default -> 8; };
                boolean unsigned = col.typeName().toUpperCase(Locale.ROOT).contains("UNSIGNED");
                BigInteger min = unsigned ? BigInteger.ZERO : BigInteger.ONE.shiftLeft(bits - 1).negate();
                BigInteger max = BigInteger.ONE.shiftLeft(unsigned ? bits : bits - 1).subtract(BigInteger.ONE);
                if (number.compareTo(min) < 0 || number.compareTo(max) > 0) throw new IllegalArgumentException("Integer outside column range: " + col.name());
                yield number.toString();
            }
            case Types.DECIMAL, Types.NUMERIC -> {
                BigDecimal number = new BigDecimal(str);
                if (col.size() > 0 && ((number.signum() != 0 && (long) number.precision() - number.scale() > col.size() - col.scale())
                        || number.stripTrailingZeros().scale() > col.scale())) throw new IllegalArgumentException("Decimal outside column precision: " + col.name());
                yield number.setScale(Math.min(Math.max(number.scale(), 0), col.scale()), java.math.RoundingMode.UNNECESSARY).toPlainString();
            }
            case Types.DATE -> {
                LocalDate date = LocalDate.parse(str);
                if (date.getYear() < 1000 || date.getYear() > 9999) throw new java.time.DateTimeException("DATE year must be 1000-9999");
                yield "'" + date + "'";
            }
            case Types.TIMESTAMP -> isTimestamp(col)
                    ? "'" + java.time.format.DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSSSSS").withZone(ZoneOffset.UTC)
                            .format(timestamp(value, binlogTimeZone)) + "+00:00'"
                    : "'" + dateTime(value).format(java.time.format.DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSSSSS")) + "'";
            case Types.VARCHAR, Types.CHAR, Types.LONGVARCHAR, Types.CLOB, Types.NCHAR, Types.NVARCHAR, Types.LONGNVARCHAR -> {
                if (col.size() > 0 && str.codePointCount(0, str.length()) > col.size()) throw new IllegalArgumentException("Text exceeds column size: " + col.name());
                yield "CONVERT(X'" + hex(str) + "' USING utf8mb4)";
            }
            default -> throw new IllegalArgumentException("Unsupported value type " + col.typeName());
        };
    }
    public static boolean valuesEqual(Object a, Object b, TableSchema.ColumnDefinition col) {
        return valuesEqual(a, b, col, ZoneId.systemDefault());
    }
    public static boolean valuesEqual(Object a, Object b, TableSchema.ColumnDefinition col, ZoneId binlogTimeZone) {
        if (a == null || b == null) return a == b;
        if (isJson(col)) return json(a).equals(json(b));
        if (isOrdinal(col)) return ordinal(a, col).equals(ordinal(b, col));
        if (isYear(col)) return new BigDecimal(a.toString()).compareTo(new BigDecimal(b.toString())) == 0;
        return switch (col.jdbcType()) {
            case Types.BIT, Types.BOOLEAN -> bitValue(a, col).equals(bitValue(b, col));
            case Types.FLOAT, Types.REAL, Types.DOUBLE -> singlePrecision(col)
                    ? Float.parseFloat(a.toString()) == Float.parseFloat(b.toString())
                    : Double.parseDouble(a.toString()) == Double.parseDouble(b.toString());
            case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY, Types.BLOB -> Arrays.equals(binary(a), binary(b));
            case Types.TIME -> timeSeconds(a).compareTo(timeSeconds(b)) == 0;
            case Types.BIGINT, Types.INTEGER, Types.SMALLINT, Types.TINYINT, Types.DECIMAL, Types.NUMERIC ->
                    new BigDecimal(a.toString()).compareTo(new BigDecimal(b.toString())) == 0;
            case Types.DATE -> LocalDate.parse(a.toString()).equals(LocalDate.parse(b.toString()));
            case Types.TIMESTAMP -> isTimestamp(col) ? timestamp(a, binlogTimeZone).equals(timestamp(b, binlogTimeZone)) : dateTime(a).equals(dateTime(b));
            default -> a.toString().equals(b.toString());
        };
    }
    public static boolean isTimestamp(TableSchema.ColumnDefinition col) {
        return col.typeName().toUpperCase(Locale.ROOT).startsWith("TIMESTAMP");
    }
    public static boolean isOrdinal(TableSchema.ColumnDefinition col) {
        return col.typeName().toUpperCase(Locale.ROOT).matches("^(ENUM|SET).*");
    }
    public static boolean isYear(TableSchema.ColumnDefinition col) {
        return col.typeName().toUpperCase(Locale.ROOT).startsWith("YEAR");
    }
    static boolean isJson(TableSchema.ColumnDefinition col) {
        return col.typeName().equalsIgnoreCase("JSON");
    }
    static String jsonGuard(String name, Object value) {
        return value == null ? name + " IS NULL"
                : "CAST(" + name + " AS BINARY) = CAST(" + jsonLiteral(value) + " AS BINARY)";
    }
    private static String jsonLiteral(Object value) {
        return "CAST(CONVERT(X'" + hex(value.toString()) + "' USING utf8mb4) AS JSON)";
    }
    private static boolean singlePrecision(TableSchema.ColumnDefinition col) {
        return col.typeName().toUpperCase(Locale.ROOT).startsWith("FLOAT") || col.jdbcType() == Types.REAL;
    }
    private static BigInteger ordinal(Object value, TableSchema.ColumnDefinition col) {
        BigInteger number = new BigInteger(value.toString());
        if (col.typeName().toUpperCase(Locale.ROOT).startsWith("SET") && number.signum() < 0) number = number.add(BigInteger.ONE.shiftLeft(64));
        if (number.signum() < 0) throw new IllegalArgumentException("Negative ENUM index: " + col.name());
        return number;
    }
    private static byte[] binary(Object value) {
        return value instanceof byte[] bytes ? bytes : value.toString().getBytes(StandardCharsets.ISO_8859_1);
    }
    private static JsonNode json(Object value) {
        try {
            JsonNode result = JSON.readTree(value.toString());
            if (result == null) throw new IllegalArgumentException("Empty JSON value");
            return result;
        } catch (JsonProcessingException error) {
            throw new IllegalArgumentException("Invalid JSON value", error);
        }
    }
    private static void validateJsonNumbers(JsonNode value, String column) {
        if (value.isIntegralNumber()) {
            BigInteger number = value.bigIntegerValue();
            if (number.bitLength() > (number.signum() < 0 ? 63 : 64)) {
                throw new IllegalArgumentException("JSON number cannot be restored without precision loss: " + column);
            }
        } else if (value.isFloatingPointNumber()) {
            double number = value.doubleValue();
            if (!Double.isFinite(number) || BigDecimal.valueOf(number).compareTo(value.decimalValue()) != 0) {
                throw new IllegalArgumentException("JSON number cannot be restored without precision loss: " + column);
            }
        } else {
            for (JsonNode child : value) validateJsonNumbers(child, column);
        }
    }
    private static BigDecimal timeSeconds(Object value) {
        BigDecimal seconds;
        if (value instanceof Number number) seconds = new BigDecimal(number.toString());
        else {
            var match = TIME.matcher(value.toString());
            if (!match.matches()) throw new IllegalArgumentException("Invalid TIME value: " + value);
            seconds = BigDecimal.valueOf(Long.parseLong(match.group(2)) * 3600 + Long.parseLong(match.group(3)) * 60 + Long.parseLong(match.group(4)));
            if (match.group(5) != null) seconds = seconds.add(new BigDecimal("0." + match.group(5)));
            if (!match.group(1).isEmpty()) seconds = seconds.negate();
        }
        if (seconds.abs().compareTo(new BigDecimal("3020399")) > 0) throw new IllegalArgumentException("TIME outside column range");
        return seconds;
    }
    private static BigInteger bitValue(Object value, TableSchema.ColumnDefinition col) {
        BigInteger number = value instanceof Boolean flag ? (flag ? BigInteger.ONE : BigInteger.ZERO)
                : value instanceof byte[] bytes ? new BigInteger(1, bytes) : new BigInteger(value.toString());
        int bits = col.jdbcType() == Types.BOOLEAN ? 1 : col.size();
        // Canal represents BIT(64) using a signed Java long; recover its unsigned bit pattern.
        if (bits == 64 && number.signum() < 0 && number.compareTo(BigInteger.ONE.shiftLeft(63).negate()) >= 0) {
            number = number.add(BigInteger.ONE.shiftLeft(64));
        }
        if (number.signum() < 0 || number.bitLength() > bits) throw new IllegalArgumentException("Bit value outside column range: " + col.name());
        return number;
    }
    private static Instant timestamp(Object value, ZoneId binlogTimeZone) {
        if (value instanceof java.sql.Timestamp time) return time.toInstant();
        if (value instanceof Number number) {
            BigDecimal seconds = new BigDecimal(number.toString());
            return Instant.ofEpochSecond(seconds.longValue(), seconds.remainder(BigDecimal.ONE).movePointRight(9).longValueExact());
        }
        return dateTime(value).atZone(binlogTimeZone).toInstant();
    }
    private static BigDecimal epochSeconds(Instant value) {
        return BigDecimal.valueOf(value.getEpochSecond()).add(BigDecimal.valueOf(value.getNano(), 9)).stripTrailingZeros();
    }
    private static LocalDateTime dateTime(Object value) {
        LocalDateTime time = value instanceof java.sql.Timestamp t ? t.toLocalDateTime() : LocalDateTime.parse(value.toString().replace(' ', 'T'));
        if (time.getYear() < 1000 || time.getYear() > 9999 || time.getNano() % 1000 != 0) throw new java.time.DateTimeException("DATETIME requires year 1000-9999 and at most six fractional digits");
        return time;
    }
    private static String hex(String value) { return HexFormat.of().formatHex(value.getBytes(StandardCharsets.UTF_8)); }
}

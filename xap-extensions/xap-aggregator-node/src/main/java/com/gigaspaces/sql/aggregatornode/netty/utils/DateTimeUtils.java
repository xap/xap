/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gigaspaces.sql.aggregatornode.netty.utils;

import com.gigaspaces.sql.aggregatornode.netty.exception.NonBreakingException;
import com.gigaspaces.sql.aggregatornode.netty.exception.ProtocolException;
import com.gigaspaces.sql.aggregatornode.netty.query.Session;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.*;
import java.util.Calendar;
import java.util.GregorianCalendar;

public class DateTimeUtils {
    private static final int MAX_NANOS_BEFORE_WRAP_ON_ROUND = 999999500;
    private static final long DATE_POSITIVE_INFINITY = 9223372036825200000L;
    private static final long DATE_NEGATIVE_INFINITY = -9223372036832400000L;
    private static final char[] ZEROS = {'0', '0', '0', '0', '0', '0', '0', '0', '0'};
    private static final char[][] NUMBERS;
    private static final java.time.Duration ONE_MICROSECOND = java.time.Duration.ofNanos(1000);
    private static final java.time.LocalTime MAX_LOCAL_TIME = java.time.LocalTime.MAX.minus(java.time.Duration.ofNanos(500));
    private static final java.time.LocalDate MIN_LOCAL_DATE = java.time.LocalDate.of(4713, 1, 1).with(java.time.temporal.ChronoField.ERA, java.time.chrono.IsoEra.BCE.getValue());
    private static final java.time.LocalDateTime MIN_LOCAL_DATETIME = MIN_LOCAL_DATE.atStartOfDay();
    private static final java.time.LocalDateTime MAX_LOCAL_DATETIME = java.time.LocalDateTime.MAX.minus(java.time.Duration.ofMillis(500));


    static {
        // The expected maximum value is 60 (seconds), so 64 is used "just in case"
        NUMBERS = new char[64][];
        for (int i = 0; i < NUMBERS.length; i++) {
            NUMBERS[i] = ((i < 10 ? "0" : "") + i).toCharArray();
        }
    }

    private final Calendar calendarWithTz = new GregorianCalendar();
    private final StringBuilder sbuf = new StringBuilder();
    private final Session session;

    public DateTimeUtils(Session session) {
        this.session = session;
    }

    public static String convertTimeZone(String tz) {
        if (tz == null || tz.length() < 4)
            return tz;
        switch (tz.substring(0, 4).toUpperCase()) {
            case "GMT+":
                return "GMT-" + tz.substring(4);
            case "GMT-":
                return "GMT+" + tz.substring(4);
            case "UTC+":
                return "UTC-" + tz.substring(4);
            case "UTC-":
                return "UTC+" + tz.substring(4);
            default:
                return tz;
        }
    }

    private Calendar setupCalendar() {
        Calendar tmp = calendarWithTz;
        tmp.setTimeZone(session.getTimeZone());
        return tmp;
    }

    private static boolean nanosExceed499(int nanos) {
        return nanos % 1000 > 499;
    }

    public String toString(Object x) throws ProtocolException {
        return toString(x, true);
    }

    public String toString(Object x, boolean withTimeZone) throws ProtocolException {
        if (x == null)
            return "NULL";

        if (x instanceof java.util.Date) {
            return toString((java.util.Date) x, withTimeZone);
        }

        if (!withTimeZone) {
            if (x instanceof LocalDate) {
                return toString((LocalDate) x);
            }
            if (x instanceof LocalTime) {
                return toString((LocalTime) x);
            }
            if (x instanceof LocalDateTime) {
                return toString((LocalDateTime) x);
            }
        } else {
            if (x instanceof OffsetDateTime) {
                return toString((OffsetDateTime) x);
            }
            if (x instanceof OffsetTime) {
                return toString((OffsetTime) x);
            }
        }

        throw new NonBreakingException(ErrorCodes.INVALID_PARAMETER_VALUE, "Unsupported object type: " + x.getClass());
    }

    public String toString(java.util.Date x) {
        return toString(x, true);
    }

    public String toString(java.util.Date x, boolean withTimeZone) {
        if (x instanceof Timestamp) {
            return toString((Timestamp) x, withTimeZone);
        }
        if (x instanceof Time) {
            return toString((Time) x, withTimeZone);
        }
        if (x instanceof Date) {
            return toString((Date) x, withTimeZone);
        }

        return toString(new Timestamp(x.getTime()), withTimeZone);
    }

    public String toString(Timestamp x) {
        return toString(x, true);
    }

    public String toString(Timestamp x, boolean withTimeZone) {
        if (x.getTime() == DATE_POSITIVE_INFINITY) {
            return "infinity";
        } else if (x.getTime() == DATE_NEGATIVE_INFINITY) {
            return "-infinity";
        }

        Calendar cal = setupCalendar();
        long timeMillis = x.getTime();

        // Round to microseconds
        int nanos = x.getNanos();
        if (nanos >= MAX_NANOS_BEFORE_WRAP_ON_ROUND) {
            nanos = 0;
            timeMillis++;
        } else if (nanosExceed499(nanos)) {
            // PostgreSQL does not support nanosecond resolution yet, and appendTime will just ignore
            // 0..999 part of the nanoseconds, however we subtract nanos % 1000 to make the value
            // a little bit saner for debugging reasons
            nanos += 1000 - nanos % 1000;
        }
        cal.setTimeInMillis(timeMillis);

        sbuf.setLength(0);

        appendDate(sbuf, cal);
        sbuf.append(' ');
        appendTime(sbuf, cal, nanos);
        if (withTimeZone) {
            appendTimeZone(sbuf, cal);
        }
        appendEra(sbuf, cal);

        return sbuf.toString();
    }

    public String toString(Date x) {
        return toString(x, true);
    }

    public String toString(Date x, boolean withTimeZone) {
        if (x.getTime() == DATE_POSITIVE_INFINITY) {
            return "infinity";
        } else if (x.getTime() == DATE_NEGATIVE_INFINITY) {
            return "-infinity";
        }

        Calendar cal = setupCalendar();
        cal.setTime(x);

        sbuf.setLength(0);

        appendDate(sbuf, cal);
        appendEra(sbuf, cal);
        if (withTimeZone) {
            sbuf.append(' ');
            appendTimeZone(sbuf, cal);
        }

        return sbuf.toString();
    }

    public String toString(Time x) {
        return toString(x, true);
    }

    public String toString(Time x, boolean withTimeZone) {
        Calendar cal = setupCalendar();
        cal.setTime(x);

        sbuf.setLength(0);

        appendTime(sbuf, cal, cal.get(Calendar.MILLISECOND) * 1000000);

        if (withTimeZone) {
            appendTimeZone(sbuf, cal);
        }

        return sbuf.toString();
    }

    public String toString(java.time.LocalDate localDate) {
        if (java.time.LocalDate.MAX.equals(localDate)) {
            return "infinity";
        } else if (localDate.isBefore(MIN_LOCAL_DATE)) {
            return "-infinity";
        }

        sbuf.setLength(0);

        appendDate(sbuf, localDate);
        appendEra(sbuf, localDate);

        return sbuf.toString();
    }

    public String toString(java.time.LocalTime localTime) {
        sbuf.setLength(0);

        if (localTime.isAfter(MAX_LOCAL_TIME)) {
            appendTime(sbuf, 24, 0, 0, 0);
        } else {
            int nano = localTime.getNano();
            if (nanosExceed499(nano)) {
                // Technically speaking this is not a proper rounding, however
                // it relies on the fact that appendTime just truncates 000..999 nanosecond part
                localTime = localTime.plus(ONE_MICROSECOND);
            }
            appendTime(sbuf, localTime);
        }

        return sbuf.toString();
    }

    public String toString(java.time.LocalDateTime localDateTime) {
        sbuf.setLength(0);

        if (localDateTime.isAfter(MAX_LOCAL_DATETIME)) {
            return "infinity";
        } else if (localDateTime.isBefore(MIN_LOCAL_DATETIME)) {
            return "-infinity";
        }

        int nano = localDateTime.getNano();
        if (nanosExceed499(nano)) {
            // Technically speaking this is not a proper rounding, however
            // it relies on the fact that appendTime just truncates 000..999 nanosecond part
            localDateTime = localDateTime.plus(ONE_MICROSECOND);
        }
        java.time.LocalDate localDate = localDateTime.toLocalDate();
        appendDate(sbuf, localDate);
        sbuf.append(' ');
        appendTime(sbuf, localDateTime.toLocalTime());
        appendEra(sbuf, localDate);

        return sbuf.toString();
    }

    public String toString(java.time.OffsetDateTime offsetDateTime) {
        sbuf.setLength(0);

        java.time.LocalDateTime localDateTime = offsetDateTime.toLocalDateTime();
        if (localDateTime.isAfter(MAX_LOCAL_DATETIME)) {
            return "infinity";
        } else if (localDateTime.isBefore(MIN_LOCAL_DATETIME)) {
            return "-infinity";
        }

        int nano = offsetDateTime.getNano();
        if (nanosExceed499(nano)) {
            // Technically speaking this is not a proper rounding, however
            // it relies on the fact that appendTime just truncates 000..999 nanosecond part
            offsetDateTime = offsetDateTime.plus(ONE_MICROSECOND);
        }
        java.time.LocalDate localDate = localDateTime.toLocalDate();
        appendDate(sbuf, localDate);
        sbuf.append(' ');
        appendTime(sbuf, localDateTime.toLocalTime());
        appendTimeZone(sbuf, offsetDateTime.getOffset());
        appendEra(sbuf, localDate);

        return sbuf.toString();
    }

    public String toString(java.time.ZonedDateTime zonedDateTime) {
        sbuf.setLength(0);

        java.time.LocalDateTime localDateTime = zonedDateTime.toLocalDateTime();
        if (localDateTime.isAfter(MAX_LOCAL_DATETIME)) {
            return "infinity";
        } else if (localDateTime.isBefore(MIN_LOCAL_DATETIME)) {
            return "-infinity";
        }

        int nano = zonedDateTime.getNano();
        if (nanosExceed499(nano)) {
            // Technically speaking this is not a proper rounding, however
            // it relies on the fact that appendTime just truncates 000..999 nanosecond part
            zonedDateTime = zonedDateTime.plus(ONE_MICROSECOND);
        }
        java.time.LocalDate localDate = localDateTime.toLocalDate();
        appendDate(sbuf, localDate);
        sbuf.append(' ');
        appendTime(sbuf, localDateTime.toLocalTime());
        appendTimeZone(sbuf, zonedDateTime.getOffset());
        appendEra(sbuf, localDate);

        return sbuf.toString();
    }

    public String toString(java.time.OffsetTime offsetTime) {
        sbuf.setLength(0);

        LocalTime localTime = offsetTime.toLocalTime();
        if (localTime.isAfter(MAX_LOCAL_TIME)) {
            appendTime(sbuf, 24, 0, 0, 0);
        } else {
            int nano = offsetTime.getNano();
            if (nanosExceed499(nano)) {
                // Technically speaking this is not a proper rounding, however
                // it relies on the fact that appendTime just truncates 000..999 nanosecond part
                offsetTime = offsetTime.plus(ONE_MICROSECOND);
            }
            appendTime(sbuf, localTime);
        }
        appendTimeZone(sbuf, offsetTime.getOffset());

        return sbuf.toString();
    }

    public String toString(java.time.Instant instant) {
        return toString(instant.atZone(session.getTimeZone().toZoneId()));
    }

    public String toString(Calendar calendar) {
        sbuf.setLength(0);

        appendDate(sbuf, calendar);
        sbuf.append(' ');
        appendTime(sbuf, calendar, calendar.get(Calendar.MILLISECOND) * 1000000);
        appendTimeZone(sbuf, calendar);
        appendEra(sbuf, calendar);

        return sbuf.toString();
    }

    private static void appendEra(StringBuilder sb, Calendar cal) {
        if (cal.get(Calendar.ERA) == GregorianCalendar.BC) {
            sb.append(" BC");
        }
    }

    private static void appendEra(StringBuilder sb, java.time.LocalDate localDate) {
        if (localDate.get(java.time.temporal.ChronoField.ERA) == java.time.chrono.IsoEra.BCE.getValue()) {
            sb.append(" BC");
        }
    }

    private static void appendDate(StringBuilder sb, Calendar cal) {
        int year = cal.get(Calendar.YEAR);
        int month = cal.get(Calendar.MONTH) + 1;
        int day = cal.get(Calendar.DAY_OF_MONTH);
        appendDate(sb, year, month, day);
    }

    private static void appendDate(StringBuilder sb, int year, int month, int day) {
        // always use at least four digits for the year so very
        // early years, like 2, don't get misinterpreted
        //
        int prevLength = sb.length();
        sb.append(year);
        int leadingZerosForYear = 4 - (sb.length() - prevLength);
        if (leadingZerosForYear > 0) {
            sb.insert(prevLength, ZEROS, 0, leadingZerosForYear);
        }

        sb.append('-');
        sb.append(NUMBERS[month]);
        sb.append('-');
        sb.append(NUMBERS[day]);
    }

    private static void appendDate(StringBuilder sb, java.time.LocalDate localDate) {
        int year = localDate.get(java.time.temporal.ChronoField.YEAR_OF_ERA);
        int month = localDate.getMonthValue();
        int day = localDate.getDayOfMonth();
        appendDate(sb, year, month, day);
    }

    private static void appendTime(StringBuilder sb, Calendar cal, int nanos) {
        int hours = cal.get(Calendar.HOUR_OF_DAY);
        int minutes = cal.get(Calendar.MINUTE);
        int seconds = cal.get(Calendar.SECOND);
        appendTime(sb, hours, minutes, seconds, nanos);
    }

    private static void appendTime(StringBuilder sb, int hours, int minutes, int seconds, int nanos) {
        sb.append(NUMBERS[hours]);

        sb.append(':');
        sb.append(NUMBERS[minutes]);

        sb.append(':');
        sb.append(NUMBERS[seconds]);

        // Add nanoseconds.
        // This won't work for server versions < 7.2 which only want
        // a two digit fractional second, but we don't need to support 7.1
        // anymore and getting the version number here is difficult.
        //
        if (nanos < 1000) {
            return;
        }
        sb.append('.');
        int len = sb.length();
        sb.append(nanos / 1000); // append microseconds
        int needZeros = 6 - (sb.length() - len);
        if (needZeros > 0) {
            sb.insert(len, ZEROS, 0, needZeros);
        }

        int end = sb.length() - 1;
        while (sb.charAt(end) == '0') {
            sb.deleteCharAt(end);
            end--;
        }
    }

    private static void appendTime(StringBuilder sb, java.time.LocalTime localTime) {
        int hours = localTime.getHour();
        int minutes = localTime.getMinute();
        int seconds = localTime.getSecond();
        int nanos = localTime.getNano();
        appendTime(sb, hours, minutes, seconds, nanos);
    }

    private static void appendTimeZone(StringBuilder sb, java.time.ZoneOffset offset) {
        int offsetSeconds = offset.getTotalSeconds();

        appendTimeZone(sb, offsetSeconds);
    }

    private static void appendTimeZone(StringBuilder sb, java.util.Calendar cal) {
        int offset = (cal.get(Calendar.ZONE_OFFSET) + cal.get(Calendar.DST_OFFSET)) / 1000;

        appendTimeZone(sb, offset);
    }

    private static void appendTimeZone(StringBuilder sb, int offset) {
        int absoff = Math.abs(offset);
        int hours = absoff / 60 / 60;
        int mins = (absoff - hours * 60 * 60) / 60;
        int secs = absoff - hours * 60 * 60 - mins * 60;

        sb.append((offset >= 0) ? "+" : "-");

        sb.append(NUMBERS[hours]);

        if (mins == 0 && secs == 0) {
            return;
        }
        sb.append(':');

        sb.append(NUMBERS[mins]);

        if (secs != 0) {
            sb.append(':');
            sb.append(NUMBERS[secs]);
        }
    }
}

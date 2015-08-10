package realtime;

import org.joda.time.DateTime;
import org.joda.time.MutableDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * Created by mike on 09.08.2015.
 */
public enum Granularity {

    SECOND {
        @Override
        public DateTime truncate(DateTime time) {
            final MutableDateTime mutableDateTime = time.toMutableDateTime();
            mutableDateTime.setMillisOfSecond(0);
            return mutableDateTime.toDateTime();
        }

        @Override
        public String format(DateTime time) {
            return time.toString(DateTimeFormat.forPattern("yyyy-MM-dd-HH-mm-ss"));
        }
    },

    HOUR {
        @Override
        public DateTime truncate(DateTime time) {
            final MutableDateTime mutableDateTime = time.toMutableDateTime();
            mutableDateTime.setMillisOfSecond(0);
            mutableDateTime.setSecondOfMinute(0);
            mutableDateTime.setMinuteOfHour(0);
            return mutableDateTime.toDateTime();
        }

        @Override
        public String format(DateTime time) {
            return time.toString(DateTimeFormat.forPattern("yyyy-MM-dd-HH"));
        }
    },

    DAY {
        @Override
        public DateTime truncate(DateTime time) {
            final MutableDateTime mutableDateTime = time.toMutableDateTime();
            mutableDateTime.setMillisOfDay(0);
            return mutableDateTime.toDateTime();
        }

        @Override
        public String format(DateTime time) {
            return time.toString(DateTimeFormat.forPattern("yyyy-MM-dd"));
        }
    };

    public abstract DateTime truncate(DateTime time);

    public abstract String format(DateTime time);

    public static String defaultFormat(DateTime time) {
        return time.toString(DateTimeFormat.forPattern("yyyy-MM-dd-HH-mm-ss"));
    }
}

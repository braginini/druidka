package realtime;

import org.joda.time.DateTime;
import org.joda.time.MutableDateTime;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * Created by mike on 09.08.2015.
 */
public enum Granularity {



    SECOND(new Period(0, 0, 1, 0)) {
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

    HOUR(new Period(1, 0, 0, 0)) {
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

    DAY(new Period(0, 0, 0, 1, 0, 0, 0, 0)) {
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

    public Period duration() {
        return duration;
    }

    public static String defaultFormat(DateTime time) {
        return time.toString(DateTimeFormat.forPattern("yyyy-MM-dd-HH-mm-ss"));
    }

    Period duration;

    Granularity(Period duration) {
        this.duration = duration;
    }
}

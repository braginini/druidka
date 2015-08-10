package realtime;

import org.joda.time.DateTime;
import org.joda.time.MutableDateTime;

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
    },

    DAY {
        @Override
        public DateTime truncate(DateTime time) {
            final MutableDateTime mutableDateTime = time.toMutableDateTime();
            mutableDateTime.setMillisOfDay(0);
            return mutableDateTime.toDateTime();
        }
    };

    public abstract DateTime truncate(DateTime time);
}

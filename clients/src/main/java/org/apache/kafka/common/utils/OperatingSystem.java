package org.apache.kafka.common.utils;

import java.util.Locale;

public final class OperatingSystem {

    private OperatingSystem() {
    }

    public static final String NAME;

    public static final boolean IS_WINDOWS;

    public static final boolean IS_ZOS;

    static {
        NAME = System.getProperty("os.name").toLowerCase(Locale.ROOT);
        IS_WINDOWS = NAME.startsWith("windows");
        IS_ZOS = NAME.startsWith("z/os");
    }
}

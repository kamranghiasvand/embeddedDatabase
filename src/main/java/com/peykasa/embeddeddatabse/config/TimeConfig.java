package com.peykasa.embeddeddatabse.config;

import java.util.ArrayList;
import java.util.List;

/**
 * @author kamran
 */
public class TimeConfig {

    private long millis = 0;
    private String text = "";

    public TimeConfig(String text) {
        this.text = text;
        millis = toMillis(text);
    }

    public static List<Long> millisList(TimeConfig[] times) {
        if (times == null) {
            return null;
        }
        List<Long> r = new ArrayList<>();
        for (TimeConfig time : times) {
            r.add(time.millis());
        }
        return r;
    }

    private long toMillis(String time) {
        String t = validate(time);
        int d = fetch(t, "d");
        int h = fetch(t, "h");
        int m = fetch(t, "m([^s]|$)");
        int s = fetch(t, "s");
        int ms = fetch(t, "ms");
        return (d * 24 * 60 * 60 * 1000L) + (h * 60 * 60 * 1000L) + (m * 60 * 1000) + (s * 1000L) + ms;
    }

    private int fetch(String t, String unit) {
        String s = t.replaceAll("(^|.*\\s*)(\\d+)" + unit + ".*", "$2");
        if (!s.matches("\\d+"))
            return 0;
        return Integer.parseInt(s);
    }

    private String validate(String t) {
        String time = t + "";
        if (t == null || time.isEmpty() || time.trim().isEmpty() || !time.matches(".*\\d+.*")) {
            exception(t, null);
        }
        time = time.trim().toLowerCase();
        if (count("\\d+d", time) > 1 || count("\\d+h", time) > 1 || count("\\d+m([^s]|$)", time) > 1 || count("\\d+s", time) > 1 || count("\\d+ms", time) > 1) {
            exception(t, null);
        }
        if (time.contains("mss") || time.contains("sms") || time.contains("mms") || time.contains("msm")) {
            exception(t, null);
        }
        for (String s : time.split("[a-z]+")) {
            try {
                Integer.parseInt(s.trim());
            } catch (Exception e) {
                exception(t, e);
            }
        }
        if (time.matches("\\d+")) {
            exception(t, null);
        }
        if (!time.matches("(\\s*\\d*[dhms])+")) {
            exception(t, null);
        }
        return time;
    }

    private void exception(String time, Throwable e) {
        if (e == null)
            throw new IllegalArgumentException("invalid time string : " + time + ", valid-example=1h 1d 1m 1s 1ms");
        else
            throw new IllegalArgumentException("invalid time string : " + time + ", valid-example=1h 1d 1m 1s 1ms", e);
    }

    public long millis() {
        return millis;
    }

    private int count(String regex, String input) {
        return input.split(regex, -1).length - 1;
    }

    @Override
    public String toString() {
        return "TimeConfig{" +
                "text='" + text + '\'' +
                ", millis=" + millis +
                '}';
    }
}


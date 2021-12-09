package dev.bischoff.es.enrich.testset;

import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.time.LocalDateTime;

public class Stats {
    public double top10;
    public double top100;
    public double top1000;
    public double quartile;
    public double half;
    public long picks;
    public LocalDateTime endTime;
    public LocalDateTime startTime;
    public long csvSize;
    public long jsonSize;
    public long jsonBzSize;
    public long userActivitySize;
    public long userActivityBzSize;
    public double percentUnknownSites;


    public String asString() {
        return  String.format("top10            : %12.0f %3.1f%% \n", top10, (top10/picks)*100.0) +
                String.format("top100           : %12.0f %3.1f%% \n", top100, (top100/picks)*100.0) +
                String.format("top1000          : %12.0f %3.1f%% \n", top1000, (top1000/picks)*100.0) +
                String.format("quartile         : %12.0f %3.1f%% \n", quartile, (quartile/picks)*100.0) +
                String.format("half             : %12.0f %3.1f%% \n", half, (half/picks)*100.0) +
                String.format("Time range       : %s - %s \n", startTime, endTime) +
                String.format("Unknown Sites    : %3.1f%% \n", percentUnknownSites*100.0) +
                String.format("user_activity    : %15s \n", humanReadableByteCountSI(userActivitySize)) +
                String.format("user_activity.bz : %15s \n", humanReadableByteCountSI(userActivityBzSize)) +
                String.format("csv              : %15s \n", humanReadableByteCountSI(csvSize)) +
                String.format("json             : %15s \n", humanReadableByteCountSI(jsonSize)) +
                String.format("json             : %15s \n", humanReadableByteCountSI(jsonBzSize));
    }

    public void registerPick(int pick, long options) {
        picks++;
        if (pick < 10) {
            top10++;
        }
        if (pick < 100) {
            top100++;
        }
        if (pick < 1000) {
            top1000++;
        }
        if (pick < options / 4) {
            quartile++;
        }
        if (pick < options / 2) {
            half++;
        }
    }

    private static String humanReadableByteCountSI(long bytes) {
        if (-1000 < bytes && bytes < 1000) {
            return bytes + " B";
        }
        CharacterIterator ci = new StringCharacterIterator("kMGTPE");
        while (bytes <= -999_950 || bytes >= 999_950) {
            bytes /= 1000;
            ci.next();
        }
        return String.format("%.1f %cB", bytes / 1000.0, ci.current());
    }
}

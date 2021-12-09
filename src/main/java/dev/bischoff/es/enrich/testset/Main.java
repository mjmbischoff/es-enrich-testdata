package dev.bischoff.es.enrich.testset;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.javafaker.Faker;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;

import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.DayOfWeek;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Downloads from https://www.domcop.com the openpagerank data set
 * Converts the dataset to jsonnd for easy ingesting and allow using it as corpus for rally
 * Generates a fictional DNS access log in jsonnd
 * - This uses the open pagerank list and favors picking higher ranked sites randomly
 * - Also mixes in sites for which we have no pagerank
 * - for the timestamp we use a poisson to introduce further randomness and adjust the mean to the time of
 *   day to make our synthetic data a little more convincing
 *
 * uses https://www.domcop.com/openpagerank/what-is-openpagerank
 */
public class Main {

    public static void main(String[] args) throws IOException {
        new Main().run();
    }

    private final CSVParser parser = new CSVParserBuilder().build();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Faker faker = new Faker();
    private final Stats stats = new Stats();
    private final Random random = ThreadLocalRandom.current();
    private final Path outputDir = Path.of(System.getProperty("user.dir"), "output");

    public void run() throws IOException {
        Files.createDirectories(outputDir);

        URL url = new URL("https://www.domcop.com/files/top/top10milliondomains.csv.zip");
        Path csv = extractZip(url);
        Path json = writeCsvAsJson(csv);

        List<PageRankEntry> options;
        try (BufferedReader reader = Files.newBufferedReader(csv)) {
            options = reader
                    .lines()
                    .skip(1)
                    .map(this::parseLine)
                    .map(items -> new PageRankEntry(items[0], items[1], items[2]))
                    .collect(Collectors.toList());
        }

        int totalCustomers = 100_000;
        List<String> clientIps = new ArrayList<>();
        for(int i=0;i<totalCustomers;i++) {
            String clientIp = faker.internet().ipV4Address();
            clientIps.add(clientIp);
        }

        int iterations=50_000_000;
        double percentUnknownSites = 0.1;
        stats.percentUnknownSites = percentUnknownSites;
        LocalDateTime eventTime = LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS);
        stats.startTime = eventTime;
        double defaultMean = 20;

        Path userActivity = outputDir.resolve("user_activity.json");
        if(!Files.exists(userActivity)) {
            try (BufferedWriter out = Files.newBufferedWriter(userActivity, StandardCharsets.UTF_8)) {
                for (int i = 0; i < iterations; i++) {

                    String clientIp = computeClientIp(clientIps);
                    String domain = computeDomain(percentUnknownSites, options);
                    eventTime = computeNextTime(eventTime, defaultMean);
                    out.append("{ \"@timestamp\": \"")
                        .append(String.valueOf(eventTime))
                        .append("\", \"clientIp\": \"")
                        .append(clientIp)
                        .append("\", \"domain\": \"")
                        .append(domain)
                        .append("\" }\n");
                }
            }
        }
        stats.endTime = eventTime;
        stats.userActivitySize = Files.size(userActivity);


        Path jsonBz = outputDir.resolve("top10milliondomains.json.bz2");
        if(!Files.exists(jsonBz)) {
            try (OutputStream out = new BZip2CompressorOutputStream(new BufferedOutputStream(Files.newOutputStream(jsonBz)), 1)) {
                Files.copy(json, out);
            }
            stats.jsonBzSize = Files.size(jsonBz);
        }
        Path userActivityBz = outputDir.resolve("user_activity.json.bz2");
        if(!Files.exists(userActivityBz)) {
            try (OutputStream out = new BZip2CompressorOutputStream(new BufferedOutputStream(Files.newOutputStream(userActivityBz)), 1)) {
                Files.copy(userActivity, out);
            }
            stats.userActivityBzSize = Files.size(userActivityBz);
        }

        Path statsFile = outputDir.resolve("stats");
        Files.writeString(statsFile, stats.asString());
    }

    private LocalDateTime computeNextTime(LocalDateTime eventTime, double defaultMean) {
        boolean isWeekend = eventTime.getDayOfWeek().equals(DayOfWeek.SATURDAY) || eventTime.getDayOfWeek().equals(DayOfWeek.SUNDAY);
        double mean = makeUpMean(eventTime.getHour(), isWeekend, defaultMean);
        return eventTime.plus(poissonRandomInterarrivalDelay(mean), ChronoUnit.MILLIS);
    }

    private double makeUpMean(int hour, boolean isWeekend, double defaultMean) {
        if(isWeekend) {
            return switch (hour) {
                case 17, 18, 19, 20, 21, 22, 23, 24, 0, 1, 2, 3, 4, 5, 6, 7, 8 -> defaultMean * 0.15;
                default -> defaultMean * 0.25;
            };

        }
        return switch (hour) {
            case 19, 20, 21, 22, 23, 24, 0, 1, 2, 3, 4, 5, 6, 7 -> defaultMean * 0.15;
            case 12 -> defaultMean * 0.8; // lunch
            case 11, 13 -> defaultMean * 0.95; // early / late lunch
            case 8, 18 -> defaultMean * 0.35; // working late / eary
            default -> defaultMean * 1.2;
        };
    }

    private Path writeCsvAsJson(Path csv) throws IOException {
        Path jsonTop10MDomains = outputDir.resolve("top10milliondomains.json");
        if(!Files.exists(jsonTop10MDomains)) {
            try (BufferedWriter out = Files.newBufferedWriter(jsonTop10MDomains, StandardCharsets.UTF_8)) {
                try (BufferedReader reader = Files.newBufferedReader(csv)) {
                    reader
                            .lines()
                            .skip(1)
                            .map(this::parseLine)
                            .map(items -> new PageRankEntry(items[0], items[1], items[2]))
                            .map(this::toJson)
                            .forEach(json -> consume(out, json));
                }
            }
        }
        stats.jsonSize = Files.size(jsonTop10MDomains);
        return jsonTop10MDomains;
    }

    public String computeClientIp(List<String> clientIps) {
        return clientIps.get(random.nextInt(clientIps.size()));
    }

    public String computeDomain(double percentUnknownSites, List<PageRankEntry> options) {
        if(random.nextDouble() < percentUnknownSites) {
            return faker.internet().domainName()+"."+faker.address().countryCode();
        } else {
            int pick = getNext(random, options.size());
            stats.registerPick(pick, options.size());
            return options.get(pick).getDomain();
        }
    }

    public void consume(BufferedWriter out, String json) {
        try {
            out.append(json);
            out.append("\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String toJson(PageRankEntry pageRankEntry) {
        try {
            return objectMapper.writeValueAsString(pageRankEntry);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public String[] parseLine(String line) {
        try {
            return parser.parseLine(line);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Path extractZip(URL zipFile) throws IOException {
        try(ZipInputStream stream = new ZipInputStream(zipFile.openStream())) {
            ZipEntry entry;
            while((entry = stream.getNextEntry()) != null) {
                if(entry.getName().endsWith("csv")) {
                    Path targetPath = outputDir.resolve(entry.getName());
                    if(!Files.exists(targetPath)) {
                        Files.copy(stream, targetPath);
                    }
                    stats.csvSize = Files.size(targetPath);
                    return targetPath;
                }
            }
        }
        return null;
    }

    public long poissonRandomInterarrivalDelay(double L) {
        return (long) (Math.log(1.0-random.nextDouble())/-L);
    }

    public int getNext(Random random, double lambda) {
        double randomNumber = random.nextDouble();
        return (int) Math.floor(lambda-((1.0 - (randomNumber * randomNumber * randomNumber)) * lambda));
    }
}

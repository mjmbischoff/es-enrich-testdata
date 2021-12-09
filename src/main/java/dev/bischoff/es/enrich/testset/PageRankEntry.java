package dev.bischoff.es.enrich.testset;

public class PageRankEntry {
    private final long rank;
    private final String domain;
    private final double openPageRank;

    /**
     * "Rank","Domain","Open Page Rank"
     * @param rank
     * @param domain
     * @param openPageRank
     */
    public PageRankEntry(String rank, String domain, String openPageRank) {
        this.rank = Long.parseLong(rank);
        this.domain = domain;
        this.openPageRank = Double.parseDouble(openPageRank);
    }

    public long getRank() {
        return rank;
    }

    public String getDomain() {
        return domain;
    }

    public double getOpenPageRank() {
        return openPageRank;
    }
}

package statistics.service;

import btree.service.BTreeService;
import data_file.service.DataService;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.ToString;
import statistics.entity.Statistics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collector;
import java.util.stream.Collectors;

@Builder
@ToString
@AllArgsConstructor
public class StatisticsService {

    private DataService dataService;
    private BTreeService bTreeService;

    /**
     * In theory, TapeService could manage many tapes for different databases, so every StatisticsService should know
     * of which tapes its database consists.
     */
    private UUID dataTapeID;

    /**
     * In theory, TapeService could manage many tapes for different databases, so every StatisticsService should know
     * of which tapes its database consists.
     */
    private UUID indexTapeID;

    /**
     * Statistics for each operation on the index file.
     */
    private HashMap<Integer, Statistics> indexStatistics;

    /**
     * Statistics for each operation on the data file.
     */
    private HashMap<Integer, Statistics> dataFileStatistics;


    public Statistics getCurrentState(UUID tapeID)
    {
        if(tapeID == indexTapeID)
            return Statistics.builder()
                    .operation(0)
                    .type("STATE")
                    .merges(bTreeService.getMerges())
                    .splits(bTreeService.getSplits())
                    .compensations(bTreeService.getCompensations())
                    .tapeReads(bTreeService.getReads(tapeID))
                    .tapeWrites(bTreeService.getWrites(tapeID))
                    .build();

        if(tapeID == dataTapeID)
            return Statistics.builder()
                    .operation(0)
                    .type("STATE")
                    .merges(0)
                    .splits(0)
                    .compensations(0)
                    .tapeReads(dataService.getReads(tapeID))
                    .tapeWrites(dataService.getWrites(tapeID))
                    .build();

        throw new IllegalArgumentException("Provided tape ID wasn't equal to any of index tape ID or data tape ID for this database." +
                " This database consists from only these 2 tapes, so it doesn't have to know anything correctly about other tapes.");
    }

    public void setOperationStats(UUID tapeID, int number, Statistics stats)
    {
        if(tapeID == indexTapeID) {
            this.indexStatistics.put(number, stats);
            return;
        }

        if(tapeID == dataTapeID) {
            this.dataFileStatistics.put(number, stats);
            return;
        }

        throw new IllegalArgumentException("Provided tape ID wasn't equal to any of index tape ID or data tape ID for this database." +
                " Operations statistics for tapes other than these 2 are not being stored.");
    }

    public Statistics getOperationStats(UUID tapeID, int number)
    {
        if(tapeID == indexTapeID)
            return this.indexStatistics.get(number);

        if(tapeID == dataTapeID)
            return this.dataFileStatistics.get(number);

        throw new IllegalArgumentException("Provided tape ID wasn't equal to any of index tape ID or data tape ID for this database." +
                " Operations statistics for tapes other than these 2 are not being stored.");
    }

    public Statistics getSummedStats(UUID tapeID, String type)
    {
        if(tapeID == indexTapeID)
            return this.indexStatistics.values().stream()
                    .filter(stat -> type.equals(stat.getType()))
                    .reduce(Statistics.builder().build(), this::addStats);
        
        if(tapeID == dataTapeID)
            return this.dataFileStatistics.values().stream()
                    .filter(stat -> type.equals(stat.getType()))
                    .reduce(Statistics.builder().build(), this::addStats);

        throw new IllegalArgumentException("Provided tape ID wasn't equal to any of index tape ID or data tape ID for this database." +
                " Operations statistics for tapes other than these 2 are not being stored.");
    }

    public List<Statistics> getAllSummedStats(UUID tapeID)
    {
        List<String> types = this.getStatsTypes(tapeID);
        List<Statistics> stats = new ArrayList<>();
        for(String type : types) {
            Statistics singleTypeStats = this.getSummedStats(tapeID, type);
            singleTypeStats.setOperation(this.getOperations(tapeID, type));
            stats.add(singleTypeStats);
        }

        return stats;
    }

    public List<String> getStatsTypes(UUID tapeID)
    {
        if(tapeID == indexTapeID)
            return this.indexStatistics.values().stream()
                    .map(Statistics::getType)
                    .distinct()
                    .collect(Collectors.toList());

        if(tapeID == dataTapeID)
            return this.dataFileStatistics.values().stream()
                    .map(Statistics::getType)
                    .distinct()
                    .collect(Collectors.toList());

        throw new IllegalArgumentException("Provided tape ID wasn't equal to any of index tape ID or data tape ID for this database." +
                " Operations statistics for tapes other than these 2 are not being stored.");
    }

    public int getOperations(UUID tapeID)
    {
        if(tapeID == indexTapeID)
            return this.indexStatistics.size();

        if(tapeID == dataTapeID)
            return this.dataFileStatistics.size();

        throw new IllegalArgumentException("Provided tape ID wasn't equal to any of index tape ID or data tape ID for this database." +
                " Operations statistics for tapes other than these 2 are not being stored.");
    }
    public int getOperations(UUID tapeID, String type)
    {
        if(tapeID == indexTapeID)
            return (int) this.indexStatistics.values().stream()
                    .filter(stat -> type.equals(stat.getType()))
                    .count();

        if(tapeID == dataTapeID)
            return (int) this.dataFileStatistics.values().stream()
                    .filter(stat -> type.equals(stat.getType()))
                    .count();

        throw new IllegalArgumentException("Provided tape ID wasn't equal to any of index tape ID or data tape ID for this database." +
                " Operations statistics for tapes other than these 2 are not being stored.");
    }

    public Statistics addStats(Statistics left, Statistics right)
    {
        return Statistics.builder()
                .operation(right.getOperation())
                .type(right.getType())
                .merges(left.getMerges() + right.getMerges())
                .splits(left.getSplits() + right.getSplits())
                .compensations(left.getCompensations() + right.getCompensations())
                .tapeReads(left.getTapeReads() + right.getTapeReads())
                .tapeWrites(left.getTapeWrites() + right.getTapeWrites())
                .build();
    }

    public Statistics subtractStats(Statistics left, Statistics right)
    {
        return Statistics.builder()
                .operation(left.getOperation())
                .type(left.getType())
                .merges(left.getMerges() - right.getMerges())
                .splits(left.getSplits() - right.getSplits())
                .compensations(left.getCompensations() - right.getCompensations())
                .tapeReads(left.getTapeReads() - right.getTapeReads())
                .tapeWrites(left.getTapeWrites() - right.getTapeWrites())
                .build();
    }
    public int getTapePages(UUID tapeID)
    {
        if(tapeID == indexTapeID)
            return bTreeService.getTapePages(tapeID);

        if(tapeID == dataTapeID)
            return dataService.getTapePages(tapeID);

        throw new IllegalArgumentException("Provided tape ID wasn't equal to any of index tape ID or data tape ID for this database." +
                " This database consists from only these 2 tapes, so it doesn't have to know anything correctly about other tapes.");
    }

    public int getTapeFreePages(UUID tapeID)
    {
        if(tapeID == indexTapeID)
            return this.bTreeService.getTapeFreePages(tapeID);

        if(tapeID == dataTapeID)
            return this.dataService.getTapeFreePages(tapeID);

        throw new IllegalArgumentException("Provided tape ID wasn't equal to any of index tape ID or data tape ID for this database." +
                " This database consists from only these 2 tapes, so it doesn't have to know anything correctly about other tapes.");
    }
}

package statistics.service;

import btree.service.BTreeService;
import data_file.service.DataService;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.ToString;
import statistics.entity.Statistics;

import java.util.HashMap;
import java.util.UUID;

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

        return null;
    }

    public Statistics calculateOperationStats(Statistics before, Statistics after)
    {
        return Statistics.builder()
                .operation(after.getOperation())
                .type(after.getType())
                .merges(after.getMerges() - before.getMerges())
                .splits(after.getSplits() - before.getSplits())
                .compensations(after.getCompensations() - before.getCompensations())
                .tapeReads(after.getTapeReads() - before.getTapeReads())
                .tapeWrites(after.getTapeWrites() - before.getTapeWrites())
                .build();
    }

    public void setOperationStats(UUID tapeID, Statistics stats, int number)
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

    public int getOperations(UUID tapeID)
    {
        if(tapeID == indexTapeID)
            return this.indexStatistics.size();

        if(tapeID == dataTapeID)
            return this.dataFileStatistics.size();

        throw new IllegalArgumentException("Provided tape ID wasn't equal to any of index tape ID or data tape ID for this database." +
                " Operations statistics for tapes other than these 2 are not being stored.");
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

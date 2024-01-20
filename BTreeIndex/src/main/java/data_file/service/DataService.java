package data_file.service;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.ToString;
import record.entity.Record;
import record.service.RecordService;

import java.security.InvalidAlgorithmParameterException;
import java.util.Comparator;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

@Builder
@ToString
@AllArgsConstructor
public class DataService {

    RecordService recordService;

    /**
     *
     * @param tapeID
     * @param record
     * @return Returns page number, on which the record was successfully stored
     * @throws InvalidAlgorithmParameterException
     */
    public int createRecord(UUID tapeID, Record record) throws InvalidAlgorithmParameterException {
        int page = this.findSpaceForRecord(tapeID, record);
        if(page == -1) {
            page = recordService.getTapePages(tapeID);
            this.assureBufferForPage(tapeID, page);
            recordService.addNextPage(tapeID);
            if(page != this.findSpaceForRecord(tapeID, record))
                throw new IllegalStateException("New page should have been added to tape buffers," +
                        " but there is still being reported no space for the record. The record might be too big" +
                        " or something different went wrong.");
        }
        else
            this.assureBufferForPage(tapeID, page);

        recordService.createRecord(tapeID, page, record);
        recordService.setFreeSpaceOnPage(tapeID, page, recordService.getFreeSpaceOnPage(tapeID, page) - record.getSize());
        return page;
    }

    public Record findRecord(UUID tapeID, int page, long key)
    {
        if(page < 0 || page >= recordService.getTapePages(tapeID))
            throw new IllegalStateException("Page requested to find a record on it doesn't exist.");

        this.assureBufferForPage(tapeID, page);
        return recordService.readRecord(tapeID, page, key);
    }

    public void updateRecord(UUID tapeID, int page, Record record) throws InvalidAlgorithmParameterException {
        if(page < 0 || page >= recordService.getTapePages(tapeID))
            throw new IllegalStateException("Page requested to update a record on it doesn't exist.");

        this.assureBufferForPage(tapeID, page);
        recordService.updateRecord(tapeID, page, record);
        // updating record doesn't change its free space amount, since in this project records are of constant size
    }

    public void deleteRecord(UUID tapeID, int page, long key) throws InvalidAlgorithmParameterException {
        if(page < 0 || page >= recordService.getTapePages(tapeID))
            throw new IllegalStateException("Page requested to delete a record on it doesn't exist.");

        this.assureBufferForPage(tapeID, page);
        recordService.removeRecord(tapeID, page, key);
        recordService.setFreeSpaceOnPage(tapeID, page, recordService.getFreeSpaceOnPage(tapeID, page)
                + Record.builder().build().getSize());
    }

    public int getReads(UUID tapeID)
    {
        return recordService.getReads(tapeID);
    }

    public int getWrites(UUID tapeID)
    {
        return recordService.getWrites(tapeID);
    }

    public int getTapePages(UUID tapeID)
    {
        return recordService.getTapePages(tapeID);
    }

    public int getTapeFreePages(UUID tapeID)
    {
        return recordService.getTapeFreePages(tapeID);
    }

    /**
     * Searches array of amounts of free space on each page of tape. Each tape has that array, and it needs to be updated.
     * @param tapeID
     * @param record
     * @return Returns page nr with enough space to write the record on it, or -1, if all pages
     * are full (or there is none yet) and new page needs to be added to tape.
     */
    private int findSpaceForRecord(UUID tapeID, Record record)
    {
        if(record == null)
            throw new IllegalStateException("Record provided to find space for was null.");

        int page = 0;
        while(page < recordService.getTapePages(tapeID))
        {
            if(record.getSize() <= recordService.getFreeSpaceOnPage(tapeID, page))
                return page;

            page++;
        }
        return -1;
    }

    /**
     * Method of choosing which buffer should be freed. Takes into account page number, which will be loaded next.
     * There will always be chosen some page to free (the most optimal from currently buffered), unless there are no pages buffered.
     * @param tapeID
     * @param pageToLoad
     * @return Number of the buffered page, which lies in the tape furthest from the next page to load.
     */
    private int choosePageToFree(UUID tapeID, int pageToLoad)
    {
        Set<Integer> bufferedPages = recordService.getBufferedPages(tapeID);
        if(bufferedPages.isEmpty())
            throw new IllegalStateException("There was no buffered pages for this tape. There was no page to choose to be freed.");

        if(bufferedPages.contains(null))
            throw new IllegalStateException("Something went wrong in maintaining buffered pages numbers set" +
                    " - it contained a null value.");

        Optional<Integer> furthestPage = bufferedPages.stream()
                .max(Comparator.comparingInt(page -> Math.abs(page - pageToLoad)));

        return furthestPage.get();
    }

    /**
     * It assures that there is a space to read a new page, if it isn't already loaded.
     * @param tapeID
     * @param page The page may not exist (it may be one that is being created just now), it just frees a buffer if needed.
     *             Page parameter is for algorithm of choosing which buffer to free, to take it into account.
     */
    private void assureBufferForPage(UUID tapeID, int page)
    {
        if(!recordService.getBufferedPages(tapeID).contains(page))
        {
            if(recordService.isTapeMaxBuffers(tapeID)) {
                int pageToFree = this.choosePageToFree(tapeID, page);
                recordService.freeBufferedBlock(tapeID, pageToFree);
            }
        }
    }
}

package database.service;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.ToString;
import record.converter.RecordConverter;
import record.entity.Record;
import tape.service.TapeService;

import java.nio.ByteBuffer;
import java.util.*;

@Builder
@ToString
@AllArgsConstructor
public class DatabaseRawReader {

    private TapeService tapeService;

    private RecordConverter recordConverter;

    private UUID dataTapeID;

    private UUID indexTapeID;

    public void readDataPage(int page)
    {
        if(page < 0)
            throw new NoSuchElementException("Requested page was below 0 - that page doesn't exist.");

        this.assureBufferForPage(dataTapeID, page);
        byte[] buffer = tapeService.readPage(dataTapeID, page);
        System.out.println("---------------------------------- Page nr "+page+" ----------------------------------");
        int consumed = 0;
        while(consumed < buffer.length) {
            Record record = recordConverter.bytesToRecord(buffer, consumed);
            if(record.getKey() == 0) {
                for(int i=0; i< record.getSize(); i++)
                    System.out.format("0x%02x ", buffer[consumed+i]);
                System.out.println();
            }
            else
                System.out.println(record);
            consumed += record.getSize();
            if(consumed + record.getSize() > buffer.length)
                break;
        }
        if(consumed < buffer.length) {
            for (int i = 0; i < buffer.length - consumed; i++)
                System.out.format("0x%02x ", buffer[consumed + i]);
            System.out.println();
        }
        System.out.println("------------------------------- End of page nr "+page+" ------------------------------");
    }

    public void readData()
    {
        int page = 0;
        while(page < tapeService.getPages(dataTapeID)) {
            this.readDataPage(page);
            page++;
        }
    }

    private int choosePageToFree(UUID tapeID, int pageToLoad)
    {
        Set<Integer> bufferedPages = tapeService.getBufferedPages(tapeID);
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
        if(!tapeService.getBufferedPages(tapeID).contains(page))
        {
            if(tapeService.isMaxBuffers(tapeID)) {
                int pageToFree = this.choosePageToFree(tapeID, page);
                tapeService.freeBufferedBlock(tapeID, pageToFree);
            }
        }
    }

}

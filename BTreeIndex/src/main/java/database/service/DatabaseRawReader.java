package database.service;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.ToString;
import record.converter.RecordConverter;
import record.entity.Record;
import tape.service.TapeService;

import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import java.util.UUID;

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

}

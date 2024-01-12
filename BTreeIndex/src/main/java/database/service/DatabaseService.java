package database.service;

import btree.service.BTreeService;
import data_file.service.DataService;
import entry.entity.Entry;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.ToString;
import record.converter.RecordConverter;
import record.entity.Record;

import java.security.InvalidAlgorithmParameterException;
import java.util.UUID;

@Builder
@ToString
@AllArgsConstructor
public class DatabaseService {

    private DataService dataService;

    private BTreeService bTreeService;

    private RecordConverter recordConverter;

    /**
     * In theory, TapeService could manage many tapes for different databases, so every DatabaseService should know
     * of which tapes its database consists.
     */
    private UUID dataTapeID;

    /**
     * In theory, TapeService could manage many tapes for different databases, so every DatabaseService should know
     * of which tapes its database consists.
     */
    private UUID indexTapeID;

    // TODO add read and write stats measurer before and after each operation
    public void create(String command) throws InvalidAlgorithmParameterException {
        if(command == null)
            throw new IllegalArgumentException("Command data was null. Creating record aborted.");

        if(command.split(" ").length < 4)
            throw new IllegalArgumentException("Bad command syntax. Command for creating a record should look something like:\n" +
                    "C 100 25 10\n, where C - Create, 100 - example record key, 25 - example mass, 10 - example speed.");

        if(command.charAt(0) != 'C')
            throw new IllegalArgumentException("Bad command routing or syntax. Command for record creation should" +
                    " have a letter 'C' at the start.");

        String recordData = command.substring(2);
        Record record = recordConverter.stringToRecord(recordData);
        if(record == null)
            throw new IllegalArgumentException("Provided record data was bad syntax. Creating of the record aborted.");

        // TODO add here creating entry in B-tree index also
        int page = dataService.createRecord(this.dataTapeID, record);
        Entry entry = Entry.builder()
                .key(record.getKey())
                .dataPage(page)
                .build();
        bTreeService.createEntry(this.indexTapeID, entry);
    }
    public Record find(String command)
    {
        if(command == null)
            throw new IllegalArgumentException("Command data was null. Creating record aborted.");

        if(command.split(" ").length < 2)
            throw new IllegalArgumentException("Bad command syntax. Command for reading a record should look something like:\n" +
                    "R 100\n, where R - Read, 100 - example record key.");

        if(command.charAt(0) != 'R')
            throw new IllegalArgumentException("Bad command routing or syntax. Command for record reading should" +
                    " have a letter 'C' at the start.");

        String keyData = command.substring(2);
        long key;
        try {
            key = Long.parseUnsignedLong(keyData);
        } catch (NumberFormatException e)
        {
            e.printStackTrace();
            throw new IllegalArgumentException("Record key parsing failed. Key must be a maximum 8-byte positive number.");
        }
        // TODO add here finding the key in B-tree and retrieving its page number
        Entry entry = bTreeService.findEntry(this.indexTapeID, key);
        if(entry == null)
        {
            System.out.println("Entry with given key doesn't exist.");
            return null;
        }
        int page = entry.getDataPage();
        return dataService.findRecord(this.dataTapeID, page, key);
    }

    public void update(String command) throws InvalidAlgorithmParameterException {
        if(command == null)
            throw new IllegalArgumentException("Command data was null. Updating record aborted.");

        if(command.split(" ").length < 4)
            throw new IllegalArgumentException("Bad command syntax. Command for updating a record should look something like:\n" +
                    "U 100 25 10\n, where U - Update, 100 - example record key, 25 - example mass, 10 - example speed.");

        if(command.charAt(0) != 'U')
            throw new IllegalArgumentException("Bad command routing or syntax. Command for record update should" +
                    " have a letter 'U' at the start.");

        String recordData = command.substring(2);
        Record record = recordConverter.stringToRecord(recordData);
        if(record == null)
            throw new IllegalArgumentException("Provided record data was bad syntax. Update of the record aborted.");

        // TODO add here finding the key in B-tree and retrieving its page number
        int page = 0;
        dataService.updateRecord(this.dataTapeID, page, record);
    }

    public void delete(String command) throws InvalidAlgorithmParameterException {
        if(command == null)
            throw new IllegalArgumentException("Command data was null. Creating record aborted.");

        if(command.split(" ").length < 2)
            throw new IllegalArgumentException("Bad command syntax. Command for deleting a record should look something like:\n" +
                    "D 100\n, where D - Delete, 100 - example record key.");

        if(command.charAt(0) != 'D')
            throw new IllegalArgumentException("Bad command routing or syntax. Command for record deletion should" +
                    " have a letter 'D' at the start.");

        String keyData = command.substring(2);
        long key;
        try {
            key = Long.parseUnsignedLong(keyData);
        } catch (NumberFormatException e)
        {
            e.printStackTrace();
            throw new IllegalArgumentException("Record key parsing failed. Key must be a maximum 8-byte positive number.");
        }
        // TODO add here finding the key in B-tree and retrieving its page number
        int page = 0;
        dataService.deleteRecord(this.dataTapeID, page, key);
    }
    public void readAllRecords()
    {

    }

    public void readAllEntries()
    {

    }
}

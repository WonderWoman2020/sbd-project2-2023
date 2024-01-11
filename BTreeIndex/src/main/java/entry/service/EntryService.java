package entry.service;

import entry.converter.EntryConverter;
import entry.entity.Entry;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.ToString;
import record.entity.Record;
import tape.service.TapeService;

import java.io.File;
import java.security.InvalidAlgorithmParameterException;
import java.util.Arrays;
import java.util.Set;
import java.util.UUID;

@Builder
@ToString
@AllArgsConstructor
public class EntryService {

    private TapeService tapeService;
    private EntryConverter entryConverter;

    /**
     * This method assumes that upper layer takes responsibility of freeing buffers for tapes, if they're full.
     * @param tapeID
     * @param page
     * @param n Which entry to retrieve (its number in order, not a position in buffer - the position will be calculated).
     * @return Returns entry (can be empty entry).
     */
    public Entry readEntry(UUID tapeID, int page, int n)
    {
        if(!tapeService.getBufferedPages(tapeID).contains(page) && tapeService.isMaxBuffers(tapeID))
            throw new IllegalStateException("Reading requested entry requires loading a page from tape, but the buffer" +
                    " limit for this tape is full. Some buffer should have been freed before requesting this operation.");

        if(page < 0)
            throw new IllegalStateException("Page can't be a negative number.");

        if(n < 0)
            throw new IllegalStateException("Requested entry number was below 0 or bigger than last entry number." +
                    " Entry couldn't be read.");

        byte[] buffer = tapeService.readPage(tapeID, page);
        if(buffer == null)
            throw new IllegalStateException("Requested page should exist, but its data was null.");

        int pos = this.getEntryPosition(n);
        if(pos < 0 || pos > buffer.length - Entry.builder().build().getSize())
            throw new IllegalStateException("Position of the entry to read from buffer was below 0 or" +
                    " the entry won't fit in the buffer starting from this position.");

        return entryConverter.bytesToEntry(buffer, pos);
    }

    public void createEntry(UUID tapeID, int page, int n, Entry entry)
    {
        if(!tapeService.getBufferedPages(tapeID).contains(page) && tapeService.isMaxBuffers(tapeID))
            throw new IllegalStateException("Reading requested entry requires loading a page from tape, but the buffer" +
                    " limit for this tape is full. Some buffer should have been freed before requesting this operation.");

        if(page < 0)
            throw new IllegalStateException("Page can't be a negative number.");

        if(n < 0)
            throw new IllegalStateException("Requested entry number was below 0 or bigger than last entry number." +
                    " Entry couldn't be created.");

        byte[] buffer = tapeService.readPage(tapeID, page);
        if(buffer == null)
            throw new IllegalStateException("Requested page should exist, but its data was null.");

        int pos = this.getEntryPosition(n);
        if(pos < 0 || pos > buffer.length - entry.getSize())
            throw new IllegalStateException("Position of the entry to create in buffer was below 0 or" +
                    " the entry won't fit in the buffer starting from this position.");

        if(entryConverter.bytesToEntry(buffer, pos).getKey() != 0)
            throw new IllegalStateException("Something went wrong. In the requested correct spot in the node to create the entry" +
                    " should be free space for new entry, but there was some data already.");

        entryConverter.entryToBytes(entry, buffer, pos);
    }

    public void updateEntry(UUID tapeID, int page, int n, Entry entry)
    {
        if(!tapeService.getBufferedPages(tapeID).contains(page) && tapeService.isMaxBuffers(tapeID))
            throw new IllegalStateException("Reading requested entry requires loading a page from tape, but the buffer" +
                    " limit for this tape is full. Some buffer should have been freed before requesting this operation.");

        if(page < 0)
            throw new IllegalStateException("Page can't be a negative number.");

        if(n < 0)
            throw new IllegalStateException("Requested entry number was below 0 or bigger than last entry number." +
                    " Entry couldn't be updated.");

        byte[] buffer = tapeService.readPage(tapeID, page);
        if(buffer == null)
            throw new IllegalStateException("Requested page should exist, but its data was null.");

        int pos = this.getEntryPosition(n);
        if(pos < 0 || pos > buffer.length - entry.getSize())
            throw new IllegalStateException("Position of the entry to update in buffer was below 0 or" +
                    " the entry won't fit in the buffer starting from this position.");

        if(entryConverter.bytesToEntry(buffer, pos).getKey() == 0)
            throw new IllegalStateException("Something went wrong. In the requested correct spot in the node to update the entry" +
                    " should be already some data of the entry being updated, but there was free space.");

        entryConverter.entryToBytes(entry, buffer, pos);
    }

    public void removeEntry(UUID tapeID, int page, int n)
    {
        if(!tapeService.getBufferedPages(tapeID).contains(page) && tapeService.isMaxBuffers(tapeID))
            throw new IllegalStateException("Reading requested entry requires loading a page from tape, but the buffer" +
                    " limit for this tape is full. Some buffer should have been freed before requesting this operation.");

        if(page < 0)
            throw new IllegalStateException("Page can't be a negative number.");

        if(n < 0)
            throw new IllegalStateException("Requested entry number was below 0 or bigger than last entry number." +
                    " Entry couldn't be removed.");

        byte[] buffer = tapeService.readPage(tapeID, page);
        if(buffer == null)
            throw new IllegalStateException("Requested page should exist, but its data was null.");

        int pos = this.getEntryPosition(n);
        if(pos < 0 || pos > buffer.length - Entry.builder().build().getSize())
            throw new IllegalStateException("Position of the entry to update in buffer was below 0 or" +
                    " the entry won't fit in the buffer starting from this position.");

        if(entryConverter.bytesToEntry(buffer, pos).getKey() == 0)
            throw new IllegalStateException("Something went wrong. In the requested correct spot in the node to remove the entry" +
                    " should be already some data of the entry being removed, but there was free space.");

        Arrays.fill(buffer, pos, Entry.builder().build().getSize(), (byte) 0);
    }

    public int findEntryNumber(UUID tapeID, int page, long key)
    {
        
    }

    /**
     * Counts all non-empty entries.
     * @param tapeID
     * @param page
     * @return Entries number in this node.
     */
    public int getNodeEntries(UUID tapeID, int page)
    {

    }

    public int readNodePointer(UUID tapeID, int page, int n)
    {

    }

    public void setNodePointer(UUID tapeID, int page, int n, int pagePointer)
    {

    }

    public int readNodeParentPointer(UUID tapeID, int page)
    {

    }

    public void setNodeParentPointer(UUID tapeID, int page)
    {

    }

    public void saveNode(UUID tapeID, int page)
    {

    }
    private int getEntryPosition(int n)
    {
        return this.getNodeHeaderSize() + this.getNodePointerSize() + n * (Entry.builder().build().getSize() + this.getNodePointerSize());
    }

    private int getNodeHeaderSize()
    {
        // In header, there is stored a 4-byte parent pointer of the node
        return 4;
    }

    private int getNodePointerSize()
    {
        // All pointers in a node are of a constant size of 4 bytes
        return 4;
    }


    // Some TapeService methods, which are needed in upper app layers (This is an attempt to achieve encapsulation,
    // since only some methods of TapeService should be used in upper layers and some shouldn't)
    public Set<UUID> getTapesIDs()
    {
        return tapeService.getTapesIDs();
    }

    public UUID getInputTapeID()
    {
        return tapeService.getInputTapeID();
    }

    public Set<UUID> getIndexTapesIDs()
    {
        return tapeService.getIndexTapesIDs();
    }

    public Set<UUID> getDataTapesIDs()
    {
        return tapeService.getDataTapesIDs();
    }

    public void createTape(UUID id, boolean isIndexTape)
    {
        tapeService.create(id, isIndexTape);
    }

    public void setInputTape(UUID id, File file)
    {
        tapeService.setInputTape(id, file);
    }

    public void removeInputTape()
    {
        tapeService.removeInputTape();
    }

    public void deleteTape(UUID id)
    {
        tapeService.delete(id);
    }

    public void clearTape(UUID id)
    {
        tapeService.clear(id);
    }

    public void copyTapeFile(UUID id, String path, String fileName)
    {
        tapeService.copyTapeFile(id, path, fileName);
    }

    public void resetBlockReading(UUID id)
    {
        tapeService.resetBlockReading(id);
    }

    public void resetBlockWriting(UUID id)
    {
        tapeService.resetBlockWriting(id);
    }

    public void freeBufferedBlock(UUID tapeID, int page)
    {
        tapeService.freeBufferedBlock(tapeID, page);
    }

    public Set<Integer> getBufferedPages(UUID tapeID)
    {
        return tapeService.getBufferedPages(tapeID);
    }

    public int getReads(UUID id)
    {
        return tapeService.getReads(id);
    }

    public int getWrites(UUID id)
    {
        return tapeService.getWrites(id);
    }

    public int getTapeMaxBuffers(UUID id)
    {
        return tapeService.getMaxBuffers(id);
    }

    public void setTapeMaxBuffers(UUID id, int n)
    {
        tapeService.setMaxBuffers(id, n);
    }

    public int getFreeSpaceOnPage(UUID tapeID, int page)
    {
        return tapeService.getFreeSpaceOnPage(tapeID, page);
    }

    public void setFreeSpaceOnPage(UUID tapeID, int page, int amount)
    {
        tapeService.setFreeSpaceOnPage(tapeID, page, amount);
    }

    public int getTapePages(UUID id)
    {
        return tapeService.getPages(id);
    }

    public void addNextPage(UUID tapeID)
    {
        tapeService.addNextPage(tapeID);
    }

    public boolean isInputTape(UUID id)
    {
        return tapeService.isInputTape(id);
    }

    public boolean isIndexTape(UUID id)
    {
        return tapeService.isIndexTape(id);
    }

    public boolean isTapeMaxBuffers(UUID id)
    {
        return tapeService.isMaxBuffers(id);
    }

    public boolean isEOF(UUID id)
    {
        return tapeService.isEOF(id);
    }
}

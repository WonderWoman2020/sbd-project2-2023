package entry.service;

import entry.converter.EntryConverter;
import entry.entity.Entry;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.ToString;
import record.entity.Record;
import tape.service.TapeService;

import java.io.File;
import java.nio.ByteBuffer;
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

        byte[] buffer = tapeService.readPage(tapeID, page);
        if(buffer == null)
            throw new IllegalStateException("Requested page should exist, but its data was null.");

        if(n < 0 || n >= this.getMaxEntries(buffer.length))
            throw new IllegalStateException("Requested entry number was below 0 or bigger than max entry number for this node." +
                    " Entry couldn't be read. (it was "+n+" )");

        int pos = this.getEntryPosition(n);
        if(pos < 0 || pos > buffer.length - Entry.builder().build().getSize() - this.getNodePointerSize())
            throw new IllegalStateException("Position of the entry to read from buffer was below 0 or" +
                    " the entry won't fit in the buffer starting from this position.");

        return entryConverter.bytesToEntry(buffer, pos);
    }

    public void writeEntry(UUID tapeID, int page, int n, Entry entry)
    {
        if(!tapeService.getBufferedPages(tapeID).contains(page) && tapeService.isMaxBuffers(tapeID))
            throw new IllegalStateException("Writing requested entry requires loading a page from tape, but the buffer" +
                    " limit for this tape is full. Some buffer should have been freed before requesting this operation.");

        if(page < 0)
            throw new IllegalStateException("Page can't be a negative number.");

        if(entry == null)
            throw new IllegalStateException("Provided entry to write was null.");

        byte[] buffer = tapeService.readPage(tapeID, page);
        if(buffer == null)
            throw new IllegalStateException("Requested page should exist, but its data was null.");

        if(n < 0 || n >= this.getMaxEntries(buffer.length))
            throw new IllegalStateException("Requested entry number was below 0 or bigger than max entry number for this node." +
                    " Entry couldn't be cleared.");

        int pos = this.getEntryPosition(n);
        if(pos < 0 || pos > buffer.length - entry.getSize() - this.getNodePointerSize())
            throw new IllegalStateException("Position of the entry to create in buffer was below 0 or" +
                    " the entry won't fit in the buffer starting from this position.");

        entryConverter.entryToBytes(entry, buffer, pos);
    }

    public void clearEntry(UUID tapeID, int page, int n)
    {
        if(!tapeService.getBufferedPages(tapeID).contains(page) && tapeService.isMaxBuffers(tapeID))
            throw new IllegalStateException("Reading requested entry requires loading a page from tape, but the buffer" +
                    " limit for this tape is full. Some buffer should have been freed before requesting this operation.");

        if(page < 0)
            throw new IllegalStateException("Page can't be a negative number.");

        byte[] buffer = tapeService.readPage(tapeID, page);
        if(buffer == null)
            throw new IllegalStateException("Requested page should exist, but its data was null.");

        if(n < 0 || n >= this.getMaxEntries(buffer.length))
            throw new IllegalStateException("Requested entry number was below 0 or bigger than max entry number for this node." +
                    " Entry couldn't be cleared.");

        int pos = this.getEntryPosition(n);
        if(pos < 0 || pos > buffer.length - Entry.builder().build().getSize() - this.getNodePointerSize())
            throw new IllegalStateException("Position of the entry to create in buffer was below 0 or" +
                    " the entry won't fit in the buffer starting from this position.");

        Arrays.fill(buffer, pos, pos + Entry.builder().build().getSize(), (byte) 0);
    }

    /**
     *
     * @param tapeID
     * @param page
     * @param key
     * @return Number of an entry (its number in order in the node), which key was equal to provided key, or -1, if
     * there was no such entry.
     */
    public int findEntryNumber(UUID tapeID, int page, long key)
    {
        if(!tapeService.getBufferedPages(tapeID).contains(page) && tapeService.isMaxBuffers(tapeID))
            throw new IllegalStateException("Finding requested entry requires loading a page from tape, but the buffer" +
                    " limit for this tape is full. Some buffer should have been freed before requesting this operation.");

        if(page < 0)
            throw new IllegalStateException("Page can't be a negative number.");

        if(key <= 0)
            throw new IllegalStateException("Record key can't be below or equal to 0.");

        byte[] buffer = tapeService.readPage(tapeID, page);
        if(buffer == null)
            throw new IllegalStateException("Requested page should exist, but its data was null.");

        int n = 0;
        while(n < this.getMaxEntries(buffer.length))
        {
            Entry entry = this.readEntry(tapeID, page, n);
            if(entry.getKey() == key)
                return n;
            n++;
        }
        return -1; // There are no more entries in this node, so the requested entry isn't here
    }

    /**
     * Counts all non-empty entries. Throws exception if there are empty entries between real entries (this situation
     * can happen only when some b-tree operation hasn't been done in all parts. After every full b-tree operation,
     * there are no gap entries.). This method should only be used before or after full execution of a b-tree operation
     * (like create, delete etc.).
     * @param tapeID
     * @param page
     * @return Entries number in this node.
     */
    public int getNodeEntries(UUID tapeID, int page)
    {
        if(!tapeService.getBufferedPages(tapeID).contains(page) && tapeService.isMaxBuffers(tapeID))
            throw new IllegalStateException("Counting requested node entries requires loading a page from tape, but the buffer" +
                    " limit for this tape is full. Some buffer should have been freed before requesting this operation.");

        if(page < 0)
            throw new IllegalStateException("Page can't be a negative number.");

        byte[] buffer = tapeService.readPage(tapeID, page);
        if(buffer == null)
            throw new IllegalStateException("Requested page should exist, but its data was null.");

        int n = 0;
        int entries = 0;
        while(n < this.getMaxEntries(buffer.length))
        {
            Entry entry = this.readEntry(tapeID, page, n);
            if(entry.getKey() == 0)
                break;
            entries++;
            n++;
        }
        while(n < this.getMaxEntries(buffer.length))
        {
            Entry entry = this.readEntry(tapeID, page, n);
            if(entry.getKey() != 0)
                throw new IllegalStateException("After first empty entry, there shouldn't be any real entries in node.");
            n++;
        }

        return entries;
    }

    /**
     * Returns all node pointers, that can be non-null - all non-empty entries should have at least left node pointer, so
     * this method returns entries number + 1 (the last entry right pointer may or may not point to something).
     * @param tapeID
     * @param page
     * @return
     */
    public int getNodePointers(UUID tapeID, int page)
    {
        return this.getNodeEntries(tapeID, page) + 1;
    }

    public int readNodePointer(UUID tapeID, int page, int n)
    {
        if(!tapeService.getBufferedPages(tapeID).contains(page) && tapeService.isMaxBuffers(tapeID))
            throw new IllegalStateException("Reading requested node pointer requires loading a page from tape, but the buffer" +
                    " limit for this tape is full. Some buffer should have been freed before requesting this operation.");

        if(page < 0)
            throw new IllegalStateException("Page can't be a negative number.");

        byte[] buffer = tapeService.readPage(tapeID, page);
        if(buffer == null)
            throw new IllegalStateException("Requested page should exist, but its data was null.");

        if(n < 0 || n >= this.getMaxNodePointers(buffer.length))
            throw new IllegalStateException("Requested node pointer number was below 0 or bigger than max node pointer" +
                    " number for this node. Pointer couldn't be read.");

        int pos = this.getNodePointerPosition(n);
        if(pos < 0 || pos > buffer.length - this.getNodePointerSize())
            throw new IllegalStateException("Position of the node pointer to read from buffer was below 0 or" +
                    " the pointer won't fit in the buffer starting from this position.");

        return ByteBuffer.wrap(buffer, pos, this.getNodePointerSize()).getInt();
    }

    public void setNodePointer(UUID tapeID, int page, int n, int pagePointer)
    {
        if(!tapeService.getBufferedPages(tapeID).contains(page) && tapeService.isMaxBuffers(tapeID))
            throw new IllegalStateException("Writing requested node pointer requires loading a page from tape, but the buffer" +
                    " limit for this tape is full. Some buffer should have been freed before requesting this operation.");

        if(page < 0)
            throw new IllegalStateException("Page can't be a negative number.");

        byte[] buffer = tapeService.readPage(tapeID, page);
        if(buffer == null)
            throw new IllegalStateException("Requested page should exist, but its data was null.");

        if(n < 0 || n >= this.getMaxNodePointers(buffer.length))
            throw new IllegalStateException("Requested node pointer number was below 0 or bigger than max node pointer" +
                    " number for this node. Pointer couldn't be written.");

        int pos = this.getNodePointerPosition(n);
        if(pos < 0 || pos > buffer.length - this.getNodePointerSize())
            throw new IllegalStateException("Position of the node pointer to write in buffer was below 0 or" +
                    " the pointer won't fit in the buffer starting from this position.");

        ByteBuffer.wrap(buffer, pos, this.getNodePointerSize()).putInt(pagePointer);
    }

    /**
     *
     * @param tapeID
     * @param page
     * @param pagePointer
     * @return Requested node pointer number or -1, if there was no pointer with this value.
     */
    public int findNodePointerNumber(UUID tapeID, int page, int pagePointer)
    {
        int n = 0;
        while(n < this.getNodePointers(tapeID, page))
        {
            if(this.readNodePointer(tapeID, page, n) == pagePointer)
                return n;
            n++;
        }
        return -1;
    }
    public int readNodeParentPointer(UUID tapeID, int page)
    {
        if(!tapeService.getBufferedPages(tapeID).contains(page) && tapeService.isMaxBuffers(tapeID))
            throw new IllegalStateException("Reading requested node pointer requires loading a page from tape, but the buffer" +
                    " limit for this tape is full. Some buffer should have been freed before requesting this operation.");

        if(page < 0)
            throw new IllegalStateException("Page can't be a negative number.");

        byte[] buffer = tapeService.readPage(tapeID, page);
        if(buffer == null)
            throw new IllegalStateException("Requested page should exist, but its data was null.");

        if(buffer.length < this.getNodeHeaderSize())
            throw new IllegalStateException("The node buffer size was smaller than header. Parent node pointer" +
                    " couldn't be read.");

        return ByteBuffer.wrap(buffer, 0, this.getNodePointerSize()).getInt();
    }

    public void setNodeParentPointer(UUID tapeID, int page, int pagePointer)
    {
        if(!tapeService.getBufferedPages(tapeID).contains(page) && tapeService.isMaxBuffers(tapeID))
            throw new IllegalStateException("Writing requested node pointer requires loading a page from tape, but the buffer" +
                    " limit for this tape is full. Some buffer should have been freed before requesting this operation.");

        if(page < 0)
            throw new IllegalStateException("Page can't be a negative number.");

        byte[] buffer = tapeService.readPage(tapeID, page);
        if(buffer == null)
            throw new IllegalStateException("Requested page should exist, but its data was null.");

        if(buffer.length < this.getNodeHeaderSize())
            throw new IllegalStateException("The node buffer size was smaller than header. Parent node pointer" +
                    " couldn't be written.");

        ByteBuffer.wrap(buffer, 0, this.getNodePointerSize()).putInt(pagePointer);
    }

    public void saveNode(UUID tapeID, int page) throws InvalidAlgorithmParameterException {
        if(!tapeService.getBufferedPages(tapeID).contains(page) && tapeService.isMaxBuffers(tapeID))
            throw new IllegalStateException("Saving requested node requires loading a page from tape, but the buffer" +
                    " limit for this tape is full. Some buffer should have been freed before requesting this operation.");

        if(page < 0)
            throw new IllegalStateException("Page can't be a negative number.");

        byte[] buffer = tapeService.readPage(tapeID, page);
        if(buffer == null)
            throw new IllegalStateException("Requested page should exist, but its data was null.");

        tapeService.writePage(tapeID, page, buffer, buffer.length);
    }

    /**
     * Clears all node data except the header.
     * @param tapeID
     * @param page
     */
    public void clearNodeData(UUID tapeID, int page)
    {
        if(!tapeService.getBufferedPages(tapeID).contains(page) && tapeService.isMaxBuffers(tapeID))
            throw new IllegalStateException("Saving requested node requires loading a page from tape, but the buffer" +
                    " limit for this tape is full. Some buffer should have been freed before requesting this operation.");

        if(page < 0)
            throw new IllegalStateException("Page can't be a negative number.");

        byte[] buffer = tapeService.readPage(tapeID, page);
        if(buffer == null)
            throw new IllegalStateException("Requested page should exist, but its data was null.");

        if(buffer.length <= this.getNodeHeaderSize())
            throw new IllegalStateException("The node buffer size was smaller than or equal to header." +
                    " There was no node data to clear.");

        Arrays.fill(buffer, this.getNodeHeaderSize(), buffer.length, (byte) 0);
    }
    private int getEntryPosition(int n)
    {
        return this.getNodeHeaderSize() + this.getNodePointerSize() + n * (Entry.builder().build().getSize() + this.getNodePointerSize());
    }

    private int getNodePointerPosition(int n)
    {
        return this.getNodeHeaderSize() + n * (this.getNodePointerSize() + Entry.builder().build().getSize());
    }

    private int getMaxEntries(int bufferSize)
    {
        if(bufferSize <= 0)
            throw new IllegalStateException("Provided buffer size was below or equal to 0." +
                    " Node buffer size must be bigger than 0 to contain some data.");
        int maxEntries = (bufferSize - (this.getNodeHeaderSize() + this.getNodePointerSize())) /
                (Entry.builder().build().getSize() + this.getNodePointerSize());
        if(maxEntries < 0)
            throw new IllegalStateException("Something went wrong. Max entries number for this node was below 0." +
                    " A buffer size of the node may be too small.");
        if(maxEntries * (Entry.builder().build().getSize() + this.getNodePointerSize())
                + this.getNodeHeaderSize() + this.getNodePointerSize() != bufferSize)
            throw new IllegalStateException("Size of the node was incorrect. It should be equal to sum of possible entries" +
                    " and nodes that could be put in it, but it wasn't.");

        return maxEntries;
    }

    private int getMaxNodePointers(int bufferSize)
    {
        return this.getMaxEntries(bufferSize) + 1;
    }

    public int getNodeHeaderSize()
    {
        // In header, there is stored a 4-byte parent pointer of the node
        return 4;
    }

    public int getNodePointerSize()
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

    public int getTapeFreePages(UUID id)
    {
        return tapeService.getFreePages(id);
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

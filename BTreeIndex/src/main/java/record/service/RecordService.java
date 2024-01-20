package record.service;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.ToString;
import record.converter.RecordConverter;
import record.entity.Record;
import tape.service.TapeService;

import java.io.File;
import java.nio.ByteBuffer;
import java.security.InvalidAlgorithmParameterException;
import java.util.*;

@Builder
@ToString
@AllArgsConstructor
public class RecordService {

    private TapeService tapeService;
    private RecordConverter recordConverter;

    /**
     * Current memory block, stored untill it will be fully <strong>read</strong>
     */
    private Map<UUID, byte[]> readBlocksStored;

    /**
     * Current memory block, stored untill it will be fully <strong>written to</strong>
     */
    private Map<UUID, byte[]> writeBlocksStored;

    /**
     * Offsets to position at which we are currently in reading the block
     */
    private Map<UUID, Integer> readBlocksOffs;

    /**
     * Offsets to position at which we are currently in writing to the block
     */
    private Map<UUID, Integer> writeBlocksOffs;


    // Reading and writing single Record methods
    public Record readNextRecord(UUID tapeID)
    {
        if(!this.hasNextRecord(tapeID))
            return null;

        byte[] currentBlock = this.readBlocksStored.get(tapeID);
        Integer off = this.readBlocksOffs.get(tapeID);
        off = (off == null) ? 0 : off;

        if(!recordConverter.isFullRecord(currentBlock, off))
        {
            byte[] restBytes = null;
            if(currentBlock != null) {
                if (currentBlock.length - off > 0) {
                    restBytes = new byte[currentBlock.length - off];
                    System.arraycopy(currentBlock, off, restBytes, 0, currentBlock.length - off);
                }
            }

            currentBlock = tapeService.readNextBlock(tapeID);
            if(currentBlock == null) // End of file
            {
                if(restBytes != null)
                    throw new IllegalStateException("Some bytes were left in the end of the tape data file, but a full" +
                            " record hasn't been read. File may contain incorrect records.");
                this.readBlocksStored.remove(tapeID);
                this.readBlocksOffs.remove(tapeID);
                return null;
            }

            if(restBytes != null)
            {
                byte[] allBytes = new byte[restBytes.length + currentBlock.length];
                System.arraycopy(restBytes, 0, allBytes, 0, restBytes.length);
                System.arraycopy(currentBlock, 0, allBytes, restBytes.length, currentBlock.length);
                currentBlock = allBytes;
            }

            this.readBlocksStored.put(tapeID, currentBlock);
            this.readBlocksOffs.put(tapeID, 0);
        }

        if(!recordConverter.isFullRecord(this.readBlocksStored.get(tapeID), this.readBlocksOffs.get(tapeID)))
            throw new IllegalStateException("New block has been successfully read, but the record still can't be read fully.");

        Record record = recordConverter.bytesToRecord(this.readBlocksStored.get(tapeID), this.readBlocksOffs.get(tapeID));
        this.readBlocksOffs.put(tapeID, this.readBlocksOffs.get(tapeID)+record.getSize());
        return record;
    }

    public void writeNextRecord(UUID tapeID, Record record) throws InvalidAlgorithmParameterException {
        if(record == null)
            return;

        if(this.writeBlocksStored.get(tapeID) == null)
        {
            this.writeBlocksStored.put(tapeID, new byte[tapeService.BLOCK_SIZE]);
            this.writeBlocksOffs.put(tapeID, 0);
        }

        byte[] currentBlock = this.writeBlocksStored.get(tapeID);
        Integer off = this.writeBlocksOffs.get(tapeID);

        if(off + record.getSize() <= currentBlock.length)
        {
            recordConverter.recordToBytes(record, currentBlock, off);
            this.writeBlocksOffs.put(tapeID, this.writeBlocksOffs.get(tapeID)+record.getSize());
            return;
        }

        int leftSpace = currentBlock.length - off;
        byte[] recordData = recordConverter.recordToBytes(record);

        // Copy that much of a record data which will fit into the block and write it to tape
        System.arraycopy(recordData, 0, currentBlock, off, leftSpace);
        tapeService.writeNextBlock(tapeID, currentBlock, currentBlock.length);

        // Get a fresh writing block and write all the record data that haven't fit into the previous block
        Arrays.fill(currentBlock, (byte) 0);
        System.arraycopy(recordData, leftSpace, currentBlock, 0, recordData.length - leftSpace);
        this.writeBlocksStored.put(tapeID, currentBlock);
        this.writeBlocksOffs.put(tapeID, recordData.length - leftSpace);
    }

    public void endWriting(UUID tapeID) throws InvalidAlgorithmParameterException {
        byte[] currentBlock = this.writeBlocksStored.get(tapeID);
        Integer off = this.writeBlocksOffs.get(tapeID);
        if(currentBlock == null)
            return;

        if(off == null)
            throw new NoSuchElementException("For some reason, offset for this write block hasn't been initialized.");

        if(off <= 0)
            return;

        if(off > currentBlock.length)
            throw new IllegalStateException("Offset for this write block was greater than the block size.");

        tapeService.writeNextBlock(tapeID, currentBlock, off);
    }

    /**
     * Optimistically assumes, that if there are any bytes left to read from the tape, the tape contains next Record.
     * @param tapeID
     * @return Whether the tape has a Record to read left (or a possibility of that, if there
     * are some unread bytes left).
     */
    public boolean hasNextRecord(UUID tapeID)
    {
        if(!tapeService.isEOF(tapeID))
            return true;

        if(this.readBlocksStored.get(tapeID) == null)
            return false;

        if(this.readBlocksOffs.get(tapeID) == null)
            throw new NoSuchElementException("For some reason, offset for this read block hasn't been initialized.");

        if(this.readBlocksOffs.get(tapeID) < this.readBlocksStored.get(tapeID).length)
            return true;

        return false;
    }

    // Non-sequential read/write methods for Record
    /**
     * This method assumes that upper layer takes responsibility of freeing buffers for tapes, if they're full.
     * It also assumes, that a record must be written as a whole on a single page. It assumes too, that records
     * are written one by one and there are no gaps - writeRecord() function should be responsible for
     * shifting them that way, when removing or adding a record, before the page will be saved on disk.
     * @param tapeID
     * @param page
     * @param key
     * @return Requested record or null, if the record with key equal to provided one hasn't been found on this page.
     */
    public Record readRecord(UUID tapeID, int page, long key)
    {
        if(!tapeService.getBufferedPages(tapeID).contains(page) && tapeService.isMaxBuffers(tapeID))
            throw new IllegalStateException("Reading requested record requires loading a page from tape, but the buffer" +
                    " limit for this tape is full. Some buffer should have been freed before requesting this operation.");

        if(page < 0)
            throw new IllegalStateException("Page can't be a negative number.");

        byte[] buffer = tapeService.readPage(tapeID, page);
        if(buffer == null)
            throw new IllegalStateException("Requested page should exist, but its data was null.");

        int pos = this.findRecordPosition(buffer, key);
        if(pos == -1)
            return null; // Record not found

        return recordConverter.bytesToRecord(buffer, pos);
    }

    /**
     * Writes provided record to requested page buffer and saves the page on disk. Requires loading the page on which
     * record will be stored, so number of free buffers needs to be checked beforehand to contain at least 1 buffer spot.
     * @param tapeID
     * @param page
     * @param record
     * @throws InvalidAlgorithmParameterException
     */
    public void createRecord(UUID tapeID, int page, Record record) throws InvalidAlgorithmParameterException {
        if(!tapeService.getBufferedPages(tapeID).contains(page) && tapeService.isMaxBuffers(tapeID))
            throw new IllegalStateException("Creating requested record requires loading a page from tape, but the buffer" +
                    " limit for this tape is full. Some buffer should have been freed before requesting this operation.");

        if(page < 0)
            throw new IllegalStateException("Page can't be a negative number.");

        if(record == null)
            throw new IllegalStateException("Record provided to write was null. Creation of the record aborted.");

        byte[] buffer = tapeService.readPage(tapeID, page);
        if(buffer == null)
            throw new IllegalStateException("Requested page should exist, but its data was null.");

        if(this.findRecordPosition(buffer, record.getKey()) != -1)
            throw new IllegalStateException("Record with that key already exists on this page." +
                    " A key of a record should be unique for a whole tape.");

        int freeSpaceStart = this.findStartOfFreeSpace(buffer);
        if(freeSpaceStart == -1 || buffer.length - freeSpaceStart < record.getSize())
            throw new IllegalStateException("There is not enough space on this page to store new record on it.");

        recordConverter.recordToBytes(record, buffer, freeSpaceStart);
        tapeService.writePage(tapeID, page, buffer, buffer.length);
    }

    /**
     * This method assumes, that all records are of the same size (which is true for my type of Record for this project).
     * @param tapeID
     * @param page
     * @param record
     */
    public void updateRecord(UUID tapeID, int page, Record record) throws InvalidAlgorithmParameterException {
        if(!tapeService.getBufferedPages(tapeID).contains(page) && tapeService.isMaxBuffers(tapeID))
            throw new IllegalStateException("Creating requested record requires loading a page from tape, but the buffer" +
                    " limit for this tape is full. Some buffer should have been freed before requesting this operation.");

        if(page < 0)
            throw new IllegalStateException("Page can't be a negative number.");

        if(record == null)
            throw new IllegalStateException("Record provided to write was null. Creation of the record aborted.");

        byte[] buffer = tapeService.readPage(tapeID, page);
        if(buffer == null)
            throw new IllegalStateException("Requested page should exist, but its data was null.");

        int pos = this.findRecordPosition(buffer, record.getKey());
        if(pos == -1)
            throw new IllegalStateException("Record with that key doesn't exists on this page, so it can't be updated.");

        if(buffer.length - pos < record.getSize())
            throw new IllegalStateException("Since the record to update was found on this page, there should be enough" +
                    " space for the updating record (all records of type Record are the same size), but it wasn't.");

        recordConverter.recordToBytes(record, buffer, pos);
        tapeService.writePage(tapeID, page, buffer, buffer.length);
    }

    public void removeRecord(UUID tapeID, int page, long key) throws InvalidAlgorithmParameterException {
        if(!tapeService.getBufferedPages(tapeID).contains(page) && tapeService.isMaxBuffers(tapeID))
            throw new IllegalStateException("Creating requested record requires loading a page from tape, but the buffer" +
                    " limit for this tape is full. Some buffer should have been freed before requesting this operation.");

        if(page < 0)
            throw new IllegalStateException("Page can't be a negative number.");

        byte[] buffer = tapeService.readPage(tapeID, page);
        if(buffer == null)
            throw new IllegalStateException("Requested page should exist, but its data was null.");

        int pos = this.findRecordPosition(buffer, key);
        if(pos == -1)
            throw new IllegalStateException("Record with that key doesn't exists on this page, so it can't be removed.");

        Record record = recordConverter.bytesToRecord(buffer, pos);
        if(record == null)
            throw new IllegalStateException("Record with provided key was found on the page, but after trying to read" +
                    " it, it turned out null. Something went wrong, removing the record aborted.");

        // Erase the record
        Arrays.fill(buffer, pos, pos + record.getSize(), (byte) 0);
        // Shift all bytes (possible records) from right to the pos, to remove gap between records
        System.arraycopy(buffer, pos + record.getSize(), buffer, pos, buffer.length - (pos + record.getSize()));
        // Erase old bytes at the end of array, that stayed he same (there could be a redundant copy of a
        // record, that has been shifted 1 spot left)
        Arrays.fill(buffer, buffer.length - record.getSize(), buffer.length, (byte) 0);
        
        tapeService.writePage(tapeID, page, buffer, buffer.length);
    }

    /**
     * Gets a buffer and searches for the requested record position in it.
     * @param buffer
     * @param key
     * @return Position of the record in the buffer, if found, or -1, if not.
     */
    public int findRecordPosition(byte[] buffer, long key)
    {
        if(buffer == null)
            throw new IllegalStateException("Provided page data buffer was null.");

        int consumed = 0;
        while(recordConverter.isFullRecord(buffer, consumed))
        {
            Record record = recordConverter.bytesToRecord(buffer, consumed);
            if(record.getKey() == 0) // Assuming end of records on that page, key equal to 0 is forbidden
                return -1;

            if(record.getKey() == key)
                return consumed;

            consumed += record.getSize();
        }
        // Since this method assumes only a whole record could be on one page, now the whole page is assumed to be read
        return -1;
    }

    public int findStartOfFreeSpace(byte[] buffer)
    {
        if(buffer == null)
            throw new IllegalStateException("Provided page data buffer was null.");

        int consumed = 0;
        while(recordConverter.isFullRecord(buffer, consumed))
        {
            Record record = recordConverter.bytesToRecord(buffer, consumed);
            if(record.getKey() == 0) // Assuming end of records on that page, key equal to 0 is forbidden
                return consumed;

            consumed += record.getSize();
        }
        // Since this method assumes only a whole record could be on one page, now the whole page is assumed to be read
        return -1;
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
        this.readBlocksStored.remove(id);
        this.readBlocksOffs.remove(id);
    }

    public void resetBlockWriting(UUID id)
    {
        tapeService.resetBlockWriting(id);
        this.writeBlocksStored.remove(id);
        this.writeBlocksOffs.remove(id);
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

package record.service;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.ToString;
import record.converter.RecordConverter;
import record.entity.Record;
import tape.service.TapeService;

import java.io.File;
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

    public void createTape(UUID id)
    {
        tapeService.create(id);
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

    public int getReads(UUID id)
    {
        return tapeService.getReads(id);
    }

    public int getWrites(UUID id)
    {
        return tapeService.getWrites(id);
    }

    public int getRealRuns(UUID id)
    {
        return tapeService.getRealRuns(id);
    }

    public int getEmptyRuns(UUID id)
    {
        return tapeService.getEmptyRuns(id);
    }

    public int getAllRuns(UUID id)
    {
        return tapeService.getAllRuns(id);
    }

    public List<Integer> getRecordsInEachRun(UUID id)
    {
        return tapeService.getRecordsInEachRun(id);
    }

    /**
     * Get records number of one specific run
     * @param id
     * @param run Index of the run which records count is requested
     * @return
     */
    public int getRecordsInRun(UUID id, int run)
    {
        return tapeService.getRecordsInRun(id, run);
    }

    public int getNextRunRecords(UUID id)
    {
        return tapeService.getNextRunRecords(id);
    }

    public void addNextRun(UUID id, int records)
    {
        tapeService.addNextRun(id, records);
    }

    public void removeNextRun(UUID id)
    {
        tapeService.removeNextRun(id);
    }

    /*public void setRealRuns(UUID id, int realRuns)
    {
        tapeService.setRealRuns(id, realRuns);
    }

    public void setEmptyRuns(UUID id, int emptyRuns)
    {
        tapeService.setEmptyRuns(id, emptyRuns);
    }

    public void setRecordsInEachRun(UUID id, List<Integer> recordsCount)
    {
        tapeService.setRecordsInEachRun(id, recordsCount);
    }*/

    public boolean isInputTape(UUID id)
    {
        return tapeService.isInputTape(id);
    }

    public boolean isTapeEmpty(UUID id)
    {
        return tapeService.isEmpty(id);
    }

    public boolean isEOF(UUID id)
    {
        return tapeService.isEOF(id);
    }

}

package tape.service;

import data_generator.FilesUtility;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.ToString;
import tape.entity.Tape;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.security.InvalidAlgorithmParameterException;
import java.util.*;
import java.util.stream.Collectors;

@Builder
@ToString
@AllArgsConstructor
public class TapeService {

    /**
     * All tapes used in algorithm and the input tape.
     */
    private Map<UUID, Tape> tapes;

    /**
     * Block read counter for each tape, for realisation of sequential block reading (to know where we are at the file).
     * It is resetted after each whole tape reading cycle.
     */
    private Map<UUID, Integer> tapesCurrentReadBlock;

    /**
     * Block write counter for each tape, for realisation of sequential block writing (to know where we are at the file).
     * It is resetted after each whole tape writing cycle.
     */
    private Map<UUID, Integer> tapesCurrentWriteBlock;

    /**
     * Constant memory block size, used while reading the tapes (sequentially reading block by block).
     */
    public final int BLOCK_SIZE;

    /**
     * Location where the tapes files will be stored
     */
    private final String filesPath;

    /**
     * Base name for the tapes files, it is concatenated with tape ID to create different names
     */
    private final String filesBaseName;

    /**
     * Utility object to manage file operations
     */
    private FilesUtility filesUtility;


    // CRUD operations on tapes (in particular, on the data files)

    /**
     * Returns all tapes IDs except input tape
     * @return
     */
    public Set<UUID> getTapesIDs()
    {
        UUID inputTapeID = this.getInputTapeID();
        return this.tapes.keySet().stream()
            .filter(id -> !id.equals(inputTapeID))
            .collect(Collectors.toSet());
    }

    /**
     * Method to retrieve the input tape ID
     * @return
     */
    public UUID getInputTapeID()
    {
        Optional<Tape> inputTape = this.tapes.values().stream()
                .filter(Tape::isInputTape)
                .findFirst();
        if(inputTape.isEmpty())
            throw new NoSuchElementException();

        return inputTape.get().getId();
    }
    public void create(UUID id)
    {
        filesUtility.createDirs(Path.of(filesPath));
        File emptyFile = filesUtility.createFile(Path.of(filesPath,filesBaseName +"_"+ id.toString() + ".dat"));
        Tape tape = Tape.builder()
                .id(id)
                .file(emptyFile)
                .isInputTape(false)
                .reads(0)
                .writes(0)
                .realRuns(0)
                .emptyRuns(0)
                .recordsInEachRun(new ArrayList<>())
                .build();

        this.tapes.put(tape.getId(), tape);
        this.tapesCurrentReadBlock.put(tape.getId(), 0);
        this.tapesCurrentWriteBlock.put(tape.getId(), 0);
    }

    // Special create method, only to create input tape
    public void setInputTape(UUID id, File file)
    {
        Tape inputTape = Tape.builder()
                .id(id)
                .file(file)
                .isInputTape(true)
                .reads(0)
                .writes(0)
                .realRuns(0)
                .emptyRuns(0)
                .recordsInEachRun(new ArrayList<>())
                .build();

        this.tapes.put(inputTape.getId(), inputTape);
        this.tapesCurrentReadBlock.put(inputTape.getId(), 0);
        this.tapesCurrentWriteBlock.put(inputTape.getId(), 0);
    }

    /** Special delete method, only to remove input tape object without deleting the input file from disk
     *
     */
    public void removeInputTape()
    {
        Tape tape = this.tapes.remove(this.getInputTapeID());
        if(tape == null)
            throw new NoSuchElementException();

        this.tapesCurrentReadBlock.remove(tape.getId(), 0);
        this.tapesCurrentWriteBlock.remove(tape.getId(), 0);
    }

    public void delete(UUID id)
    {
        Tape tape = this.tapes.remove(id);
        if(tape == null)
            throw new NoSuchElementException();

        this.tapesCurrentReadBlock.remove(tape.getId(), 0);
        this.tapesCurrentWriteBlock.remove(tape.getId(), 0);

        if(tape.getFile() == null)
            throw new NoSuchElementException("File in tape was null.");

        filesUtility.deleteFile(tape.getFile().toPath());
    }

    public void clear(UUID id)
    {
        Tape tape = this.tapes.get(id);
        if(tape == null)
            throw new NoSuchElementException();

        try (RandomAccessFile raf = new RandomAccessFile(tape.getFile(), "rw")){
            raf.setLength(0);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        this.tapesCurrentReadBlock.put(tape.getId(), 0);
        this.tapesCurrentWriteBlock.put(tape.getId(), 0);
    }

    public void copyTapeFile(UUID id, String path, String fileName)
    {
        Tape tape = this.tapes.get(id);
        if(tape == null)
            throw new NoSuchElementException();

        if(path == null || fileName == null)
            throw new RuntimeException("There was no path or filename provided where the tape file should be copied to.");

        filesUtility.createDirs(Path.of(path));
        filesUtility.copyFile(tape.getFile(), Path.of(path), Path.of(fileName));
    }

    /**
     * Reads memory blocks from file. Blocks are of size of the {@link TapeService#BLOCK_SIZE} constant or smaller,
     * if there is not that much amount of data left at the end of the file.
     * @param id
     * @param off
     * @return Byte array with data read from the tape file
     */
    public byte[] readBlock(UUID id, long off)
    {
        Tape tape = this.tapes.get(id);
        if(tape == null)
            throw new NoSuchElementException();

        try (RandomAccessFile raf = new RandomAccessFile(tape.getFile(), "r")){
            byte[] data = new byte[this.BLOCK_SIZE];
            raf.seek(off);
            int read = raf.read(data);

            if(read == -1)
                return null;

            if(read < this.BLOCK_SIZE)
            {
                byte[] smallerChunk = new byte[read];
                System.arraycopy(data, 0, smallerChunk, 0, read);
                data = smallerChunk;
            }

            this.incReads(tape.getId());
            return data;

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Reads tape file sequentially (block by block or, in other words, chunk by chunk)
     * controlling the current position in file with {@link TapeService#tapesCurrentReadBlock} field.
     * @param id
     * @return Byte array with data read from the tape file.
     * (Maybe of size of the {@link TapeService#BLOCK_SIZE} or smaller, if it is the last chunk of data in file)
     */
    public byte[] readNextBlock(UUID id)
    {
        byte[] data = this.readBlock(id, (long) this.BLOCK_SIZE *this.tapesCurrentReadBlock.get(id));

        Tape tape = this.tapes.get(id);
        if(tape == null)
            throw new NoSuchElementException();

        if(data != null)
            this.tapesCurrentReadBlock.put(tape.getId(), tapesCurrentReadBlock.get(tape.getId()) + 1);

        return data;
    }

    /**
     * Writes memory blocks to file. Requires blocks of size of the {@link TapeService#BLOCK_SIZE} constant
     * and a number {@code len} of bytes to write from this block ({@code len} should always be equal
     * {@link TapeService#BLOCK_SIZE} to achieve constant memory amount usage (if TapeService is used
     * in merge algorithm) and may be smaller than that <strong>only if there is not that much amount of data left
     * to write in the last block</strong>)
     * @param id
     * @param data
     * @param off position in file (in bytes)
     * @param len
     * @return
     */
    public boolean writeBlock(UUID id, long off, byte[] data, int len) throws InvalidAlgorithmParameterException {
        if(data == null)
            throw new InvalidAlgorithmParameterException("No data to write was provided.");

        if(data.length < this.BLOCK_SIZE)
            throw new InvalidAlgorithmParameterException("Too small block of data to write was provided (to achieve" +
                    " block writing, all blocks should be of an equal size).");

        Tape tape = this.tapes.get(id);
        if(tape == null)
            throw new NoSuchElementException();

        try (RandomAccessFile raf = new RandomAccessFile(tape.getFile(), "rw")){
            raf.seek(off);
            raf.write(data, 0, len);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        this.incWrites(tape.getId());
        return true;
    }

    /**
     * Writes to tape file sequentially (block by block or, in other words, chunk by chunk)
     * controlling the current position in file with {@link TapeService#tapesCurrentWriteBlock} field.
     * @param id
     * @param data Provided data blocks should be of size of the {@link TapeService#BLOCK_SIZE} and ({@code len}
     *             parameter tells how much data we want to write from it.
     * @param len How much data we want to write from the block to tape file. It should be equal
     *            to {@link TapeService#BLOCK_SIZE} or can smaller, but only when it is the last block to
     *            write to the tape file.
     * @return Whether operation succeeded.
     */
    public boolean writeNextBlock(UUID id, byte[] data, int len) throws InvalidAlgorithmParameterException
    {
        boolean written = this.writeBlock(id, (long) this.BLOCK_SIZE *this.tapesCurrentWriteBlock.get(id), data, len);

        Tape tape = this.tapes.get(id);
        if(tape == null)
            throw new NoSuchElementException();

        if(written)
            this.tapesCurrentWriteBlock.put(tape.getId(), tapesCurrentWriteBlock.get(tape.getId()) + 1);

        return written;
    }

    public void resetBlockReading(UUID id)
    {
        Tape tape = this.tapes.get(id);
        if(tape == null)
            throw new NoSuchElementException();

        this.tapesCurrentReadBlock.put(tape.getId(), 0);
    }

    public void resetBlockWriting(UUID id)
    {
        Tape tape = this.tapes.get(id);
        if(tape == null)
            throw new NoSuchElementException();

        this.tapesCurrentWriteBlock.put(tape.getId(), 0);
    }

    // Getters and setters for some tape properties

    public int getReads(UUID id)
    {
        Tape tape = this.tapes.get(id);
        if(tape == null)
            throw new NoSuchElementException();

        return tape.getReads();
    }

    public int getWrites(UUID id)
    {
        Tape tape = this.tapes.get(id);
        if(tape == null)
            throw new NoSuchElementException();

        return tape.getWrites();
    }

    public int getRealRuns(UUID id)
    {
        Tape tape = this.tapes.get(id);
        if(tape == null)
            throw new NoSuchElementException();

        return tape.getRealRuns();
    }

    public int getEmptyRuns(UUID id)
    {
        Tape tape = this.tapes.get(id);
        if(tape == null)
            throw new NoSuchElementException();

        return tape.getEmptyRuns();
    }

    public int getAllRuns(UUID id)
    {
        return this.getRealRuns(id) + this.getEmptyRuns(id);
    }

    public List<Integer> getRecordsInEachRun(UUID id)
    {
        Tape tape = this.tapes.get(id);
        if(tape == null)
            throw new NoSuchElementException();

        return tape.getRecordsInEachRun();
    }

    /**
     * Get records number of one specific run
     * @param id
     * @param run Index of the run which records count is requested
     * @return
     */
    public int getRecordsInRun(UUID id, int run)
    {
        Tape tape = this.tapes.get(id);
        if(tape == null)
            throw new NoSuchElementException();

        List<Integer> recordsInEachRun = tape.getRecordsInEachRun();
        return recordsInEachRun.get(run);
    }

    public int getNextRunRecords(UUID id)
    {
        return this.getRecordsInRun(id, 0);
    }
    public void incReads(UUID id)
    {
        Tape tape = this.tapes.get(id);
        if(tape == null)
            throw new NoSuchElementException();

        tape.setReads(tape.getReads()+1);
    }

    public void incWrites(UUID id)
    {
        Tape tape = this.tapes.get(id);
        if(tape == null)
            throw new NoSuchElementException();

        tape.setWrites(tape.getWrites()+1);
    }

    public void addNextRun(UUID id, int records)
    {
        Tape tape = this.tapes.get(id);
        if(tape == null)
            throw new NoSuchElementException();

        if(tape.isInputTape())
            throw new IllegalStateException("Cannot add runs to an input tape. This is an input file, so it shouldn't be changed.");

        if(records < 0)
            throw new IllegalStateException("Records count provided for this new run was below 0." +
                    " Records number in a run must be a natural number or 0.");

        List<Integer> recordsInEachRun = tape.getRecordsInEachRun();
        /*if(!recordsInEachRun.isEmpty()) {
            int previousRunRecords = recordsInEachRun.get(recordsInEachRun.size() - 1);
            if (previousRunRecords == 0 && records > 0)
                throw new IllegalStateException("New run contained some records while previous was an empty run." +
                        " After empty runs, there can't be any more real runs on the tape.");
        }*/
        if(this.getEmptyRuns(id) > 0)
            if(records > 0)
                throw new IllegalStateException("New run contained some records while there already are some empty runs." +
                        " After empty runs, there can't be any more real runs on the tape.");

        if(records == 0) {
            recordsInEachRun.add(0, records);
            tape.setRecordsInEachRun(recordsInEachRun);
            tape.setEmptyRuns(tape.getEmptyRuns() + 1);
        }
        else {
            recordsInEachRun.add(records);
            tape.setRecordsInEachRun(recordsInEachRun);
            tape.setRealRuns(tape.getRealRuns() + 1);
        }
    }

    public void removeNextRun(UUID id)
    {
        Tape tape = this.tapes.get(id);
        if(tape == null)
            throw new NoSuchElementException();

        if(tape.isInputTape())
            throw new IllegalStateException("Input tape doesn't know how many runs is on it. The runs can be" +
                    " counted for the first time in the distribution phase.");

        List<Integer> recordsInEachRun = tape.getRecordsInEachRun();
        if(recordsInEachRun.isEmpty())
            throw new NoSuchElementException("No runs to remove were left on the tape.");

        // When reading the tape sequentially, we first read the first runs,
        // so first run in the list needs to be cleared after reading the run
        int records = recordsInEachRun.remove(0);
        tape.setRecordsInEachRun(recordsInEachRun);
        if(records == 0) {
            if (tape.getEmptyRuns() == 0)
                throw new IllegalStateException("Some calculations went wrong." +
                        " Empty runs counter was 0, while there was an empty run in the list.");
            tape.setEmptyRuns(tape.getEmptyRuns() - 1);
        }
        else {
            if (tape.getRealRuns() == 0)
                throw new IllegalStateException("Some calculations went wrong." +
                        " Real runs counter was 0, while there was a real run in the list.");
            tape.setRealRuns(tape.getRealRuns() - 1);
        }
    }

    // Some boolean check methods

    public boolean isInputTape(UUID id)
    {
        Tape tape = this.tapes.get(id);
        if(tape == null)
            throw new NoSuchElementException();

        return tape.isInputTape();
    }

    public boolean isEmpty(UUID id)
    {
        Tape tape = this.tapes.get(id);
        if(tape == null)
            throw new NoSuchElementException();

        return (tape.getRealRuns() + tape.getEmptyRuns() <= 0);
    }

    /**
     * Checks if sequential block reading has already reached end of the tape data file. It compares file length and
     * current reading position to tell, whether the sequential reading is finished. (Assumes correct execution of
     * sequential reading - from start to the end of the file. Ignores possibility of skipping blocks while reading.)
     * @param id
     * @return True - if current reading position is equal to or bigger than the file size. False - otherwise.
     */
    public boolean isEOF(UUID id)
    {
        Tape tape = this.tapes.get(id);
        if(tape == null)
            throw new NoSuchElementException();

        try(RandomAccessFile raf = new RandomAccessFile(tape.getFile(), "r"))
        {
            if((long) this.tapesCurrentReadBlock.get(id) * this.BLOCK_SIZE >= raf.length())
                return true;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return false;
    }

}

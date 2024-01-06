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

    /**
     * Currently buffered blocks (disk pages) of tapes, with numbers of pages.
     */
    private HashMap<UUID, HashMap<Integer, byte[]>> tapesBufferedBlocks;

    /**
     * Information whether sequential reading reached End of file on each tape.
     */
    private HashMap<UUID, Boolean> isEOF;


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

    /**
     * Returns all tapes IDs that are of a type of index tape except input tape
     * @return
     */
    public Set<UUID> getIndexTapesIDs()
    {
        return this.tapes.keySet().stream()
                .filter(id -> this.isIndexTape(id) && !this.isInputTape(id))
                .collect(Collectors.toSet());
    }

    /**
     * Returns all tapes IDs that are not of a type of index tape except input tape - and hence, since in this project
     * there are only 2 tape types, it returns all the data tapes IDs.
     * @return
     */
    public Set<UUID> getDataTapesIDs()
    {
        return this.tapes.keySet().stream()
                .filter(id -> !this.isIndexTape(id) && !this.isInputTape(id))
                .collect(Collectors.toSet());
    }
    public void create(UUID id, boolean isIndexTape)
    {
        filesUtility.createDirs(Path.of(filesPath));
        File emptyFile = filesUtility.createFile(Path.of(filesPath,filesBaseName +"_"+ id.toString() + ".dat"));
        Tape tape = Tape.builder()
                .id(id)
                .file(emptyFile)
                .isInputTape(false)
                .isIndexTape(isIndexTape)
                .freeSpaceOnEachPage(new ArrayList<>())
                .maxBuffers(0)
                .reads(0)
                .writes(0)
                .build();

        this.tapes.put(tape.getId(), tape);
        this.tapesCurrentReadBlock.put(tape.getId(), 0);
        this.tapesCurrentWriteBlock.put(tape.getId(), 0);
        this.tapesBufferedBlocks.put(tape.getId(), new HashMap<>());
        this.isEOF.put(tape.getId(), false);
    }

    // Special create method, only to create input tape
    public void setInputTape(UUID id, File file)
    {
        Tape inputTape = Tape.builder()
                .id(id)
                .file(file)
                .isInputTape(true)
                .isIndexTape(false)
                .freeSpaceOnEachPage(new ArrayList<>())
                .maxBuffers(0)
                .reads(0)
                .writes(0)
                .build();

        this.tapes.put(inputTape.getId(), inputTape);
        this.tapesCurrentReadBlock.put(inputTape.getId(), 0);
        this.tapesCurrentWriteBlock.put(inputTape.getId(), 0);
        this.tapesBufferedBlocks.put(inputTape.getId(), new HashMap<>());
        this.isEOF.put(inputTape.getId(), false);
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
        this.tapesBufferedBlocks.remove(tape.getId());
        this.isEOF.remove(tape.getId());
    }

    public void delete(UUID id)
    {
        Tape tape = this.tapes.remove(id);
        if(tape == null)
            throw new NoSuchElementException();

        this.tapesCurrentReadBlock.remove(tape.getId(), 0);
        this.tapesCurrentWriteBlock.remove(tape.getId(), 0);
        this.tapesBufferedBlocks.remove(tape.getId());
        this.isEOF.remove(tape.getId());

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
        this.tapesBufferedBlocks.remove(tape.getId());
        this.isEOF.remove(tape.getId());
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
     * <strong>This method is updated from its first version to use (read) buffered blocks, if they are loaded.</strong>
     * @param id
     * @return Byte array with data read from the tape file.
     * (Maybe of size of the {@link TapeService#BLOCK_SIZE} or smaller, if it is the last chunk of data in file)
     */
    // TODO If I ever use removeLastPage(), I should change isEOF() method or the removal method to include also resizing file
    public byte[] readNextBlock(UUID id)
    {
        Tape tape = this.tapes.get(id);
        if(tape == null)
            throw new NoSuchElementException();

        byte[] data = this.readPage(id, tapesCurrentReadBlock.get(tape.getId()));

        if(data == null)
            this.isEOF.put(id, true);

        if(data != null)
            this.tapesCurrentReadBlock.put(tape.getId(), tapesCurrentReadBlock.get(tape.getId()) + 1);

        return data;
    }

    public byte[] readPage(UUID id, int page)
    {
        Tape tape = this.tapes.get(id);
        if(tape == null)
            throw new NoSuchElementException();

        if(page < 0)
            throw new NoSuchElementException("Requested page to read doesn't exist.");

        HashMap<Integer, byte[]> tapeBufferedBlocks = this.tapesBufferedBlocks.get(id);
        if(tapeBufferedBlocks == null)
            throw new NoSuchElementException("Something went wrong. Requested tape exists, but its buffers hashmap" +
                    " hasn't been initialized.");

        byte[] bufferedBlock = tapeBufferedBlocks.get(page);
        byte[] data = null;
        if(bufferedBlock != null)
            data = bufferedBlock;
        else
            data = this.readBlock(id, (long) this.BLOCK_SIZE * page);

        if(page >= this.getPages(id)) {
            if (data != null)
                throw new IllegalStateException("This page shouldn't exist (taking in account the counter), but reading from" +
                        " file returned data (which means End of file hasn't been reached). File is larger than pages count.");
            
            throw new NoSuchElementException("Requested page to read doesn't exist.");
        }

        if(data == null)
            throw new IllegalStateException("This page should exist (taking in account the counter), but reading from" +
                    " file returned null (which means End of file in this method). File is shorter than pages count.");

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
     * <strong>This method is updated from its first version to use (update) buffered blocks, if they are loaded.</strong>
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
        Tape tape = this.tapes.get(id);
        if(tape == null)
            throw new NoSuchElementException();

        boolean written = this.writePage(id, this.tapesCurrentWriteBlock.get(id), data, len);

        if(written)
            this.tapesCurrentWriteBlock.put(tape.getId(), tapesCurrentWriteBlock.get(tape.getId()) + 1);

        return written;
    }

    public boolean writePage(UUID id, int page, byte[] data, int len) throws InvalidAlgorithmParameterException {
        Tape tape = this.tapes.get(id);
        if(tape == null)
            throw new NoSuchElementException();

        if(page < 0 || page >= this.getPages(id))
            throw new NoSuchElementException("Requested page to write doesn't exist.");

        boolean written = this.writeBlock(id, (long) this.BLOCK_SIZE * page, data, len);

        HashMap<Integer, byte[]> tapeBufferedBlocks = this.tapesBufferedBlocks.get(id);
        if(tapeBufferedBlocks == null)
            throw new NoSuchElementException("Something went wrong. Requested tape exists, but its buffers hashmap" +
                    " hasn't been initialized.");

        byte[] bufferedBlock = tapeBufferedBlocks.get(page);
        if(bufferedBlock != null)
        {
            bufferedBlock = data;
            tapeBufferedBlocks.put(page, bufferedBlock);
            this.tapesBufferedBlocks.put(id, tapeBufferedBlocks);
        }

        return written;
    }

    public void resetBlockReading(UUID id)
    {
        Tape tape = this.tapes.get(id);
        if(tape == null)
            throw new NoSuchElementException();

        this.tapesCurrentReadBlock.put(tape.getId(), 0);
        this.isEOF.put(tape.getId(), false);
    }

    public void resetBlockWriting(UUID id)
    {
        Tape tape = this.tapes.get(id);
        if(tape == null)
            throw new NoSuchElementException();

        this.tapesCurrentWriteBlock.put(tape.getId(), 0);
    }

    public void freeBufferedBlock(UUID id, int page)
    {
        Tape tape = this.tapes.get(id);
        if(tape == null)
            throw new NoSuchElementException();

        if(page < 0 || page >= this.getPages(id))
            throw new NoSuchElementException("Requested page to free its buffer doesn't exist.");

        HashMap<Integer, byte[]> tapeBufferedBlocks = this.tapesBufferedBlocks.get(id);
        if(tapeBufferedBlocks == null)
            throw new NoSuchElementException("Something went wrong. Requested tape exists, but its buffers hashmap" +
                    " hasn't been initialized.");

        byte[] bufferedBlock = tapeBufferedBlocks.get(page);
        if(bufferedBlock == null)
            throw new NoSuchElementException("Requested page buffer to free isn't even loaded, so it cannot be freed.");

        tapeBufferedBlocks.remove(page);
        this.tapesBufferedBlocks.put(id, tapeBufferedBlocks);
    }

    /**
     * Returns numbers of pages, which currently are being buffered for this tape.
     * @return
     */
    public Set<Integer> getBufferedPages(UUID id)
    {
        Tape tape = this.tapes.get(id);
        if(tape == null)
            throw new NoSuchElementException();

        HashMap<Integer, byte[]> tapeBufferedBlocks = this.tapesBufferedBlocks.get(id);
        if(tapeBufferedBlocks == null)
            throw new NoSuchElementException("Something went wrong. Requested tape exists, but its buffers hashmap" +
                    " hasn't been initialized.");

        return tapeBufferedBlocks.keySet();
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

    public int getMaxBuffers(UUID id)
    {
        Tape tape = this.tapes.get(id);
        if(tape == null)
            throw new NoSuchElementException();

        return tape.getMaxBuffers();
    }

    public void setMaxBuffers(UUID id, int n)
    {
        Tape tape = this.tapes.get(id);
        if(tape == null)
            throw new NoSuchElementException();

        tape.setMaxBuffers(n);
    }

    /**
     *
     * @param id
     * @param page
     * @return Amount of declared free space on this page or throws exception, if there is no such page.
     */
    public int getFreeSpaceOnPage(UUID id, int page)
    {
        Tape tape = this.tapes.get(id);
        if(tape == null)
            throw new NoSuchElementException();

        if(page < 0)
            throw new IllegalStateException("Provided page parameter was below 0. The page parameter should be" +
                    " equal to or bigger than 0.");

        if(page >= this.getPages(id))
            throw new NoSuchElementException("The page, of which free space was requested to set, doesn't exist.");

        List<Integer> freeSpaces = tape.getFreeSpaceOnEachPage();
        Integer freeSpace = freeSpaces.get(page);
        if(freeSpace == null)
            throw new NoSuchElementException("For some reason, free space amount for this page wasn't initialized.");

        return freeSpace;
    }

    /**
     * This method stores declared free space amount in relation to the requested page. (A Hashmap, where page is key,
     * and free space on it is a value)
     * @param id
     * @param page Count starts from 0.
     * @param amount Can range from 0 to {@link TapeService#BLOCK_SIZE}, which is a size of the disk page.
     */
    public void setFreeSpaceOnPage(UUID id, int page, int amount)
    {
        Tape tape = this.tapes.get(id);
        if(tape == null)
            throw new NoSuchElementException();

        if(page < 0 || amount < 0)
            throw new IllegalStateException("Provided page or amount parameter was below 0. Both parameters should be" +
                    " equal to or bigger than 0.");

        if(amount > this.BLOCK_SIZE)
            throw new IllegalStateException("Declared page free space available was bigger than the page size.");

        if(page >= this.getPages(id))
            throw new NoSuchElementException("The page, of which free space was requested to set, doesn't exist.");

        List<Integer> freeSpaces = tape.getFreeSpaceOnEachPage();
        freeSpaces.set(page, amount);
        tape.setFreeSpaceOnEachPage(freeSpaces);
    }

    /**
     * Get existing pages count.
     * @param id
     * @return
     */
    public int getPages(UUID id)
    {
        Tape tape = this.tapes.get(id);
        if(tape == null)
            throw new NoSuchElementException();

        return tape.getFreeSpaceOnEachPage().size();
    }

    /**
     * New page size is assumed to be the equal to {@link TapeService#BLOCK_SIZE} and as it is a new page, free
     * space is set to its size.
     * @param id
     */
    public void addNextPage(UUID id)
    {
        Tape tape = this.tapes.get(id);
        if(tape == null)
            throw new NoSuchElementException();

        List<Integer> freeSpaces = tape.getFreeSpaceOnEachPage();
        freeSpaces.add(this.BLOCK_SIZE);
        tape.setFreeSpaceOnEachPage(freeSpaces);
    }

    public void removeLastPage(UUID id)
    {
        Tape tape = this.tapes.get(id);
        if(tape == null)
            throw new NoSuchElementException();

        List<Integer> freeSpaces = tape.getFreeSpaceOnEachPage();
        if(freeSpaces.isEmpty())
            throw new NoSuchElementException("There was no more pages to remove.");

        int lastPage = freeSpaces.size() - 1;
        freeSpaces.remove(lastPage);
        tape.setFreeSpaceOnEachPage(freeSpaces);

        // Clean up also the buffer, if the page is loaded, to not store wrong data
        HashMap<Integer, byte[]> tapeBufferedBlocks = this.tapesBufferedBlocks.get(id);
        if(tapeBufferedBlocks != null)
            tapeBufferedBlocks.remove(lastPage);
    }

    // Some boolean check methods

    public boolean isInputTape(UUID id)
    {
        Tape tape = this.tapes.get(id);
        if(tape == null)
            throw new NoSuchElementException();

        return tape.isInputTape();
    }

    public boolean isIndexTape(UUID id)
    {
        Tape tape = this.tapes.get(id);
        if(tape == null)
            throw new NoSuchElementException();

        return tape.isIndexTape();
    }

    public boolean isMaxBuffers(UUID id)
    {
        if(this.getBufferedPages(id).size() > this.getMaxBuffers(id))
            throw new IllegalStateException("There was too many buffered pages (more than max buffers limit for this tape.");

        return this.getBufferedPages(id).size() == this.getMaxBuffers(id);
    }

    /** <strong>(Updated version)</strong>
     * Checks if sequential block reading has already reached end of the tape data file. It just checks boolean
     * variable, which is set in sequential reading methods like readNextBlock() when EOF is reached.
     * (Assumes correct execution of sequential reading - from start to the end of the file. Ignores possibility
     * of skipping blocks while reading.)
     * @param id
     * @return True - if EOF was reached in sequential reading methods. False - otherwise. (Its correctness depends
     * on correct execution of sequential read methods.)
     */
    public boolean isEOF(UUID id)
    {
        Tape tape = this.tapes.get(id);
        if(tape == null)
            throw new NoSuchElementException();

        Boolean isEOF = this.isEOF.get(id);
        if(isEOF == null)
            throw new NoSuchElementException("For some reason, variable to check if End of file was reached " +
                    "(in sequential reading of this tape) hasn't been initialized.");

        return isEOF;
    }

}

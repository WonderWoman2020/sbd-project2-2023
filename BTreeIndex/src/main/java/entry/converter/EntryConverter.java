package entry.converter;

import entry.entity.Entry;

import java.nio.ByteBuffer;

public class EntryConverter {

    /**
     *
     * @param data
     * @return Entry read from bytes. Returning null indicates that there is too little data
     * to read the whole entry (more data needs to be provided).
     */
    public Entry bytesToEntry(byte[] data)
    {
        return this.bytesToEntry(data, 0);
    }

    /**
     *
     * @param data
     * @param off offset in the input byte data array
     * @return Entry read from bytes. Returning null indicates that there is too little data
     * to read the whole entry (more data needs to be provided).
     */
    public Entry bytesToEntry(byte[] data, int off)
    {
        if (data == null)
            return null;

        Entry entry = Entry.builder().build();
        if((data.length - off) < entry.getSize())
            return null;

        entry.setLeftPointer(ByteBuffer.wrap(data, off, 4).getInt());
        entry.setKey(ByteBuffer.wrap(data, off + 4, 8).getLong());
        entry.setDataPage(ByteBuffer.wrap(data, off + 12, 4).getInt());
        entry.setRightPointer(ByteBuffer.wrap(data, off + 16, 4).getInt());
        return entry;
    }

    /**
     *
     * @param entry
     * @return Entry converted to byte array. Returning null indicates that the given entry was null.
     */
    public byte[] entryToBytes(Entry entry)
    {
        if(entry == null)
            return null;

        return ByteBuffer.allocate(entry.getSize())
                .putInt(0, entry.getLeftPointer())
                .putLong(4, entry.getKey())
                .putInt(12,entry.getDataPage())
                .putInt(16, entry.getRightPointer())
                .array();
    }

    /**
     *
     * @param entry
     * @param output byte array buffer, where the entry will be stored
     * @param off offset in byte array, at which the method will start writing the data
     * @return Whether entry conversion to bytes was successful.
     */
    public boolean entryToBytes(Entry entry, byte[] output, int off)
    {
        if(entry == null)
            return false;

        if(output == null || (output.length - off) < entry.getSize())
            return false;

        try {
            ByteBuffer.wrap(output, off, entry.getSize())
                    .putInt(off, entry.getLeftPointer())
                    .putLong(off + 4, entry.getKey())
                    .putInt(off + 12, entry.getDataPage())
                    .putInt(off + 16, entry.getRightPointer());
        } catch (IndexOutOfBoundsException e)
        {
            e.printStackTrace();
            return false;
        }

        return true;
    }
    public boolean isFullEntry(byte[] data, int off)
    {
        if (data == null)
            return false;

        Entry entry = Entry.builder().build();
        if((data.length - off) < entry.getSize())
            return false;

        return true;
    }
}

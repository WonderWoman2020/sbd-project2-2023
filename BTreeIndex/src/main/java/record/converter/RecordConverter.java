package record.converter;

import record.entity.Record;

import java.nio.ByteBuffer;

public class RecordConverter {

    /**
     *
     * @param data
     * @return Record read from bytes. Returning null indicates that there is too little data
     * to read the whole record (more data needs to be provided).
     */
    public Record bytesToRecord(byte[] data)
    {
        return this.bytesToRecord(data, 0);
    }

    /**
     *
     * @param data
     * @param off offset in the input byte data array
     * @return Record read from bytes. Returning null indicates that there is too little data
     * to read the whole record (more data needs to be provided).
     */
    public Record bytesToRecord(byte[] data, int off)
    {
        if (data == null)
            return null;

        Record record = Record.builder().build();
        if((data.length - off) < record.getSize())
            return null;

        record.setKey(ByteBuffer.wrap(data, off, 8).getLong());
        record.setMass(ByteBuffer.wrap(data, off + 8, 4).getInt());
        record.setSpeed(ByteBuffer.wrap(data, off + 12, 4).getInt());
        return record;
    }

    /**
     *
     * @param record
     * @return Record converted to byte array. Returning null indicates that the given record was null.
     */
    public byte[] recordToBytes(Record record)
    {
        if(record == null)
            return null;

        return ByteBuffer.allocate(record.getSize())
                .putLong(0, record.getKey())
                .putInt(8,record.getMass())
                .putInt(12, record.getSpeed())
                .array();
    }

    /**
     *
     * @param record
     * @param output byte array buffer, where the record will be stored
     * @param off offset in byte array, at which the method will start writing the data
     * @return Record converted to byte array. Returning null indicates that the given record was null.
     */
    public boolean recordToBytes(Record record, byte[] output, int off)
    {
        if(record == null)
            return false;

        if(output == null || (output.length - off) < record.getSize())
            return false;

        try {
            ByteBuffer.wrap(output, off, record.getSize())
                    .putLong(off, record.getKey())
                    .putInt(off + 8, record.getMass())
                    .putInt(off + 12, record.getSpeed());
        } catch (IndexOutOfBoundsException e)
        {
            e.printStackTrace();
            return false;
        }

        return true;
    }
    public boolean isFullRecord(byte[] data, int off)
    {
        if (data == null)
            return false;

        Record record = Record.builder().build();
        if((data.length - off) < record.getSize())
            return false;

        return true;
    }

    public String recordToString(Record record)
    {
        if(record == null)
            return null;

        return record.getKey()+" "+record.getMass()+" "+record.getSpeed();
    }

    public Record stringToRecord(String data)
    {
        if (data == null)
            return null;

        String[] recordData = data.split(" ");
        if(recordData.length < 3)
            return null;

        try {
            return Record.builder()
                    .key(Long.parseUnsignedLong(recordData[0]))
                    .mass(Integer.parseInt(recordData[1]))
                    .speed(Integer.parseInt(recordData[2]))
                    .build();
        } catch (NumberFormatException e)
        {
            e.printStackTrace();
            return null;
        }
    }
}

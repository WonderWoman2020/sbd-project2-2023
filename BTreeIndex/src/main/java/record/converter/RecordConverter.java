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

        record.setMass(ByteBuffer.wrap(data, off, 4).getInt());
        record.setSpeed(ByteBuffer.wrap(data, off + 4, 4).getInt());
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
                .putInt(0,record.getMass())
                .putInt(4, record.getSpeed())
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
                    .putInt(off, record.getMass())
                    .putInt(off + 4, record.getSpeed());
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

        return record.getMass()+" "+record.getSpeed();
    }

    public Record stringToRecord(String data)
    {
        if (data == null)
            return null;

        String[] recordData = data.split(" ");
        if(recordData.length < 2)
            return null;

        try {
            return Record.builder()
                    .mass(Integer.parseInt(recordData[0]))
                    .speed(Integer.parseInt(recordData[1]))
                    .build();
        } catch (NumberFormatException e)
        {
            e.printStackTrace();
            return null;
        }
    }
}

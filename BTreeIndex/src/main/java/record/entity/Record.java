package record.entity;

import lombok.*;

@Getter
@Setter
@Builder
@ToString
@EqualsAndHashCode
@AllArgsConstructor
public class Record {

    /**
     * Unique 8-byte key (id) of the record entity.
     */
    private long key;

    /**
     * Mass of an object in kilograms. Data size: 4 bytes.
     */
    private int mass;

    /**
     * Speed of an object in m/s (meters per second). Data size: 4 bytes.
     */
    private int speed;

    /**
     * Calculates how many bytes the record data takes up in memory/file.
     * @return Size of data stored in the record, calculated in bytes.
     */
    public int getSize()
    {
        return (8 + (4*2));
    }

}

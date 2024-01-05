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
        return 4*2;
    }

}

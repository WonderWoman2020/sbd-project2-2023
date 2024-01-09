package entry.entity;

import lombok.*;

@Getter
@Setter
@Builder
@ToString
@EqualsAndHashCode
@AllArgsConstructor
public class Entry {

    /**
     * Record key.
     */
    private long key;

    /**
     * Data tape page number, on which record with key equal to this entry key is stored.
     */
    private int dataPage;

    /**
     * Calculates how many bytes the entry data takes up in memory/file.
     * @return Size of data stored in the entry, calculated in bytes.
     */
    public int getSize()
    {
        return 8 + 4;
    }
}

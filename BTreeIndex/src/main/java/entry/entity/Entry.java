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
     * <strong>Index page number</strong>, on which is stored a b-tree node, that is a child node to the node containing this entry.
     * Each entry has 2 pointers - <strong>the left one points to a child node with all keys lower than this entry key</strong>, and
     * the right one redirects to node, in which all keys are bigger than this entry key.
     */
    private int leftPointer;

    /**
     * Record key.
     */
    private long key;

    /**
     * Data tape page number, on which record with key equal to this entry key is stored.
     */
    private int dataPage;

    /**
     * <strong>Index page number</strong>, on which is stored a b-tree node, that is a child node to the node containing this entry.
     * Each entry has 2 pointers - the left one points to a child node with all keys lower than this entry key, and
     * <strong>the right one redirects to node, in which all keys are bigger than this entry key.</strong>
     */
    private int rightPointer;

    /**
     * Calculates how many bytes the entry data takes up in memory/file.
     * @return Size of data stored in the entry, calculated in bytes.
     */
    public int getSize()
    {
        return ((3*4) + 8);
    }
}

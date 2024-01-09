package node.entity;

import entry.entity.Entry;
import lombok.*;

import java.util.List;

@Getter
@Setter
@Builder
@ToString
@EqualsAndHashCode
@AllArgsConstructor
public class Node {

    // Not stored on disk data, set by program
    /**
     * <strong>Index page pointer (a pointer is page number + 1, to exclude 0 value as special and meaning null pointer)</strong>,
     * on which is stored this b-tree node. As each node takes up one page, it can be treated as a node ID.
     * <strong>Pointer to self doesn't have to be stored in node header on disk, because its value is page number (+1), from
     * which it is being read.</strong>
     */
    private int selfPointer;

    // Data stored on disk

    /**
     * <strong>Index page pointer (a pointer is page number + 1, to exclude 0 value as special and meaning null pointer)</strong>,
     * on which is stored a b-tree node, that is a parent node to this node.
     */
    private int parentPointer;

    /**
     * All entries stored in this node.
     */
    private List<Entry> entries;

    /**
     * All index page pointers to children of this b-tree node. <strong>Index page pointer - a pointer is page number + 1,
     * to exclude 0 value as special and meaning null pointer.</strong>
     */
    private List<Integer> childPointers;

    /**
     * Calculates how many bytes the node data takes up in memory/file. <strong>Note: selfPointer value isn't stored.</strong>
     * @return Size of data stored in the node, calculated in bytes.
     */
    public int getSize()
    {
        return 4 + entries.stream().mapToInt(Entry::getSize).sum() + childPointers.size()*4;
    }

}

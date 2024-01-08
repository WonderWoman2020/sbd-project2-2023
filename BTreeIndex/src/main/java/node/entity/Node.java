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

    /**
     * <strong>Index page number</strong>, on which is stored a b-tree node, that is a parent node to this node.
     */
    private int parentPointer;

    /**
     * <strong>Index page number</strong>, on which is stored this b-tree node.
     */
    private int page;

    /**
     * Level in the b-tree structure, on which this node is placed.
     */
    private int level;

    /**
     * All entries stored in this node.
     */
    private List<Entry> entries;

    /**
     * Calculates how many bytes the node data takes up in memory/file.
     * @return Size of data stored in the node, calculated in bytes.
     */
    public int getSize()
    {
        return (3*4) + entries.stream().mapToInt(Entry::getSize).sum();
    }

}

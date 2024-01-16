package node.converter;

import entry.converter.EntryConverter;
import entry.entity.Entry;
import lombok.AllArgsConstructor;
import node.entity.Node;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@AllArgsConstructor
public class NodeConverter {

    private EntryConverter entryConverter;

    /**
     *
     * @param data
     * @return Node read from bytes. Returning null indicates that there is too little data
     * to read the whole node (more data needs to be provided).
     */
    public Node bytesToNode(byte[] data)
    {
        return this.bytesToNode(data, 0);
    }

    /**
     *
     * @param data
     * @param off offset in the input byte data array
     * @return Node read from bytes. Returning null indicates that there is too little data
     * to read the whole node (more data needs to be provided), or size of the data can't be a correct node size
     * (a multiple of entries and child pointers minus header and first child pointer).
     */
    public Node bytesToNode(byte[] data, int off)
    {
        if (data == null)
            return null;

        if(!this.isFullNode(data, off))
            return null;

        List<Entry> entries = new ArrayList<>();
        List<Integer> childPointers = new ArrayList<>();
        int parentPointer = ByteBuffer.wrap(data, off, 4).getInt();
        int consumed = 4;
        while(consumed < (data.length - off))
        {
            if((data.length - off - consumed) < 4)
                break;
            int child = ByteBuffer.wrap(data, off + consumed, 4).getInt();
            consumed += 4;
            childPointers.add(child);

            if((data.length - off - consumed) < Entry.builder().build().getSize())
                break;
            Entry entry = entryConverter.bytesToEntry(data, off + consumed);
            consumed += entry.getSize();
            if(entry.getKey() == 0) // Record key can't be 0, so it is assumed to not be an entry, but just an empty space for it
                continue;
            entries.add(entry);
        }
        if(data.length - off != consumed)
            throw new IllegalStateException("Converting a node from bytes ended, but read bytes counter doesn't match" +
                    " provided array size. Provided array length (minus offset) should match a size of a possible node.");
        Node node = Node.builder().build();
        node.setParentPointer(parentPointer);
        node.setSelfPointer(0);
        node.setEntries(entries);
        node.setChildPointers(childPointers);
        return node;
    }

    /**
     *
     * @param node
     * @return Node converted to byte array. Returning null indicates that the given node was null.
     */
    public byte[] nodeToBytes(Node node)
    {
        if(node == null)
            return null;

        byte[] output = new byte[node.getSize()];
        this.nodeToBytes(node, output, 0);
        return output;
    }

    /**
     *
     * @param node
     * @param output byte array buffer, where the node will be stored
     * @param off offset in byte array, at which the method will start writing the data
     * @return Whether node conversion to bytes was successful.
     */
    public boolean nodeToBytes(Node node, byte[] output, int off)
    {
        if(node == null)
            return false;

        if(output == null || (output.length - off) < node.getSize())
            return false;

        if(node.getChildPointers().isEmpty())
            return false;

        try {
            ByteBuffer byteBuffer = ByteBuffer.wrap(output, off, node.getSize());
            byteBuffer.putInt(off, node.getParentPointer());
            byteBuffer.putInt(off + 4, node.getChildPointers().get(0));
            int consumed = 8;
            for(int i = 0; i < node.getChildPointers().size() - 1; i++) {
                if(i < node.getEntries().size())
                    entryConverter.entryToBytes(node.getEntries().get(i), output, off + consumed);
                else
                    Arrays.fill(output, consumed, consumed + Entry.builder().build().getSize(), (byte) 0);
                consumed += Entry.builder().build().getSize();
                byteBuffer.putInt(off + consumed, node.getChildPointers().get(i+1));
                consumed += 4;
            }
        } catch (IndexOutOfBoundsException e)
        {
            e.printStackTrace();
            return false;
        }

        return true;
    }
    public boolean isFullNode(byte[] data, int off)
    {
        if (data == null)
            return false;

        /* 8 - 4-byte parent pointer + 4 byte first child pointer,
          rest of the node is n * (constant size of Entry + 4 for child pointer).
          So data array size (minus first 8 bytes) will always be a multiple of (entry + child pointer) size
         */
        if((data.length - off - 8) % (Entry.builder().build().getSize() + 4) != 0)
            return false;

        return true;
    }

    public String nodeToString(Node node)
    {
        if(node == null)
            return null;

        if(node.getChildPointers().isEmpty())
            return null;

        StringBuilder nodeData = new StringBuilder();
        nodeData.append("H ");
        nodeData.append(node.getParentPointer());
        nodeData.append(" H |");
        nodeData.append(node.getChildPointers().get(0));
        nodeData.append("|");
        for(int i = 0; i<node.getChildPointers().size() - 1; i++)
        {
            nodeData.append(" ");
            nodeData.append(i < node.getEntries().size() ? node.getEntries().get(i).getKey() : 0);
            nodeData.append(" ");
            nodeData.append(i < node.getEntries().size() ? node.getEntries().get(i).getDataPage() : 0);
            nodeData.append(" |");
            nodeData.append(node.getChildPointers().get(i+1));
            nodeData.append("|");
        }
        return nodeData.toString();
    }
}

package database.service;

import btree.service.BTreeService;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import node.converter.NodeConverter;
import node.entity.Node;
import record.converter.RecordConverter;
import record.entity.Record;
import tape.service.TapeService;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Builder
@ToString
@AllArgsConstructor
public class DatabaseRawReader {

    private TapeService tapeService;

    private RecordConverter recordConverter;

    private NodeConverter nodeConverter;

    @Getter
    private UUID dataTapeID;

    @Getter
    private UUID indexTapeID;

    public void readDataPage(int page)
    {
        if(page < 0)
            throw new NoSuchElementException("Requested page was below 0 - that page doesn't exist.");

        this.assureBufferForPage(dataTapeID, page);
        byte[] buffer = tapeService.readPage(dataTapeID, page);
        System.out.println("---------------------------------- Page nr "+page+" ----------------------------------");
        int consumed = 0;
        while(consumed < buffer.length) {
            Record record = recordConverter.bytesToRecord(buffer, consumed);
            if(record.getKey() == 0) {
                for(int i=0; i< record.getSize(); i++)
                    System.out.format("0x%02x ", buffer[consumed+i]);
                System.out.println();
            }
            else
                System.out.println(record);
            consumed += record.getSize();
            if(consumed + record.getSize() > buffer.length)
                break;
        }
        if(consumed < buffer.length) {
            for (int i = 0; i < buffer.length - consumed; i++)
                System.out.format("0x%02x ", buffer[consumed + i]);
            System.out.println();
        }
        System.out.println("------------------------------- End of page nr "+page+" ------------------------------");
    }

    public void readData()
    {
        int page = 0;

        if(tapeService.getPages(dataTapeID) == 0)
        {
            System.out.println("Data file has no pages to read yet.");
            return;
        }

        while(page < tapeService.getPages(dataTapeID)) {
            this.readDataPage(page);
            page++;
        }
    }

    public void readNextNode(int nodePointer, int level, List<Integer> readPages)
    {
        if(nodePointer == 0)
            return;

        if(this.pointerToPage(nodePointer) < 0)
            throw new NoSuchElementException("Requested page was below 0 - that page doesn't exist.");

        this.assureBufferForPage(indexTapeID, this.pointerToPage(nodePointer));
        byte[] buffer = tapeService.readPage(indexTapeID, this.pointerToPage(nodePointer));
        readPages.add(this.pointerToPage(nodePointer));
        Node node = nodeConverter.bytesToNode(buffer);
        String levelIndentation = " ".repeat(level*8);
        String nodeDescription = "Lvl: "+level+", Page: "+this.pointerToPage(nodePointer)+", Node: "+nodePointer+" => ";
        String nodeData = nodeConverter.nodeToString(node);
        System.out.println(levelIndentation + nodeDescription + nodeData);
        for(int i = 0; i < node.getChildPointers().size(); i++)
            this.readNextNode(node.getChildPointers().get(i), level + 1, readPages);
    }

    public void readIndex()
    {
        int rootPage = this.findRootPage();
        if(rootPage == -1) {
            System.out.println("Root page of the database index file wasn't found. There are probably no entries in the database yet.");
            return;
        }
        List<Integer> readPages = new ArrayList<>();
        System.out.println("********************************** B-tree index **********************************");
        this.readNextNode(this.pageToPointer(rootPage), 0, readPages);
        System.out.println("******************************* End of B-tree index ******************************");
        List<Integer> allPages = IntStream.range(0, tapeService.getPages(indexTapeID)).boxed().collect(Collectors.toList());
        allPages.removeAll(readPages);
        if(allPages.isEmpty())
            System.out.println("There were no empty pages in index file.");
        else
            System.out.println("Index file pages "+ allPages.stream().map(Object::toString).collect(Collectors.joining(", "))+" were empty.");
    }

    private int findRootPage()
    {
        int p = 0;
        while(p < tapeService.getPages(indexTapeID))
        {
            this.assureBufferForPage(indexTapeID, p);
            byte[] buffer = tapeService.readPage(indexTapeID, p);
            Node node = nodeConverter.bytesToNode(buffer);
            if(node.getParentPointer() == 0 && !node.getEntries().isEmpty())
                return p;
            p++;
        }
        return -1;
    }

    private int pageToPointer(int page)
    {
        return page + 1;
    }
    private int pointerToPage(int pointer)
    {
        return pointer - 1;
    }
    private int choosePageToFree(UUID tapeID, int pageToLoad)
    {
        Set<Integer> bufferedPages = tapeService.getBufferedPages(tapeID);
        if(bufferedPages.isEmpty())
            throw new IllegalStateException("There was no buffered pages for this tape. There was no page to choose to be freed.");

        if(bufferedPages.contains(null))
            throw new IllegalStateException("Something went wrong in maintaining buffered pages numbers set" +
                    " - it contained a null value.");

        Optional<Integer> furthestPage = bufferedPages.stream()
                .max(Comparator.comparingInt(page -> Math.abs(page - pageToLoad)));

        return furthestPage.get();
    }

    /**
     * It assures that there is a space to read a new page, if it isn't already loaded.
     * @param tapeID
     * @param page The page may not exist (it may be one that is being created just now), it just frees a buffer if needed.
     *             Page parameter is for algorithm of choosing which buffer to free, to take it into account.
     */
    private void assureBufferForPage(UUID tapeID, int page)
    {
        if(!tapeService.getBufferedPages(tapeID).contains(page))
        {
            if(tapeService.isMaxBuffers(tapeID)) {
                int pageToFree = this.choosePageToFree(tapeID, page);
                tapeService.freeBufferedBlock(tapeID, pageToFree);
            }
        }
    }

}

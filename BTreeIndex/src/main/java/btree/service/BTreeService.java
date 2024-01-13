package btree.service;


import entry.entity.Entry;
import entry.service.EntryService;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.ToString;

import java.security.InvalidAlgorithmParameterException;
import java.util.*;

@Builder
@ToString
@AllArgsConstructor
public class BTreeService {

    private EntryService entryService;

    /**
     * B-tree degree - how many entries is a minimal amount for a node (2*d is the maximum amount on the other hand).
     */
    private final int d;

    /**
     * Current b-tree height.
     */
    private int h;

    /**
     * Current index page, which contains root node.
     */
    private int rootPage;

    /**
     * Saves node pointer that was checked as the last in findEntryInSubtree() method
     */
    private int lastSearchedNode;

    public void createEntry(UUID tapeID, Entry entry) throws InvalidAlgorithmParameterException {
        if(entryService.getTapePages(tapeID) == 0) // Add first index page, if it doesn't have any yet
        {
            this.assureBufferForPage(tapeID, entryService.getTapePages(tapeID));
            entryService.addNextPage(tapeID);
            entryService.setFreeSpaceOnPage(tapeID, 0, 0); // Make this page taken by the first node
            entryService.setNodeParentPointer(tapeID, 0, this.pageToPointer(0));
            this.rootPage = 0;
        }

        Entry existingEntry = this.findEntry(tapeID, entry.getKey());
        if(existingEntry != null)
        {
            System.out.println("Entry with provided key already exists. Creation of new entry hasn't succeeded.");
            return;
        }

        // Insert on current page
        int insertionNodePointer = this.lastSearchedNode;
        if(entryService.getNodeEntries(tapeID, this.pointerToPage(insertionNodePointer)) < (2 * this.d))
        {
            this.insertEntry(tapeID, insertionNodePointer, entry);
            return;
        }

        // Try compensation
        int whichSibling = this.whichSiblingForCompensation(tapeID, insertionNodePointer);
        if(whichSibling != -1)
        {
            this.compensate(tapeID, insertionNodePointer, whichSibling);
            return;
        }

        // Split

    }

    public Entry findEntry(UUID tapeID, long key)
    {
        return this.findEntryInSubtree(tapeID, this.rootPage + 1, key);
    }

    public Entry findEntryInSubtree(UUID tapeID, int nodePointer, long key)
    {
        if(nodePointer == 0)
            return null;

        if(this.pointerToPage(nodePointer) < 0 || this.pointerToPage(nodePointer) >= entryService.getTapePages(tapeID))
            throw new IllegalStateException("Page requested to find a node in it doesn't exist.");

        // Saving for other methods to know, which was the last searched node
        this.lastSearchedNode = nodePointer;

        this.assureBufferForPage(tapeID, this.pointerToPage(nodePointer));
        if(entryService.getNodeEntries(tapeID, this.pointerToPage(nodePointer)) == 0) // There are no entries yet (possible only with first ever entry)
            return null;

        int entryNumber = entryService.findEntryNumber(tapeID, this.pointerToPage(nodePointer), key);
        if(entryNumber != -1)
            return entryService.readEntry(tapeID, this.pointerToPage(nodePointer), entryNumber);

        Entry minEntry = entryService.readEntry(tapeID, this.pointerToPage(nodePointer), 0);
        if(key < minEntry.getKey()) {
            int leftChildPointer = entryService.readNodePointer(tapeID, this.pointerToPage(nodePointer), 0);
            return this.findEntryInSubtree(tapeID, leftChildPointer, key);
        }

        int lastEntryNumber = entryService.getNodeEntries(tapeID, this.pointerToPage(nodePointer)) - 1;
        Entry maxEntry = entryService.readEntry(tapeID, this.pointerToPage(nodePointer), lastEntryNumber);
        if(key > maxEntry.getKey()) {
            int rightChildPointer = entryService.readNodePointer(tapeID, this.pointerToPage(nodePointer), lastEntryNumber + 1);
            return this.findEntryInSubtree(tapeID, rightChildPointer, key);
        }

        int firstBiggerEntryNumber = this.findFirstBiggerEntryNumber(tapeID, nodePointer, key);
        if(firstBiggerEntryNumber == -1)
            throw new IllegalStateException("Something went wrong. The requested key was between this node min and max key," +
                    " so bigger key than provided one should be found, but it wasn't.");
        int leftChildPointer = entryService.readNodePointer(tapeID, this.pointerToPage(nodePointer), firstBiggerEntryNumber);
        return this.findEntryInSubtree(tapeID, leftChildPointer, key);
    }

    public void updateEntry(UUID tapeID, Entry entry)
    {

    }

    public void deleteEntry(UUID tapeID, long key)
    {

    }

    private void merge(UUID tapeID, int nodePointer1, int nodePointer2)
    {

    }

    private void split(UUID tapeID, int nodePointer1, int nodePointer2)
    {

    }

    private void compensate(UUID tapeID, int nodePointer, int whichSibling)
    {
        this.assureBufferForPage(tapeID, this.pointerToPage(nodePointer));
        int parentPointer = entryService.readNodeParentPointer(tapeID, this.pointerToPage(nodePointer)); // both nodes should have the same parent
        if(parentPointer == 0)
            throw new IllegalStateException("If compensation was confirmed as possible, then a parent node of provided" +
                    " nodes should exist, but it didn't.");

        List<Entry> nodeEntries = this.readAllNodeEntries(tapeID, nodePointer);
        List<Integer> nodePointers = this.readAllNodePointers(tapeID, nodePointer);

        this.assureBufferForPage(tapeID, this.pointerToPage(parentPointer));
        int childPointerNumber = entryService.findNodePointerNumber(tapeID, this.pointerToPage(parentPointer), nodePointer);
        Entry parentEntry;
        int siblingPointer = 0;
        if(whichSibling == 1) { // Sibling is the left node
            parentEntry = entryService.readEntry(tapeID, this.pointerToPage(parentPointer), childPointerNumber - 1);
            siblingPointer = entryService.readNodePointer(tapeID, this.pointerToPage(parentPointer), childPointerNumber - 1);
        }
        else { // Sibling is the right node
            parentEntry = entryService.readEntry(tapeID, this.pointerToPage(parentPointer), childPointerNumber);
            siblingPointer = entryService.readNodePointer(tapeID, this.pointerToPage(parentPointer), childPointerNumber + 1);
        }

        this.assureBufferForPage(tapeID, this.pointerToPage(siblingPointer));
        List<Entry> siblingEntries = this.readAllNodeEntries(tapeID, siblingPointer);
        List<Integer> siblingPointers = this.readAllNodePointers(tapeID, siblingPointer);

        List<Entry> allEntries = new ArrayList<>();
        List<Integer> allPointers = new ArrayList<>();

        allEntries.addAll((whichSibling == 1) ? siblingEntries : nodeEntries);
        allEntries.add(parentEntry);
        allEntries.addAll((whichSibling == 1) ? nodeEntries : siblingEntries);
        allPointers.addAll((whichSibling == 1) ? siblingPointers : nodePointers);
        allPointers.addAll((whichSibling == 1) ? nodePointers : siblingPointers);

        int middleEntryNumber = allEntries.size() / 2;
        // TODO distribute the entries and nodes

    }

    private List<Entry> readAllNodeEntries(UUID tapeID, int nodePointer)
    {
        this.assureBufferForPage(tapeID, this.pointerToPage(nodePointer));
        List<Entry> entries = new ArrayList<>();
        int n = 0;
        while(n < entryService.getNodeEntries(tapeID, this.pointerToPage(nodePointer)))
        {
            Entry readEntry = entryService.readEntry(tapeID, this.pointerToPage(nodePointer), n);
            entries.add(readEntry);
            n++;
        }
        return entries;
    }

    private List<Integer> readAllNodePointers(UUID tapeID, int nodePointer)
    {
        this.assureBufferForPage(tapeID, this.pointerToPage(nodePointer));
        List<Integer> pointers = new ArrayList<>();
        int n = 0;
        while(n < entryService.getNodePointers(tapeID, this.pointerToPage(nodePointer)))
        {
            int readPointer = entryService.readNodePointer(tapeID, this.pointerToPage(nodePointer), n);
            pointers.add(readPointer);
            n++;
        }
        return pointers;
    }

    /**
     *
     * @param tapeID
     * @param nodePointer
     * @return 1 - if left sibling can be used,<br></br> 2 - if right sibling can be used,
     * <br></br> -1 - if no sibling can be used
     */
    private int whichSiblingForCompensation(UUID tapeID, int nodePointer)
    {
        this.assureBufferForPage(tapeID, this.pointerToPage(nodePointer));
        int parentPointer = entryService.readNodeParentPointer(tapeID, this.pointerToPage(nodePointer));
        if(parentPointer != 0) // If a node doesn't have a parent, then it is root, and it doesn't have siblings to compensate
        {
            this.assureBufferForPage(tapeID, this.pointerToPage(parentPointer));
            int childPointerNumber = entryService.findNodePointerNumber(tapeID, this.pointerToPage(parentPointer), nodePointer);
            if(childPointerNumber == -1)
                throw new IllegalStateException("This node should be a parent of some child node (according to the child header)," +
                        " but it didn't contain a pointer equal to the child pointer.");

            int leftSiblingPointer = 0;
            int rightSiblingPointer = 0;
            if(childPointerNumber > 0) // Can have left sibling
                leftSiblingPointer = entryService.readNodePointer(tapeID, this.pointerToPage(parentPointer), childPointerNumber - 1);

            if(childPointerNumber < entryService.getNodePointers(tapeID, this.pointerToPage(parentPointer)) - 1) // Can have right sibling
                rightSiblingPointer = entryService.readNodePointer(tapeID, this.pointerToPage(parentPointer), childPointerNumber + 1);

            if(leftSiblingPointer != 0) {
                this.assureBufferForPage(tapeID, this.pointerToPage(leftSiblingPointer));
                if(entryService.getNodeEntries(tapeID, this.pointerToPage(leftSiblingPointer)) < (2 * this.d))
                    return 1;
            }
            if(rightSiblingPointer != 0)
            {
                this.assureBufferForPage(tapeID, this.pointerToPage(rightSiblingPointer));
                if(entryService.getNodeEntries(tapeID, this.pointerToPage(rightSiblingPointer)) < (2 * this.d))
                    return 2;
            }
        }
        return -1;
    }

    private void insertEntry(UUID tapeID, int nodePointer, Entry entry) throws InvalidAlgorithmParameterException {
        if(nodePointer == 0)
            throw new IllegalStateException("Node pointer, to which was requested to insert an entry, was 0.");

        this.assureBufferForPage(tapeID, this.pointerToPage(nodePointer));
        if(entryService.getNodeEntries(tapeID, this.pointerToPage(nodePointer)) >= (2 * this.d))
            throw new IllegalStateException("Entry can't be inserted into a node, which is full of entries already.");

        // Read all entries from the leaf node. As we should be in a leaf, node pointers can be omitted,
        // because they should be null
        List<Entry> entries = this.readAllNodeEntries(tapeID, nodePointer);
        Optional<Entry> firstBiggerEntry = entries.stream()
                .filter(nodeEntry -> nodeEntry.getKey() > entry.getKey())
                .min(Comparator.comparingLong(Entry::getKey));
        if(firstBiggerEntry.isEmpty()) // There was no bigger key in this node, so this is the new biggest key entry
        {
            int newLastEntryNumber = entryService.getNodeEntries(tapeID, this.pointerToPage(nodePointer));
            entryService.writeEntry(tapeID, this.pointerToPage(nodePointer), newLastEntryNumber, entry);
        }
        else
        {
            int biggerEntryNumber = entries.indexOf(firstBiggerEntry.get());
            entries.add(biggerEntryNumber, entry);
            for(int i = 0; i < entries.size(); i++) // Rewrite all node entries, so they will have order as in the list
                entryService.writeEntry(tapeID, this.pointerToPage(nodePointer), i, entries.get(i));
        }
        entryService.saveNode(tapeID, this.pointerToPage(nodePointer));
    }

    /**
     *
     * @param tapeID
     * @param nodePointer
     * @param key
     * @return First entry number, which had a bigger key than provided, or -1, if there was no bigger entry key in this node
     */
    private int findFirstBiggerEntryNumber(UUID tapeID, int nodePointer, long key)
    {
        if(nodePointer == 0)
            throw new IllegalStateException("Node pointer, in which entries were requested to be compared with provided key, was 0.");

        this.assureBufferForPage(tapeID, this.pointerToPage(nodePointer));
        int n = 0;
        while(n < entryService.getNodeEntries(tapeID, this.pointerToPage(nodePointer)))
        {
            Entry entry = entryService.readEntry(tapeID, this.pointerToPage(nodePointer), n);
            if(key < entry.getKey())
                return n;
            n++;
        }
        return -1;
    }


    /**
     * Map page to node pointer. Adds 1, so pointer of value 0 couldn't exist and the value can be used as null pointer value.
     * @param page
     * @return
     */
    private int pageToPointer(int page)
    {
        return page + 1;
    }

    /**
     * Map node pointer to page. Does the exact opposite to {@link BTreeService#pageToPointer} method (Decreases value by 1).
     * @param pointer
     * @return
     */
    private int pointerToPage(int pointer)
    {
        return pointer - 1;
    }



    /**
     * Searches array of amounts of free space on each page of tape. Each tape has that array, and it needs to be updated.
     * @param tapeID
     * @return Returns page nr with enough space to write the node on it, or -1, if all pages
     * are full (or there is none yet) and new page needs to be added to tape.
     */
    private int findSpaceForNode(UUID tapeID)
    {
        int page = 0;
        while(page < entryService.getTapePages(tapeID))
        {
            if(entryService.getFreeSpaceOnPage(tapeID, page) != 0) // Each node takes up full page, so it can be only full or empty
                return page;

            page++;
        }
        return -1;
    }

    /**
     * Method of choosing which buffer should be freed. Takes into account page number, which will be loaded next.
     * There will always be chosen some page to free (the most optimal from currently buffered), unless there are no pages buffered.
     * @param tapeID
     * @param pageToLoad
     * @return Number of the buffered page, which lies in the tape furthest from the next page to load.
     */
    private int choosePageToFree(UUID tapeID, int pageToLoad)
    {
        Set<Integer> bufferedPages = entryService.getBufferedPages(tapeID);
        bufferedPages.remove(this.rootPage); // Never free root page
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
        if(!entryService.getBufferedPages(tapeID).contains(page))
        {
            if(entryService.isTapeMaxBuffers(tapeID)) {
                int pageToFree = this.choosePageToFree(tapeID, page);
                entryService.freeBufferedBlock(tapeID, pageToFree);
            }
        }
    }
}

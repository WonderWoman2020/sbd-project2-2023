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
            this.insertEntry(tapeID, insertionNodePointer, entry, 0); // TODO pass here some pointer from createEntry()
            return;
        }

        // Try compensation
        List<Integer> siblingsPointers = this.getSiblingsPointers(tapeID, insertionNodePointer);
        if(siblingsPointers != null)
        {
            if(this.canNodeCompensate(tapeID, siblingsPointers.get(0))) {
                this.compensate(tapeID, insertionNodePointer, siblingsPointers.get(0), true);
                return;
            }
            if(this.canNodeCompensate(tapeID, siblingsPointers.get(1))) {
                this.compensate(tapeID, insertionNodePointer, siblingsPointers.get(1), false);
                return;
            }
            if(siblingsPointers.get(0) == 0 && siblingsPointers.get(1) == 0)
                throw new IllegalStateException("Something went wrong. This node has a parent, but it doesn't have any siblings," +
                    " which shouldn't happen (there should be always at least 1 sibling).");
        }
        // TODO Split here

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

    private void compensate(UUID tapeID, int nodePointer, int siblingPointer, boolean leftSibling) throws InvalidAlgorithmParameterException {
        // Read parent node pointer
        this.assureBufferForPage(tapeID, this.pointerToPage(siblingPointer));
        int parentPointer = entryService.readNodeParentPointer(tapeID, this.pointerToPage(siblingPointer)); // both nodes should have the same parent
        if(parentPointer == 0)
            throw new IllegalStateException("If compensation was confirmed as possible, then a parent node of provided" +
                    " nodes should exist, but it didn't.");

        // Read all node entries and pointers from sibling
        List<Entry> siblingEntries = this.readAllNodeEntries(tapeID, siblingPointer);
        List<Integer> siblingPointers = this.readAllNodePointers(tapeID, siblingPointer);

        // Read a parent node entry, which is between the nodePointer and its sibling pointer
        this.assureBufferForPage(tapeID, this.pointerToPage(parentPointer));
        int nodePointerNumber = entryService.findNodePointerNumber(tapeID, this.pointerToPage(parentPointer), nodePointer);
        int parentEntryNumber = leftSibling ? nodePointerNumber - 1 : nodePointerNumber;
        Entry parentEntry = entryService.readEntry(tapeID, this.pointerToPage(parentPointer), parentEntryNumber);

        // Read all node entries and pointers from the insertion node
        this.assureBufferForPage(tapeID, this.pointerToPage(nodePointer));
        List<Entry> nodeEntries = this.readAllNodeEntries(tapeID, nodePointer);
        List<Integer> nodePointers = this.readAllNodePointers(tapeID, nodePointer);

        // Get all entries and node pointers together, without changing their order which they have in nodes,
        // and distribute them equally between nodes
        List<Entry> allEntries = new ArrayList<>();
        List<Integer> allPointers = new ArrayList<>();
        allEntries.addAll(leftSibling ? siblingEntries : nodeEntries);
        allEntries.add(parentEntry);
        allEntries.addAll(leftSibling ? nodeEntries : siblingEntries);
        allPointers.addAll(leftSibling ? siblingPointers : nodePointers);
        allPointers.addAll(leftSibling ? nodePointers : siblingPointers);

        int middleEntryNumber = allEntries.size() / 2;

        // Distribution in left node
        int leftChildPointer = leftSibling ? siblingPointer : nodePointer;
        this.assureBufferForPage(tapeID, this.pointerToPage(leftChildPointer));
        for(int i = 0; i < middleEntryNumber; i++)
            entryService.writeEntry(tapeID, this.pointerToPage(leftChildPointer), i, allEntries.get(i));
        for(int i = 0; i < middleEntryNumber + 1; i++)
            entryService.setNodePointer(tapeID, this.pointerToPage(leftChildPointer), i, allPointers.get(i));
        entryService.saveNode(tapeID, leftChildPointer);

        // Set parent entry (without modifying pointers)
        this.assureBufferForPage(tapeID, this.pointerToPage(parentPointer));
        entryService.writeEntry(tapeID, this.pointerToPage(parentPointer), parentEntryNumber, allEntries.get(middleEntryNumber));
        entryService.saveNode(tapeID, parentPointer);

        // Distribution in right node
        int rightChildPointer = leftSibling ? nodePointer : siblingPointer;
        this.assureBufferForPage(tapeID, this.pointerToPage(rightChildPointer));
        for(int i = middleEntryNumber + 1; i < allEntries.size() ; i++)
            entryService.writeEntry(tapeID, this.pointerToPage(rightChildPointer), i - (middleEntryNumber + 1), allEntries.get(i));
        for(int i = middleEntryNumber + 1; i < allPointers.size(); i++)
            entryService.setNodePointer(tapeID, this.pointerToPage(rightChildPointer), i - (middleEntryNumber + 1), allPointers.get(i));
        entryService.saveNode(tapeID, rightChildPointer);
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

    private boolean canNodeCompensate(UUID tapeID, int nodePointer)
    {
        if(nodePointer != 0) {
            this.assureBufferForPage(tapeID, this.pointerToPage(nodePointer));
            if(entryService.getNodeEntries(tapeID, this.pointerToPage(nodePointer)) < (2 * this.d))
                return true;
        }
        return false;
    }

    private List<Integer> getSiblingsPointers(UUID tapeID, int nodePointer)
    {
        this.assureBufferForPage(tapeID, this.pointerToPage(nodePointer));
        int parentPointer = entryService.readNodeParentPointer(tapeID, this.pointerToPage(nodePointer));
        if(parentPointer != 0) // If a node doesn't have a parent, then it is root, and it doesn't have siblings
        {
            this.assureBufferForPage(tapeID, this.pointerToPage(parentPointer));
            int childPointerNumber = entryService.findNodePointerNumber(tapeID, this.pointerToPage(parentPointer), nodePointer);
            if (childPointerNumber == -1)
                throw new IllegalStateException("This node should be a parent of some child node (according to the child header)," +
                        " but it didn't contain a pointer equal to the child pointer.");

            int leftSiblingPointer = 0;
            int rightSiblingPointer = 0;
            if (childPointerNumber > 0) // Can have left sibling
                leftSiblingPointer = entryService.readNodePointer(tapeID, this.pointerToPage(parentPointer), childPointerNumber - 1);

            if(childPointerNumber < entryService.getNodePointers(tapeID, this.pointerToPage(parentPointer)) - 1) // Can have right sibling
                rightSiblingPointer = entryService.readNodePointer(tapeID, this.pointerToPage(parentPointer), childPointerNumber + 1);

            List<Integer> siblingsPointers = new ArrayList<>();
            siblingsPointers.add(leftSiblingPointer);
            siblingsPointers.add(rightSiblingPointer);
            return siblingsPointers;
        }
        return null;
    }

    private void insertEntry(UUID tapeID, int nodePointer, Entry entry, int rightPointer) throws InvalidAlgorithmParameterException {
        if(nodePointer == 0)
            throw new IllegalStateException("Node pointer, to which was requested to insert an entry, was 0.");

        if(entry == null)
            throw new IllegalStateException("Entry provided to insert was null.");

        this.assureBufferForPage(tapeID, this.pointerToPage(nodePointer));
        if(entryService.getNodeEntries(tapeID, this.pointerToPage(nodePointer)) >= (2 * this.d))
            throw new IllegalStateException("Entry can't be inserted into a node, which is full of entries already.");

        // Read all entries and pointers from the node
        List<Entry> entries = this.readAllNodeEntries(tapeID, nodePointer);
        List<Integer> pointers = this.readAllNodePointers(tapeID, nodePointer);
        int firstBiggerEntryNumber = this.findFirstBiggerEntryNumber(tapeID, nodePointer, entry.getKey());
        int insertionEntryNumber;
        if(firstBiggerEntryNumber == -1) // There was no bigger key in this node, so this is the new biggest key entry
            insertionEntryNumber = entries.size();
        else
            insertionEntryNumber = firstBiggerEntryNumber;

        entries.add(insertionEntryNumber, entry);
        pointers.add(insertionEntryNumber + 1, rightPointer);
        // Rewrite all node entries and pointers, so they will be ordered in the node as in the list
        for(int i = 0; i < entries.size(); i++)
            entryService.writeEntry(tapeID, this.pointerToPage(nodePointer), i, entries.get(i));
        for(int i = 0; i < pointers.size(); i++)
            entryService.setNodePointer(tapeID, this.pointerToPage(nodePointer), i, pointers.get(i));
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

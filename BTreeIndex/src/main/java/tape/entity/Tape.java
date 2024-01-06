package tape.entity;

import lombok.*;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

@Getter
@Setter
@Builder
@ToString
@EqualsAndHashCode
@AllArgsConstructor
public class Tape {

    // Static data

    /**
     * Form of representation of the physical tape - a file saved on computer
     */
    private File file;

    /**
     * Unique id to distinguish each tape
     */
    private UUID id;

    /**
     * Field indicating which tape is the input file and should be only read once and not written to
     */
    private boolean isInputTape;

    /**
     * Field indicating type of the tape - index tape, where the b-tree structure resides, or data tape,
     * where the records data is stored. (There are only 2 tape types in this project, so no enum is needed.)
     */
    private boolean isIndexTape;

    // Dynamic data

    /**
     * <strong>Position in list</strong> - page nr <br></br>
     * <strong>Value</strong> - how much free space is on this page <br></br><br></br>
     * How much free space is left on each tape file page. Free space is all space on the page that is declared
     * (by some upper layer manager objects) as not taken by some data. Data should be stored without gaps
     * within the page, because only a sum of free space on that page is stored here (not all gaps and their positions).
     */
    private List<Integer> freeSpaceOnEachPage;

    /**
     * Max buffers amount for the tape. It can change during the runtime to allow allocating more or less buffers.
     */
    private int maxBuffers;

    /**
     * Count of <strong>read</strong> operations done on the tape file.
     */
    private int reads;

    /**
     * Count of <strong>write</strong> operations done on the tape file.
     */
    private int writes;
}

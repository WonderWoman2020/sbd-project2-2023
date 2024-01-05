package tape.entity;

import lombok.*;

import java.io.File;
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

    // Dynamic data

    /**
     * On tape there can be 'empty' runs, which is how the algorithm of Fibonacci merge sort simulates that
     * there are always only a finonacci number of runs on tapes. <strong>Real</strong> runs have the data, while empty don't have any.
     */
    private int realRuns;

    /**
     * On tape there can be 'empty' runs, which is how the algorithm of Fibonacci merge sort simulates that
     * there are always only a finonacci number of runs on tapes. Real runs have the data, while <strong>empty</strong> don't have any.
     */
    private int emptyRuns;

    /**
     * After first distribution, number of records on each tape is known, so it can be counted and used if we want.
     */
    private List<Integer> recordsInEachRun;

    /**
     * Count of <strong>read</strong> operations done on the tape file.
     */
    private int reads;

    /**
     * Count of <strong>write</strong> operations done on the tape file.
     */
    private int writes;
}

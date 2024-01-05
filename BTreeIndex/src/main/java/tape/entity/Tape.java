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
     * Count of <strong>read</strong> operations done on the tape file.
     */
    private int reads;

    /**
     * Count of <strong>write</strong> operations done on the tape file.
     */
    private int writes;
}

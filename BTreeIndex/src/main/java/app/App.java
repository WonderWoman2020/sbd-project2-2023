package app;

import data_generator.CommandGenerator;
import data_generator.DataGenerator;
import data_generator.FilesUtility;
import record.converter.RecordConverter;

import java.io.File;

/**
 * Hello world!
 *
 */
public class App 
{
    /**
     * Temporarily hardcoded values of paths to directories used by app to store manipulated files
     */
    private static final String INPUT_PATH = "./input/";
    private static final String OUTPUT_PATH = "./output/";
    private static final String TAPES_PATH = "./tapes/";

    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );

        // Deleting temporary files created in previous app runs
        FilesUtility filesUtility1 = new FilesUtility();
        filesUtility1.deleteDir(new File(INPUT_PATH));
        filesUtility1.deleteDir(new File(OUTPUT_PATH));
        filesUtility1.deleteDir(new File(TAPES_PATH));
        System.out.println("Deleted all files created in previous app runs.");

        // Command generator tests
        DataGenerator dataGenerator = new DataGenerator(new RecordConverter(), new FilesUtility());
        CommandGenerator commandGenerator = CommandGenerator.builder()
                .filesUtility(new FilesUtility())
                .dataGenerator(dataGenerator)
                .recordConverter(new RecordConverter())
                .generatedRecords(null)
                .build();

        commandGenerator.generateCommandsFile(INPUT_PATH, "generated_commands.txt", 10);
    }
}

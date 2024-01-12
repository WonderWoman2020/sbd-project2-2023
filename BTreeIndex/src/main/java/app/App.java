package app;

import btree.service.BTreeService;
import data_file.service.DataService;
import data_generator.CommandGenerator;
import data_generator.DataGenerator;
import data_generator.FilesUtility;
import database.service.DatabaseRawReader;
import database.service.DatabaseService;
import entry.converter.EntryConverter;
import entry.entity.Entry;
import entry.service.EntryService;
import record.converter.RecordConverter;
import record.entity.Record;
import record.service.RecordService;
import tape.service.TapeService;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.InvalidAlgorithmParameterException;
import java.util.HashMap;
import java.util.UUID;

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

        // Some DatabaseService all-in-one tests
        TapeService tapeService = TapeService.builder()
                .tapes(new HashMap<>())
                .tapesCurrentReadBlock(new HashMap<>())
                .tapesCurrentWriteBlock(new HashMap<>())
                .tapesBufferedBlocks(new HashMap<>())
                .isEOF(new HashMap<>())
                .filesUtility(new FilesUtility())
                .filesPath(TAPES_PATH)
                .filesBaseName("tape")
                .BLOCK_SIZE(2*10* Entry.builder().build().getSize() + (2*10 + 1)*4 + 4) // 2d * Entry size + (2d + 1) * Pointer size + 1 parent Pointer size
                .build();

        RecordService recordService = RecordService.builder()
                .tapeService(tapeService)
                .recordConverter(new RecordConverter())
                .readBlocksStored(new HashMap<>())
                .readBlocksOffs(new HashMap<>())
                .writeBlocksStored(new HashMap<>())
                .writeBlocksOffs(new HashMap<>())
                .build();

        DataService dataService = DataService.builder()
                .recordService(recordService)
                .build();

        EntryService entryService = EntryService.builder()
                .tapeService(tapeService)
                .entryConverter(new EntryConverter())
                .build();

        BTreeService bTreeService = BTreeService.builder()
                .entryService(entryService)
                .d(10)
                .h(0)
                .lastSearchedNode(0)
                .rootPage(0)
                .build();

        UUID dataTapeID = UUID.randomUUID();
        UUID indexTapeID = UUID.randomUUID();
        tapeService.create(dataTapeID, false);
        tapeService.setMaxBuffers(dataTapeID, 1);
        tapeService.create(indexTapeID, true);
        tapeService.setMaxBuffers(indexTapeID, 1);

        DatabaseService databaseService = DatabaseService.builder()
                .dataService(dataService)
                .bTreeService(bTreeService)
                .recordConverter(new RecordConverter())
                .dataTapeID(dataTapeID)
                .indexTapeID(indexTapeID)
                .build();

        BufferedReader input = new BufferedReader(new InputStreamReader(System.in));
        /*try {
            System.out.println("Create test:");
            databaseService.create(input.readLine());
            System.out.println("Record created.");
            System.out.println("Read test:");
            Record record = databaseService.find(input.readLine());
            System.out.println("Record found:\n"+record);
            System.out.println("Update test:");
            databaseService.update(input.readLine());
            System.out.println("Read test:");
            System.out.println(databaseService.find(input.readLine()));
            System.out.println("Delete test:");
            databaseService.delete(input.readLine());
            System.out.println("Read test:");
            System.out.println(databaseService.find(input.readLine()));
        } catch (InvalidAlgorithmParameterException | IOException e) {
            throw new RuntimeException(e);
        }*/

        DatabaseRawReader databaseRawReader = DatabaseRawReader.builder()
                .tapeService(tapeService)
                .recordConverter(new RecordConverter())
                .dataTapeID(dataTapeID)
                .indexTapeID(indexTapeID)
                .build();

        try {
            System.out.println("Create some records. To stop, input 'q'");
            String command = input.readLine();
            while(!command.equals("q"))
            {
                databaseService.create(command);
                command = input.readLine();
            }
            databaseRawReader.readData();
            command = input.readLine();
            System.out.println(databaseService.find(command));
        } catch (IOException | InvalidAlgorithmParameterException e) {
            throw new RuntimeException(e);
        }

    }
}

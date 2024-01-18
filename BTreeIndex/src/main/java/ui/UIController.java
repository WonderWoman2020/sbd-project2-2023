package ui;

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
import lombok.Builder;
import lombok.ToString;
import node.converter.NodeConverter;
import record.converter.RecordConverter;
import record.entity.Record;
import record.service.RecordService;
import tape.service.TapeService;

import java.io.*;
import java.nio.file.Path;
import java.security.InvalidAlgorithmParameterException;
import java.util.HashMap;
import java.util.UUID;

@Builder
@ToString
public class UIController {

    /**
     * Database manager object. It's responsible for execution of CRUD operations on database (index and data file).
     */
    private DatabaseService databaseService;

    /**
     * Object to read raw database files (index and data file) from bytes, sequentially page by page or their
     * specific pages.
     */
    private DatabaseRawReader databaseRawReader;

    /**
     * Object to manage user files.
     */
    private FilesUtility filesUtility;

    private CommandGenerator commandGenerator;

    /**
     * Default values of paths to directories used by app to store manipulated files.
     */
    private final String INPUT_PATH = "./input/";

    private final String TAPES_PATH = "./tapes/";

    /**
     * Default max numbers of buffered pages, that can stay in memory at the same time, for each file.
     */
    private final int DATA_FILE_BUFFERS = 1;

    private final int INDEX_FILE_BUFFERS = 1;

    /**
     * Default b-tree degree.
     */
    private final int D = 2;


    public void inputLoop() throws IOException, InvalidAlgorithmParameterException {
        BufferedReader input = new BufferedReader(new InputStreamReader(System.in));
        this.initializationMenu(input);
        while(true) {
            System.out.println("\nChoose an option:\n1 - input commands from keyboard,\n2 - read commands from file," +
                    "\n3 - generate commands file.");
            String command = input.readLine();

            if(command == null) {
                System.out.println("Bad command syntax.");
                continue;
            }

            if (command.equals("1")) {
                this.databaseMenu(input);
                break;
            }

            if(command.equals("2"))
            {
                this.executeFromFile(input);
                continue;
            }

            if(command.equals("3"))
            {
                System.out.println("Input path to the commands file (without filename):");
                String path = input.readLine();
                System.out.println("Input filename of the commands file: ");
                String filename = input.readLine();
                int count = this.readIntBiggerThan(input,
                        "How many commands would you like to generate: ",
                        0,
                        10);
                commandGenerator.generateCommandsFile(path, filename, count);
                System.out.println("Commands file has been generated.");
                continue;
            }


            System.out.println("Bad command syntax.");
        }
    }

    private void executeFromFile(BufferedReader input) throws IOException {
        System.out.println("\nInput path to the commands file (without filename): ");
        String path = input.readLine();
        System.out.println("Input filename of the commands file: ");
        String filename = input.readLine();
        Path completePath = Path.of(path).resolve(filename);
        File file = new File(completePath.toString());
        String line;
        try (BufferedReader fileInput = new BufferedReader(new FileReader(file)))
        {
            line = fileInput.readLine();
            while(line != null && !line.isEmpty())
            {
                System.out.println(line);
                if(line.length() < 2)
                    System.out.println("Bad command syntax.");
                else
                    this.chooseDatabaseCommand(line);

                line = fileInput.readLine();
            }
        } catch (IOException | InvalidAlgorithmParameterException e) {
            throw new RuntimeException(e);
        }
        System.out.println("Executed all commands from input file.");
    }

    private void databaseMenu(BufferedReader input) throws IOException, InvalidAlgorithmParameterException {
        System.out.println(this.databaseMenuText());
        String command = input.readLine();
        while(true)
        {
            if(command == null) {
                command = input.readLine();
                continue;
            }

            if(command.equals("exit"))
                break;

            if(command.equals("menu")) {
                System.out.println(this.databaseMenuText());
                command = input.readLine();
                continue;
            }

            if(command.length() < 2) {
                System.out.println("Bad command syntax.");
                command = input.readLine();
                continue;
            }

            this.chooseDatabaseCommand(command);
            command = input.readLine();
        }
    }

    private String databaseMenuText()
    {
        StringBuilder menuText = new StringBuilder();
        menuText.append("\nYou can type one of following commands to manage the database:\n");
        menuText.append("Note: In all commands, key is an 8-byte number bigger than 0, while mass and speed are 4-byte signed numbers.\n");
        menuText.append("C key mass speed            (C - Create, example: C 10 20 30)\n");
        menuText.append("R key                       (R - Read, example: R 10)\n");
        menuText.append("U key mass speed            (U - Update, example: U 10 25 35)\n");
        menuText.append("D key                       (D - Delete, example: D 10)\n");
        menuText.append("RA                          (Read All records in order)\n");
        menuText.append("RE                          (Read all index Entries in order)\n");
        menuText.append("RD                          (Read Data file pages)\n");
        menuText.append("RI                          (Read Index file pages)\n");
        menuText.append("Other commands: menu (to show this menu again), exit (to leave)\n");

        return String.valueOf(menuText);
    }
    private void chooseDatabaseCommand(String command) throws InvalidAlgorithmParameterException {
        try {
            char firstLetter = command.charAt(0);
            switch (firstLetter) {
                case 'C':
                    databaseService.create(command);
                    break;
                case 'R':
                    char secondLetter = command.charAt(1);
                    switch(secondLetter)
                    {
                        case ' ':
                            System.out.println(databaseService.find(command));
                            break;
                        case 'A':
                            databaseService.readAllRecords();
                            break;
                        case 'E':
                            databaseService.readAllEntries();
                            break;
                        case 'D':
                            databaseRawReader.readData();
                            break;
                        case 'I':
                            databaseRawReader.readIndex();
                            break;
                        default:
                            System.out.println("Bad command syntax in command: "+command);
                            break;
                    }
                    break;
                case 'U':
                    databaseService.update(command);
                    break;
                case 'D':
                    databaseService.delete(command);
                    break;
                default:
                    System.out.println("Bad command syntax in command: "+command);
                    break;
            }
        } catch (IllegalArgumentException e)
        {
            System.out.println("Bad command syntax in command: "+command);
            e.printStackTrace();
        }
    }

    /**
     * App's main menu dialog. Initializes database with read parameters.
     * @return
     */
    private void initializationMenu(BufferedReader input) throws IOException {
        StringBuilder menuText = new StringBuilder();
        menuText.append("\nThis is an implementation of a database structure with b-tree index file. It can store records consisting\n" +
                "of an 8-byte key, 4-byte mass of some object and 4-byte speed of that object (e.g. k=12, m=70 kg, s=10 m/s).\n" +
                "To create a database, fill initialization options (or press enter for defaults): \n");
        System.out.print(menuText);
        System.out.print("\nPath for database files (default is '" +this.TAPES_PATH+ "'): ");
        String tapesPath = input.readLine();
        tapesPath = (tapesPath != null && !tapesPath.isEmpty()) ? tapesPath : this.TAPES_PATH;

        int dataBuffers = this.readIntBiggerThan(input,
                "Data file buffers number (default is " + this.DATA_FILE_BUFFERS + "): ",
                0,
                this.DATA_FILE_BUFFERS);

        int indexBuffers = this.readIntBiggerThan(input,
                "Index file buffers number (default is " + this.INDEX_FILE_BUFFERS + "): ",
                0,
                this.INDEX_FILE_BUFFERS);

        int bTreeDegree = this.readIntBiggerThan(input,
                "B-tree degree (default is " + this.D + "): ",
                0,
                this.D);

        this.initDatabase(tapesPath, dataBuffers, indexBuffers, bTreeDegree);
        System.out.println("\nDatabase has been initialized.");
    }

    private int readIntBiggerThan(BufferedReader input, String description, int threshold, int defaultValue) throws IOException {
        int value;
        while(true) {
            System.out.print(description);
            String valueText = input.readLine();
            try {
                value = (valueText != null && !valueText.isEmpty()) ? Integer.parseInt(valueText) : defaultValue;
                if(value > threshold)
                    return value;
            } catch (NumberFormatException e) {
                System.out.println("Incorrect input. Please input a value greater or equal to at least "+threshold+".");
            }
        }
    }
    private void initDatabase(String tapesPath, int dataFileBuffers, int indexFileBuffers, int d)
    {
        this.cleanUpAppFiles(tapesPath);

        // 2d * Entry size + (2d + 1) * Pointer size + 1 parent Pointer size
        int nodeSize = 2*d* Entry.builder().build().getSize() + (2*d + 1)*4 + 4;

        // Create all services and controllers and inject them
        TapeService tapeService = TapeService.builder()
                .tapes(new HashMap<>())
                .tapesCurrentReadBlock(new HashMap<>())
                .tapesCurrentWriteBlock(new HashMap<>())
                .tapesBufferedBlocks(new HashMap<>())
                .isEOF(new HashMap<>())
                .filesUtility(new FilesUtility())
                .filesPath(tapesPath)
                .filesBaseName("tape")
                .BLOCK_SIZE(nodeSize)
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
                .d(d)
                .h(0)
                .rootPage(0)
                .lastSearchedNode(0)
                .sequentialReadLastNode(0)
                .sequentialReadChildToReadNumber(0)
                .build();

        UUID dataTapeID = UUID.randomUUID();
        UUID indexTapeID = UUID.randomUUID();
        tapeService.create(dataTapeID, false);
        tapeService.setMaxBuffers(dataTapeID, dataFileBuffers);
        tapeService.create(indexTapeID, true);
        tapeService.setMaxBuffers(indexTapeID, indexFileBuffers);

        DatabaseService databaseService = DatabaseService.builder()
                .dataService(dataService)
                .bTreeService(bTreeService)
                .recordConverter(new RecordConverter())
                .dataTapeID(dataTapeID)
                .indexTapeID(indexTapeID)
                .build();

        DatabaseRawReader databaseRawReader = DatabaseRawReader.builder()
                .tapeService(tapeService)
                .recordConverter(new RecordConverter())
                .nodeConverter(new NodeConverter(new EntryConverter()))
                .dataTapeID(dataTapeID)
                .indexTapeID(indexTapeID)
                .build();

        this.databaseService = databaseService;
        this.databaseRawReader = databaseRawReader;

        DataGenerator dataGenerator = new DataGenerator(new RecordConverter(), new FilesUtility());
        CommandGenerator commandGenerator = CommandGenerator.builder()
                .filesUtility(new FilesUtility())
                .dataGenerator(dataGenerator)
                .recordConverter(new RecordConverter())
                .generatedRecords(null)
                .build();

        this.commandGenerator = commandGenerator;
        this.filesUtility = new FilesUtility();
    }
    private void cleanUpAppFiles(String tapesPath)
    {
        // Deleting temporary files created in previous app runs
        FilesUtility filesUtility1 = new FilesUtility();
        filesUtility1.deleteDir(new File(tapesPath));
        System.out.println("\nDeleted all files created in previous app runs.");
    }
}

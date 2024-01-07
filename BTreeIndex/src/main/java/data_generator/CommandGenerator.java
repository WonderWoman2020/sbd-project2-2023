package data_generator;

import lombok.AllArgsConstructor;
import lombok.Builder;
import record.converter.RecordConverter;
import record.entity.Record;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@Builder
@AllArgsConstructor
public class CommandGenerator {

    private FilesUtility filesUtility;

    private DataGenerator dataGenerator;

    private RecordConverter recordConverter;

    private List<Record> generatedRecords;

    /**
     *
     * @return Single generated random Command. Possible commads: Create, Update, Delete (only the ones that change the data).
     */
    public String generateCommand()
    {
        Random rand = new Random();

        int option = rand.nextInt(100) % 3;
        if(this.generatedRecords.isEmpty())
            option = 0; // Create some records first

        String operation;
        switch (option)
        {
            case 0:
                operation = "C"; // C - Create a record command
                Record record = dataGenerator.generateRecord();
                this.generatedRecords.add(record);
                operation = operation + " " + recordConverter.recordToString(record) + "\n";
                break;
            case 1:
                operation = "U"; // U - Update a record command
                Record record2 = this.generatedRecords.get(rand.nextInt(this.generatedRecords.size()));
                record2.setMass(rand.nextInt(100));
                record2.setSpeed(rand.nextInt(100));
                operation = operation + " " + recordConverter.recordToString(record2) + "\n";
                break;
            case 2:
                operation = "D"; // D - Deleted a record command
                Record record3 = this.generatedRecords.remove(rand.nextInt(this.generatedRecords.size()));
                operation = operation + " " + Long.toUnsignedString(record3.getKey()) + "\n";
                break;
            default:
                operation = "X";
                break;
        }

        return operation;
    }

    /**
     *
     * @param n Number of commands to generate
     * @param path Where to store the generated file
     * @param filename Filename (with extension included)
     * @return Text file with generated random commands in it
     */
    public File generateCommandsFile(String path, String filename, int n)
    {
        filesUtility.createDirs(Path.of(path));
        File commandsFile = filesUtility.createFile(Path.of(path, filename));
        try (FileOutputStream fos = new FileOutputStream(commandsFile))
        {
            this.generatedRecords = new ArrayList<>();
            for(int i=0; i<n; i++)
                fos.write(this.generateCommand().getBytes());
            fos.flush();
            this.generatedRecords = null;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return commandsFile;
    }
}

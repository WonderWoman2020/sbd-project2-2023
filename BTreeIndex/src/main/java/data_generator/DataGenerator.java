package data_generator;

import lombok.AllArgsConstructor;
import record.converter.RecordConverter;
import record.entity.Record;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Random;

@AllArgsConstructor
public class DataGenerator {

    private RecordConverter recordConverter;
    private FilesUtility filesUtility;

    /**
     *
     * @return Single generated random Record
     */
    public Record generateRecord()
    {
        Random rand = new Random();
        // Temporarily set 100 as a boundary, for simpler math calculations while developing app
        return Record.builder()
                .mass(rand.nextInt(100))
                .speed(rand.nextInt(100))
                .build();
    }

    /**
     *
     * @param n Number of records to generate
     * @param path Where to store the generated file
     * @param filename Filename (with extension included)
     * @return Binary file with generated random Records in it
     */
    public File generateRecordsFile(String path, String filename, int n)
    {
        filesUtility.createDirs(Path.of(path));
        File recordsFile = filesUtility.createFile(Path.of(path, filename));
        try (FileOutputStream fos = new FileOutputStream(recordsFile))
        {
            for(int i=0; i<n; i++)
                fos.write(recordConverter.recordToBytes(this.generateRecord()));
            fos.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return recordsFile;
    }



}

package data_generator;

import lombok.AllArgsConstructor;
import record.converter.RecordConverter;
import record.entity.Record;

import java.io.*;
import java.nio.file.Path;

@AllArgsConstructor
public class InputDataReader {

    private RecordConverter recordConverter;
    private FilesUtility filesUtility;

    public File createRecordsFileFromInput(String path, String filename, int n)
    {
        filesUtility.createDirs(Path.of(path));
        File recordsFile = filesUtility.createFile(Path.of(path, filename));
        try (FileOutputStream fos = new FileOutputStream(recordsFile))
        {
            BufferedReader input = new BufferedReader(new InputStreamReader(System.in));
            for(int i=0; i<n; i++) {
                Record record = recordConverter.stringToRecord(input.readLine());
                if(record == null)
                {
                    System.out.println("Bad record syntax. Write 2 numbers (mass and speed) separated" +
                            " by space and press enter. Example:\n70 10");
                    i--; // User has to repeat writing the record in a correct way for it to be counted in
                    continue;
                }
                fos.write(recordConverter.recordToBytes(record));
            }
            fos.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return recordsFile;
    }

}

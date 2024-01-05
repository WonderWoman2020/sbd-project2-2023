package data_generator;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;

public class FilesUtility {

    public void createDirs(Path path)
    {
        try {
            Files.createDirectories(path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public File createFile(Path filename)
    {
        File file = new File(filename.toString());
        try {
            boolean created = file.createNewFile();
            if(!created)
                throw new FileAlreadyExistsException(file.getAbsolutePath());

            return file;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public boolean deleteFile(Path filename)
    {
        File file = new File(filename.toString());

        if(!file.exists())
            return true;

        boolean deleted = file.delete();
        if(!deleted)
            throw new RuntimeException("File "+ filename +" hasn't successfully deleted.");

        return true;
    }

    public boolean deleteDir(File dirFile) {
        if (dirFile.isDirectory()) {
            File[] dirs = dirFile.listFiles();
            for (File dir: dirs) {
                deleteDir(dir);
            }
        }
        return this.deleteFile(dirFile.toPath());
    }

    public void copyFile(File file, Path path, Path filename)
    {
        try {
            Files.copy(file.toPath(), path.resolve(filename));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class FileManager {

    public FileManager(){
    }

    public void writeFile(String dir, ByteBuffer byteBuffer) throws IOException {
        FileChannel fileChannel = FileChannel.open(Paths.get(dir), StandardOpenOption.APPEND);
        fileChannel.write(byteBuffer.flip());
    }
    public void saveFile(String name) throws IOException {
        FileChannel.open(Paths.get(name), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
    }

}

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

public class Result<Type> implements Iterable<String> {
    private ByteBuffer data;
    private String name;
    private List<String> charData;
    private int position;
    private int block;
    private BufferManager bufferManager;
    private Charset charset;
    public Result(BufferManager bufferManager, String name, String file) throws Exception {
        data = bufferManager.readFile(name, file).flip(); //파일을 처음부터 읽어온다.
        position = 0;
        block = 0;
        this.bufferManager = bufferManager;
        this.name = name;
        this.charset = Charset.defaultCharset();
        charData = new ArrayList<>();
        if(data.hasRemaining()){
            charData.addAll(List.of(charset.decode(data).toString().split("\r\n")));
        }
    }

    @Override
    public Iterator<String> iterator(){
        Iterator<String> it = new Iterator<>(){

            @Override
            public boolean hasNext() {
                if(!charData.isEmpty()){
                    if(charData.size()>position){
                        return true;
                    }else{
                        Optional<ByteBuffer> next = Optional.empty();
                        try {
                            block+=1;
                            next = bufferManager.readNextFile(name, block);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        if(next.isPresent()){
                            data = next.get().flip();
                            if(data.hasRemaining()) {
                                List<String> tmp = List.of(charset.decode(data).toString().split("\r\n"));
                                charData.addAll(tmp);
                                return true;
                            }else{
                                return false;
                            }
                        }else{
                            return false;
                        }
                    }
                }else{
                    return false;
                }
            }

            @Override
            public String next() {
                return charData.get(position++);
            }
        };
        return it;

    }
}

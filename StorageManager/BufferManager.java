import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public class BufferManager {
    private int size; //현재 버퍼의 크기
    private int MAXsize;
    private HashMap<String, BufferInfo> buffersControl; //buffer meta data
    private List<ByteBuffer> buffer;
    private JDBC jdbc;
    private FileManager fileManager;
    public class BufferInfo{ //현재 존재하는 버퍼에 대해서만 관리 값을 기록하기 위한 클래스
        private String name;
        private int order;
        private int block; //몇번째 블록인지
        private int position; //파일에서 position
        private int pin;
        private Date accessDate;
        private Boolean dirty; //dirty 여부
        private String file;

        public BufferInfo(String name, int order, int block, int position, int pin, Date accessDate, Boolean dirty, String file) {
            this.name = name;
            this.order = order;
            this.block = block;
            this.position = position;
            this.pin = pin;
            this.accessDate = accessDate;
            this.dirty = dirty;
            this.file = file;
        }
    }
    public BufferManager(FileManager fileManager, JDBC jdbc){
        buffer = new ArrayList<>();
        for(int i=0; i<250; i++){
            ByteBuffer tmp = ByteBuffer.allocate(4096);
            buffer.add(tmp);
        }
        buffersControl = new HashMap<>();
        size = 0;
        MAXsize = 250;
        this.fileManager = fileManager;
        this.jdbc = jdbc;
    }
    public boolean isFull(){
        if(size >= MAXsize){
            return true;
        }else{
            return false;
        }

    }
    public Optional<ByteBuffer> findBuffer(String name){
        if(buffersControl.containsKey(name)){
            int num = buffersControl.get(name).order;
            return Optional.of(buffer.get(num));
        }else{
            return Optional.empty();
        }
    }


    public ByteBuffer assignBuffer(String name, String file) throws Exception {
        Optional<ByteBuffer> result = findBuffer(name);
        if(result.isPresent()){
            buffersControl.get(name).pin+=1;
            return result.get();
        }else {
            if (!isFull()) {
                this.size += 1;
                buffersControl.put(name, new BufferInfo(name, this.size, 0,0, 1, new Date(), false, file));
                return buffer.get(this.size);
            } else {
                int tmp = deleteBuffer();
                if(tmp!=-1) {
                    this.size += 1;
                    buffersControl.put(name, new BufferInfo(name, tmp, 0,0, 1, new Date(), false, name));
                    return buffer.get(tmp);
                }else{
                    throw new Exception("No buffer");
                }
            }
        }

    }

    public int deleteBuffer() throws IOException {
        //버퍼들 중에서 가장 최근에 사용했던 버퍼를 선택해서 값을 지워줌 (status -> 0)
        List<BufferInfo> result = buffersControl.values().stream().sorted((a,b)->b.accessDate.compareTo(a.accessDate)).collect(Collectors.toList());
        int i = 0;
        while(i<result.size()){
            if(result.get(i).pin>0){
                i+=1; //pin이 있다면 업데이트 연산중이므로 삭제하지 않음.
            }else{
                break;
            }
            if(i>=result.size()){
                return -1; //삭제할 수 있는 버퍼가 없음. 대기해야 한다.
            }
        }
        if(result.get(i).dirty){
            fileManager.saveFile(result.get(i).name);
            fileManager.writeFile(result.get(i).name, buffer.get(result.get(i).order));
        }
        buffer.get(result.get(i).order).clear();
        buffersControl.remove(result.get(i).name);
        this.size-=1;
        //지워준 버퍼의 정보를 그대로 반환해줌
        return result.get(i).order;
    }
    public ByteBuffer readIndexFile(String name, String file) throws Exception{
        if(buffersControl.containsKey(name) && buffersControl.get(name).block == 0){
            return buffer.get(buffersControl.get(name).order);
        }else {
            FileChannel fileChannel = FileChannel.open(Paths.get(file), StandardOpenOption.READ);
            ByteBuffer tmp = assignBuffer(name, file).clear(); //pin = 1
            fileChannel.read(tmp);
            fileChannel.close();
            buffersControl.get(name).block = 0;
            buffersControl.get(name).position = tmp.position();
            buffersControl.get(name).file = file;
            buffersControl.get(name).dirty = true;
            buffersControl.get(name).accessDate = new Date();
            return tmp;
        }
    }
    public Optional<ByteBuffer> readNextIndexFile(String name, int block) throws Exception{
        if(buffersControl.containsKey(name) && buffersControl.get(name).block == block){
            return Optional.of(buffer.get(buffersControl.get(name).order));
        }else {
            FileChannel fileChannel = FileChannel.open(Paths.get(buffersControl.get(name).file), StandardOpenOption.READ);
            ByteBuffer tmp = buffer.get(buffersControl.get(name).order);
            fileChannel = fileChannel.position(buffersControl.get(name).position);
            if (fileChannel.size() > fileChannel.position()) {
                tmp.clear();
                fileChannel.read(tmp);
                fileChannel.close();
                buffersControl.get(name).block = block;
                buffersControl.get(name).position += tmp.position();
                buffersControl.get(name).dirty = true;
                buffersControl.get(name).accessDate = new Date();
                return Optional.of(tmp);
            } else {
                return Optional.empty();
            }
        }
    }
    public ByteBuffer readFile(String name, String file) throws Exception {
        if(buffersControl.containsKey(name) && buffersControl.get(name).block == 0){
            return buffer.get(buffersControl.get(name).order);
        }else {
            FileChannel fileChannel = FileChannel.open(Paths.get(file), StandardOpenOption.READ);
            ByteBuffer tmp = assignBuffer(name, file).clear(); //pin = 1
            Charset charset = Charset.defaultCharset();
            fileChannel.read(tmp);
            String string = charset.decode(tmp.flip()).toString();
            String subString = string.contains("\r\n") ? string.substring(0, string.lastIndexOf("\r\n") + 2) : string;
            tmp.position(0).put(subString.getBytes());
            fileChannel.close();
            buffersControl.get(name).block = 0;
            buffersControl.get(name).position = tmp.position();
            buffersControl.get(name).file = file;
            buffersControl.get(name).dirty = true;
            buffersControl.get(name).accessDate = new Date();
            return tmp;
        }

    }
    public Optional<ByteBuffer> readNextFile(String name, int block) throws IOException {
        if(buffersControl.containsKey(name) && buffersControl.get(name).block == block){
            return Optional.of(buffer.get(buffersControl.get(name).order));
        }else {
            FileChannel fileChannel = FileChannel.open(Paths.get(buffersControl.get(name).file), StandardOpenOption.READ);
            ByteBuffer tmp = buffer.get(buffersControl.get(name).order);
            fileChannel = fileChannel.position(buffersControl.get(name).position);
            if (fileChannel.size() > fileChannel.position()) {
                tmp.clear();
                Charset charset = Charset.defaultCharset();
                fileChannel.read(tmp);
                String string = charset.decode(tmp.flip()).toString();
                String subString = string.contains("\r\n") ? string.substring(0, string.lastIndexOf("\r\n") + 2) : string;
                tmp.position(0).put(subString.getBytes());
                fileChannel.close();
                buffersControl.get(name).block = block;
                buffersControl.get(name).position += tmp.position();
                buffersControl.get(name).dirty = true;
                buffersControl.get(name).accessDate = new Date();
                return Optional.of(tmp);
            } else {
                return Optional.empty();
            }
        }
    }
    public ByteBuffer readJDBCSingleColumn(String name, String column) throws Exception {
        if(buffersControl.containsKey(column) && buffersControl.get(column).block == 0){
            return buffer.get(buffersControl.get(column).order);
        }else {
            ByteBuffer tmp = assignBuffer(column, column).clear();
            ResultSet resultSet = jdbc.getSingleColumnData(name, column);
            int index = 0;
            while (tmp.hasRemaining() && resultSet.next()) {
                if (resultSet.getString(column).getBytes().length < tmp.remaining()) {
                    tmp.put((resultSet.getString(column) + ",").getBytes());
                    index = resultSet.getRow();
                } else {
                    break;
                }
            }
            buffersControl.get(column).block = 0;
            buffersControl.get(column).position = index;
            buffersControl.get(column).dirty = true;
            buffersControl.get(column).accessDate = new Date();
            return tmp;
        }
    }
    public Optional<ByteBuffer> readNextJDBCSingleColumn(String name, String column,int block) throws SQLException{
        if(buffersControl.containsKey(column) && buffersControl.get(column).block == block){
            return Optional.of(buffer.get(buffersControl.get(column).order));
        }else {
            ResultSet resultSet = jdbc.getSingleColumnData(name, column);
            ByteBuffer tmp = buffer.get(buffersControl.get(column).order);
            resultSet.absolute(buffersControl.get(column).position);
            if (!resultSet.isLast()) {
                tmp.clear();
                int index = 0;
                while (resultSet.next()) {
                    if (resultSet.getString(column).getBytes().length < tmp.remaining()) {
                        tmp.put((resultSet.getString(column) + ",").getBytes());
                        index = resultSet.getRow();
                    } else {
                        resultSet.previous();
                        break;
                    }
                }
                buffersControl.get(column).block = block;
                buffersControl.get(column).position = index;
                buffersControl.get(column).dirty = true;
                buffersControl.get(column).accessDate = new Date();
                return Optional.of(tmp);
            } else {
                return Optional.empty();
            }
        }
    }
    public ByteBuffer countInit(String name, String file) throws Exception {
        return assignBuffer(name, file);
    }
    public ByteBuffer makeIndexInit(String name, String file) throws Exception {
        return assignBuffer(name, file);
    }

    public void accessBufferFinish(String name){
        if(buffersControl.containsKey(name)) {
            buffersControl.get(name).accessDate = new Date();
            buffersControl.get(name).pin -= 1;
            //pin 해제
        }
    }

    public void diskUpdated(String name){
        if(buffersControl.containsKey(name)) {
            buffersControl.get(name).dirty = false;
            if(buffersControl.get(name).position>0) {
                buffersControl.get(name).block += 1;
            }
            buffersControl.get(name).accessDate = new Date();
            buffersControl.get(name).position += buffer.get(buffersControl.get(name).order).position();
        }
    }

}

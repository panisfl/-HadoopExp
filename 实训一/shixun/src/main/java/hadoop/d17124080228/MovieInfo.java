package hadoop.d17124080228;

import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MovieInfo implements WritableComparable<MovieInfo> {
    private String title;
    private int year;
    private String type;
    private float star;
    private String director;
    private String actor;
    private String time;
    private String film_page;
    //电影信息
    @Override
    public  String toString(){
        return "MovieInfo [film_page="+ film_page+",title="+ title+",type="+ type+",year="+year+",star="+ star+"]";
    }
    @Override
    public int compareTo(MovieInfo m){
        //按评分从高到低排序该演员参演电影
        if(this.star>m.getStar()){
            return -1;
        }else if(this.star<m.getStar()){
            return 1;
        }
        //如果同分则 优先列出年份较近的
        if(this.year>=m.getYear()){
            return -1;
        }else {
            return 1;
        }
    }
    @Override
    public void readFields(DataInput input)throws IOException {
        //反序列化
        this.title=input.readUTF();
        this.year=input.readInt();
        this.type=input.readUTF();
        this.star=input.readFloat();
        this.director=input.readUTF();
        this.actor=input.readUTF();
        this.time=input.readUTF();
        this.film_page=input.readUTF();
    }
    @Override
    public void write(DataOutput output)throws IOException{
        //序列化
        output.writeUTF(this.title);
        output.writeInt(this.year);
        output.writeUTF(this.type);
        output.writeFloat(this.star);
        output.writeUTF(this.director);
        output.writeUTF(this.actor);
        output.writeUTF(this.time);
        output.writeUTF(this.film_page);
    }
    public String getTitle() {
        return title;
    }
    public void setTitle(String title) {
        this.title = title;
    }
    public int getYear() {
        return year;
    }
    public void setYear(int year) {
        this.year = year;
    }
    public String getType() {
        return type;
    }
    public void setType(String type) {
        this.type = type;
    }
    public float getStar() {
        return star;
    }
    public void setStar(float star) {
        this.star = star;
    }
    public String getDirector() {
        return director;
    }
    public void setDirector(String director) {
        this.director = director;
    }
    public String getActor() {
        return actor;
    }
    public void setActor(String actor) {
        this.actor = actor;
    }
    public String getTime() {
        return time;
    }
    public void setTime(String time) {
        this.time = time;
    }
    public String getFilm_page() {
        return film_page;
    }
    public void setFilm_page(String film_page) {
        this.film_page = film_page;
    }
}
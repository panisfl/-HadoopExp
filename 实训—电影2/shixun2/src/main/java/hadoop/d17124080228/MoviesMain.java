package hadoop.d17124080228;

import com.alibaba.fastjson.JSON;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;

public class MoviesMain {
    public static void main(String[] args) throws Exception {
        //将json转换为csv
        BufferedReader br = new BufferedReader(new FileReader("C:/Users/86131/Desktop/Hadoop部署实践/Film.json"));
        FileWriter fw = new FileWriter(new File("Film.csv"));
        MovieInfo d = null;
        String line = null;
        while ((line = br.readLine()) != null) {
            //Fastjson 把每行的json 字符串转换为对象。
            //d="title":"无间道2 無間道II","year":"2003","type":"剧情,动作,惊悚,犯罪","star":8.3,"director":"刘伟强,麦兆辉","actor":"陈冠希,余文乐,曾志伟,黄秋生,吴镇宇,刘嘉玲,胡军,张耀扬,杜汶泽,连凯,廖启智,惠英红","pp":168807,"time":119,"film_page":"https://movie.douban.com/subject/1307106/"
            d = JSON.parseObject(line, MovieInfo.class);
            if (d.getActor().indexOf("张耀扬") != -1) {
                //Film_page 作为电影ID,"film_page":"https://movie.douban.com/subject/1307106/"
                String mId = d.getFilm_page();
                //演员,"actor":"陈冠希,余文乐,曾志伟,黄秋生,吴镇宇,刘嘉玲,胡军,张耀扬,杜汶泽,连凯,廖启智,惠英红"
                String[] actors = d.getActor().split(",");
                for (String ac : actors) {
                    if(!ac.contains("张耀扬")) {
                        String act;
                        act = ac;
                        //把电影数据写入csv文件。csv表头为 ID,电影名称,评分,演员
                        fw.append(mId + "," + d.getTitle() + "," + ac + "," + d.getStar() + "\n");
                    }
                }
            }
        }
    }
}
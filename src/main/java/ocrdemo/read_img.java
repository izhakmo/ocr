package ocrdemo;
import net.sourceforge.tess4j.ITesseract;
import net.sourceforge.tess4j.Tesseract;
import net.sourceforge.tess4j.TesseractException;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.net.URL;


public class read_img {
    public static void main(String[] args){
        ITesseract img = new Tesseract();
        BufferedReader input_file;  //text of URLs

        try {
            input_file = new BufferedReader(new FileReader("C:\\Users\\izhak\\IdeaProjects\\text.images.txt"));
            String line_url = input_file.readLine();
            String picture_not_numbered = "a";
            int counter = 1;
            int shitty_counter = 1;

            while (line_url != null) {
                String name_of_file = line_url.substring(line_url.lastIndexOf('/') + 1);

                File tmp = new File(("C:\\Users\\izhak\\IdeaProjects\\files\\"+name_of_file));

                try {
                    FileUtils.copyURLToFile(new URL(line_url), tmp);    //download file from line_url to tmp
                    System.out.println(line_url + "\n=========================");

                    try {
                        String msg = img.doOCR(tmp);
                        System.out.println(msg);
                        System.out.println(Integer.toString(counter) +"works");
                        counter++;
                    } catch (TesseractException e) {
                        e.printStackTrace();
                    }
                    // read next line

                } catch (IOException e) {
                    System.out.println("\n error accrue " + e + "\n file failed num:" + shitty_counter);
                    shitty_counter++;

                }


                line_url = input_file.readLine();
            }




            input_file.close();


        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}

import java.io.*;

public class Test {
    public static void main(String [] args) {

        // The name of the file to open.
        String fileName = "routes/routes.csv";

        // This will reference one line at a time
        String line = null;

        try {
            // FileReader reads text files in the default encoding.
            FileReader fileReader = 
                new FileReader(fileName);

            // Always wrap FileReader in BufferedReader.
            BufferedReader bufferedReader = 
                new BufferedReader(fileReader);

            BufferedWriter writer = new BufferedWriter(new FileWriter("routes.txt"));

            // for(int j = 0; j < 50; j++){
            //     line = bufferedReader.readLine();
            while((line = bufferedReader.readLine()) != null) {
                if(line.substring(line.length() - 1).equals(","))
                    line = (line + " ");
                String[] split = (line.replaceAll(",,", ", ,")).split(",+");

                // FileReader reads text files in the default encoding.
                FileReader fileReader2 = 
                    new FileReader("airlines/airlines.csv");

                // Always wrap FileReader in BufferedReader.
                BufferedReader bufferedReader2 = 
                    new BufferedReader(fileReader2);

                System.out.println(split[0]);

                    for(int i = 0; i < 2; i++)
                        writer.write(split[i] + ",");
                    writer.write(split[3] + ",");
                    writer.write(split[2] + ",");
                    for(int i = 4; i < 11; i++)
                        writer.write(split[i] + ",");
                    writer.write(split[11] + "\n");


                bufferedReader2.close();

            }   

            writer.close();

            // Always close files.
            bufferedReader.close();         
        }
        catch(FileNotFoundException ex) {
            System.out.println(
                "Unable to open file '" + 
                fileName + "'");                
        }
        catch(IOException ex) {
            System.out.println(
                "Error reading file '" 
                + fileName + "'");                  
            // Or we could just do this: 
            // ex.printStackTrace();
        }
    }
}
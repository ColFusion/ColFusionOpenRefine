
package com.google.refine.commands.colfusion;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONWriter;

import com.google.refine.ProjectManager;
import com.google.refine.commands.Command;
import com.google.refine.model.Project;

//import edu.pitt.sis.exp.colfusion.dal.databaseHandlers.DatabaseHandler;
import edu.pitt.sis.exp.colfusion.dal.databaseHandlers.DatabaseHandlerBase;
import edu.pitt.sis.exp.colfusion.dal.databaseHandlers.DatabaseHandlerFactory;
import edu.pitt.sis.exp.colfusion.dal.databaseHandlers.MetadataDbHandler;
//import edu.pitt.sis.exp.colfusion.dal.databaseHandlers.TargetDatabaseHandlerFactory;
import edu.pitt.sis.exp.colfusion.utils.CSVUtils;

/**
 * @author xxl
 * 
 */
public class SaveProjectDataToDatabaseCommand extends Command  {

    private static final String PROPERTY_CSV_FILE_NAME = "csv_file_name";
    private static final String PROPERTY_CSV_FILE_DIR = "csv_file_dir";

    /*
     * By using this function, the
     * com.google.refine.myDatabase.DatabaseOperation will not be used anymore,
     * but just keep that file in case of future use.
     */
    @Override
    public void doPost(final HttpServletRequest request, final HttpServletResponse response)
            throws ServletException, IOException {
        try {
        
            Properties p = new Properties();
            String fileName = "/ColFusionOpenRefine.properties";
            InputStream in = SaveProjectDataToDatabaseCommand.class.getResourceAsStream(fileName);
            p.load(in);
            in.close();
            
            int lockTime = Integer.valueOf(p.getProperty("lock_time"));
    
            long projectId = Long.parseLong(request.getParameter("projectId"));
            String colfusionUserId = request.getParameter("colfusionUserId");
            int sid = -1;
            String tableName = "";
    
            String msg = "";
            
            MetadataDbHandler metadataDbHandler = DatabaseHandlerFactory.getMetadataDbHandler();
            DatabaseHandlerBase databaseHandler = null;
       
            sid = metadataDbHandler.getSid(String.valueOf(projectId));
           
            databaseHandler = DatabaseHandlerFactory.getTargetDatabaseHandler(sid);
            tableName = metadataDbHandler.getTableName(sid);
        
            if (metadataDbHandler.isTimeOutForCurrentUser(sid, tableName, Integer.valueOf(colfusionUserId), lockTime)) {
                msg = "Time is out, cannot save!";
            } 
            else {
                int count = metadataDbHandler.getCountFromOpenRefineHistoryHelper(sid, tableName);
                metadataDbHandler.updateOpenRefineHistoryHelper(sid, tableName, count, 1);
                /*
                 * ***********update checkpoint************************
                 * Only the "Save" is valid, copy files to temp folder
                 */
                String dir = p.getProperty("file_dir"); // + projectId + ".project";
                String tempFolder = p.getProperty("temp_folder");
                
                String tempDir = dir + tempFolder + File.separator;
                String projectDir = projectId + ".project" + File.separator;
                
                File folderPath = new File(tempDir + projectDir);
                deleteAllFilesOfDir(folderPath);
                
                copyFolder(dir + projectDir, tempDir + projectDir);
                /*
                 * *****************************************************
                 */
                // Get project
                Project project = ProjectManager.singleton.getProject(projectId);

                // Get column names from project
                ArrayList<String> columnNames = new ArrayList<>();
                for (String s : project.columnModel.getColumnNames()) {
                    columnNames.add(s);
                }

                                /*
                 * ****************Save data into database*****************
                 */
                // 0. Remove original table
               
                databaseHandler.removeTable(tableName);
               
//                // 1. Create table
//                String tableCreateQuery = "CREATE TABLE " + tableName + " (";
//                for (int i = 0; i < columnNames.size(); i++) {
//                    if (i < columnNames.size() - 1) {
//                        tableCreateQuery += headlineControl(columnNames.get(i)) + " VARCHAR(255), ";
//                    } else {
//                        tableCreateQuery += headlineControl(columnNames.get(i)) + " VARCHAR(255))";
//                    }
//                }
                
                databaseHandler.createTableIfNotExist(tableName, columnNames);
                
//                databaseHandler.createOriginalTable(tableCreateQuery, sid, tableName);

                // Get each row from project
                ArrayList<ArrayList<String>> rows = new ArrayList<>();
                rows = getReorderedRows(project);
                
                /*
                 * *************test csv begin**********************
                 */
                ArrayList<String> fileRows = new ArrayList<>();
                for (int j = 0; j < rows.size(); j++) {
                    String tempRow = "";
                    for (int k = 0; k < columnNames.size(); k++) {
                        if(k != columnNames.size() - 1) {
                            tempRow += rows.get(j).get(k) + ",";
                        } else {
                            tempRow += rows.get(j).get(k);
                        }
                    }
                    fileRows.add(tempRow);
                }
                
                String csvDir = p.getProperty(PROPERTY_CSV_FILE_DIR);
                String csvFileName = p.getProperty(PROPERTY_CSV_FILE_NAME);
                
                boolean isSuccess = CSVUtils.exportCsv(new File(dir + projectDir + csvFileName), fileRows);
                
                if(isSuccess) {
                    databaseHandler.importCsvToTable(csvDir + projectId + ".project" + File.separator + csvFileName, tableName);
                } else {
                    logger.info("csv saving failed!");
                }

                ArrayList<Integer> cids;
                    msg = "Changes have been saved!";
            }
       
            Writer w = response.getWriter();
            JSONWriter writer = new JSONWriter(w);
       
            writer.object();

            writer.key("msg");
            writer.value(msg);
            writer.endObject();
        } catch (Exception e) {
            respondException(response, e);
        }
//        finally {
//            w.flush();
//            w.close();
//        }
    }

    public static ArrayList<ArrayList<String>> getReorderedRows(final Project project) {
        ArrayList<ArrayList<String>> rows = new ArrayList<ArrayList<String>>();
        if (project.columnModel.columns.isEmpty() || project.rows.isEmpty()) {
            return rows;
        } else {
            for (int i = 0; i < project.rows.size(); i++) {
                rows.add(new ArrayList<String>());
                for (int j = 0; j < project.columnModel.columns.size(); j++) {
                    if (project.columnModel.columns.get(j).getCellIndex() > (project.rows.get(i).cells.size() - 1)) {
                        rows.get(i).add("");
                    } else {
                        if (project.rows.get(i).cells.get(project.columnModel.columns.get(j).getCellIndex()) == null) {
                            rows.get(i).add("");
                        } else {
                            rows.get(i).add(
                                    project.rows.get(i).cells.get(project.columnModel.columns.get(j).getCellIndex())
                                            .toString());
                        }
                    }
                }
            }
            return rows;
        }

    }
    
    private void copyFile(final String sourceFile, final String targetFile) {
        try {
            int byteread = 0;
            File oldfile = new File(sourceFile);
            if (oldfile.exists()) {
                InputStream inStream = new FileInputStream(sourceFile);
                @SuppressWarnings("resource")
                FileOutputStream fs = new FileOutputStream(targetFile);
                byte[] buffer = new byte[1444];
                while ((byteread = inStream.read(buffer)) != -1) {
                    fs.write(buffer, 0, byteread);
                }
                inStream.close();
            }
        } catch (Exception e) {
            System.out.println("Error happens when copying files");
            e.printStackTrace();

        }
    }

    private void copyFolder(final String sourceDir, final String targetDir) {
        try {
            (new File(targetDir)).mkdirs();
            File a = new File(sourceDir);
            String[] file = a.list();
            File temp = null;
            for (int i = 0; i < file.length; i++) {
                if (sourceDir.endsWith(File.separator)) {
                    temp = new File(sourceDir + file[i]);
                } else {
                    temp = new File(sourceDir + File.separator + file[i]);
                }

                if (temp.isFile()) {
                    FileInputStream input = new FileInputStream(temp);
                    FileOutputStream output = new FileOutputStream(targetDir + "/" + (temp.getName()).toString());
                    byte[] b = new byte[1024 * 5];
                    int len;
                    while ((len = input.read(b)) != -1) {
                        output.write(b, 0, len);
                    }
                    output.flush();
                    output.close();
                    input.close();
                }
                if (temp.isDirectory()) {
                    copyFolder(sourceDir + "/" + file[i], targetDir + "/" + file[i]);
                }
            }
        } catch (Exception e) {
            System.out.println("Error happens when copying files in folder");
            e.printStackTrace();

        }
    }
    
    private void copyZipFile(final String sourceZipFile, final String targetZipFile) {
        InputStream inStream = null;
        try {
            inStream = new FileInputStream(sourceZipFile);
        } catch (FileNotFoundException e) {
            System.err.println("读取文件[" + sourceZipFile + "]发生错误" + "\r\n" + e.getCause());
            return;
        }
        File targetFile = new File(targetZipFile);
        OutputStream outStream = null;
        try {
            targetFile.createNewFile();
            outStream = new FileOutputStream(targetFile);
            byte[] by = new byte[1024];
            while (inStream.available() > 0) {
                inStream.read(by);
                outStream.write(by);
            }
        } catch (IOException e) {
            System.err.println("Error happens when create file [" + targetZipFile + "]" + "\r\n" + e.getCause());
        } finally {
            if (null != inStream) {
                try {
                    inStream.close();
                } catch (IOException e) {
                    System.err.println(e.getCause());
                }
            }
            if (null != outStream) {
                try {
                    outStream.flush();
                } catch (IOException e) {
                    System.err.println(e.getCause());
                }
                try {
                    outStream.close();
                } catch (IOException e) {
                    System.err.println(e.getCause());
                }
            }
        }
    }
    
    public void deleteAllFilesOfDir(final File path) {
        if (!path.exists()) {
            return;
        }  
        if (path.isFile()) {  
            path.delete();  
            return;  
        }  
        File[] files = path.listFiles();  
        for (int i = 0; i < files.length; i++) {  
            deleteAllFilesOfDir(files[i]);  
        }  
        path.delete();  
    }
    
    private String removeSpace(final String str) {
        String result = "";
        for(int i = 0; i < str.length(); i++) {
            if(str.charAt(i) != ' ') {
                result += str.charAt(i);
            }
        }
        return result;
    }
}

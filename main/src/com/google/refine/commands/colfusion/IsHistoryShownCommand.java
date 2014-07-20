package com.google.refine.commands.colfusion;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.sql.SQLException;
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONException;
import org.json.JSONWriter;

import com.google.refine.commands.Command;

import edu.pitt.sis.exp.colfusion.dao.MetadataDbHandler;
import edu.pitt.sis.exp.colfusion.dao.TargetDatabaseHandlerFactory;


public class IsHistoryShownCommand extends Command {
    @Override
    public void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        Properties p = new Properties();
        String fileName = "/ColFusionOpenRefine.properties";
        InputStream in = SaveProjectDataToDatabaseCommand.class.getResourceAsStream(fileName);
        p.load(in);
        in.close();
        
        boolean isHistoryShown = false;

        String openrefineUrl = request.getParameter("openrefineUrl");
        int sid = Integer.valueOf(request.getParameter("sid"));
        String tableName = request.getParameter("tableName");
        String projectId = getProjectId(openrefineUrl);
        
        String dir = p.getProperty("file_dir");
        String tempFolder = p.getProperty("temp_folder");
        
        String tempDir = dir + tempFolder + "\\";
        String projectDir = projectId + ".project\\";
        
        File savedHistory = new File(tempDir + projectDir + "history");
        String[] savedChanges = savedHistory.list();
        int savedChangesLength = 0;
        if(savedHistory.list() != null) {
            savedChangesLength = savedChanges.length;
        }

        File notSavedHistory = new File(dir + projectDir + "history");
        String[] notSavedChanges = notSavedHistory.list();
        int notSavedChangesLength = 0;
        if(notSavedHistory.list() != null) {
            notSavedChangesLength = notSavedChanges.length;
        }
        
        MetadataDbHandler metadataDbHandler = TargetDatabaseHandlerFactory.getMetadataDbHandler(); // colfusion db
        int count = -1;
        
        try {
            count = metadataDbHandler.getCountFromOpenRefineHistoryHelper(sid, tableName);
        } catch (SQLException e1) {
            e1.printStackTrace();
        }
        
        if(savedChangesLength != notSavedChangesLength) {
            isHistoryShown = true;
        } else if(count >= 1) {
            isHistoryShown = true;
        }

        try {
            metadataDbHandler.updateOpenRefineHistoryHelper(sid, tableName, count + 1, 0);
        } catch (SQLException e1) {
            e1.printStackTrace();
        }
        
        Writer w = response.getWriter();
        JSONWriter writer = new JSONWriter(w);
        try {
            writer.object();

            writer.key("isHistoryShown");
            writer.value(isHistoryShown);
            writer.key("msg");
            writer.value(projectId + " Hey! It works!");
            writer.endObject();
        } catch (JSONException e) {
            throw new ServletException(e);
        } finally {
            w.flush();
            w.close();
        }
    }
    
    private String getProjectId(String url) {
        // 13 is the length of the projectId
        // TODO: maybe we should put "13" into the .properties file
        return url.substring(url.length() - 13);
    }
}

package com.google.refine.commands.colfusion;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONException;
import org.json.JSONObject;

import com.google.refine.commands.Command;
import com.google.refine.util.ParsingUtilities;

import edu.pitt.sis.exp.colfusion.dal.databaseHandlers.MetadataDbHandler;
import edu.pitt.sis.exp.colfusion.dal.databaseHandlers.TargetDatabaseHandlerFactory;

/**
 * @author xxl
 *
 */
public class IsChangesSavedCommand extends Command {
    @Override
    public void doGet(final HttpServletRequest request, final HttpServletResponse response)
            throws IOException, ServletException {
        Properties p = new Properties();
        String fileName = "/ColFusionOpenRefine.properties";
        InputStream in = SaveProjectDataToDatabaseCommand.class.getResourceAsStream(fileName);
        p.load(in);
        in.close();
        
        Properties parameters = ParsingUtilities.parseUrlParameters(request);

        int sid = Integer.valueOf(parameters.getProperty("sid"));
        String tableName = parameters.getProperty("tableName");
        
        MetadataDbHandler metadataDbHandler = TargetDatabaseHandlerFactory.getMetadataDbHandler();
        
        String projectId;
        JSONObject result = new JSONObject();
        
        boolean isChangesSaved = true;
        
        try {
            projectId = metadataDbHandler.getProjectId(sid, tableName);
            
            String dir = p.getProperty("file_dir");
            String tempFolder = p.getProperty("temp_folder");
            
            String tempDir = dir + tempFolder + File.separator;
            String projectDir = projectId + ".project" + File.separator;
            
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
            
            if(savedChangesLength != notSavedChangesLength) {
                isChangesSaved = false;
            } else if(metadataDbHandler.getCountFromOpenRefineHistoryHelper(sid, tableName) > 1 && metadataDbHandler.getIsSavedFromOpenRefineHistoryHelper(sid, tableName) != 1) {
                isChangesSaved = false;
            }
            result.put("isChangesSaved", isChangesSaved);
        } catch (SQLException | JSONException e1) {
            e1.printStackTrace();
        }

        response.setCharacterEncoding("UTF-8");
        response.setHeader("Content-Type", "application/json");
        respond(response, result.toString());

    }
}

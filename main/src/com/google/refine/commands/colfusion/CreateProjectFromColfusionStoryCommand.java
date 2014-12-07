
package com.google.refine.commands.colfusion;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONObject;

import com.google.refine.commands.Command;
import com.google.refine.util.ParsingUtilities;

import edu.pitt.sis.exp.colfusion.ColFusionOpenRefineProjectManager;
//import edu.pitt.sis.exp.colfusion.dao.MetadataDbHandler;
//import edu.pitt.sis.exp.colfusion.dao.TargetDatabaseHandlerFactory;
import edu.pitt.sis.exp.colfusion.dal.databaseHandlers.DatabaseHandlerFactory;
import edu.pitt.sis.exp.colfusion.dal.databaseHandlers.MetadataDbHandler;


/**
 * @author xxl
 * 
 */
public class CreateProjectFromColfusionStoryCommand extends Command {

    @Override
    public void doGet(final HttpServletRequest request, final HttpServletResponse response) throws IOException, ServletException {
        
        Properties p = new Properties();
        String fileName="/ColFusionOpenRefine.properties";
        InputStream in = CreateProjectFromColfusionStoryCommand.class.getResourceAsStream(fileName);
        p.load(in);  
        in.close();
        
        int lockTime = Integer.valueOf(p.getProperty("lock_time"));
        
        Properties parameters = ParsingUtilities.parseUrlParameters(request);
        /*
         * Get "sid" and "tableName" from "request"
         */
        int sid = Integer.valueOf(parameters.getProperty("sid"));
        String tableName = parameters.getProperty("tableName");
        int userId = Integer.valueOf(parameters.getProperty("userId"));
        
        try {
//            DatabaseHandler databaseHandler = TargetDatabaseHandlerFactory.getTargetDatabaseHandler(sid); // other db
            MetadataDbHandler metadataDbHandler = DatabaseHandlerFactory.getMetadataDbHandler(); // colfusion db
            
            int count = metadataDbHandler.getCountFromOpenRefineHistoryHelper(sid, tableName);
            
            if(count < 0) {
                metadataDbHandler.insertIntoOpenRefineHistoryHelper(sid, tableName);
            } else {
                int isSaved = metadataDbHandler.getIsSavedFromOpenRefineHistoryHelper(sid, tableName);
                metadataDbHandler.updateOpenRefineHistoryHelper(sid, tableName, 0, isSaved);
            }
            
            JSONObject result = new JSONObject();
    
            boolean isTimeOut = false;
            boolean isTableLocked = metadataDbHandler.isTableLocked(sid, tableName);
            boolean isEditingByCurrentUser = false;
                
            if (isTableLocked) {
                isEditingByCurrentUser = metadataDbHandler.isBeingEditedByCurrentUser(sid, tableName, userId);
                if (metadataDbHandler.isTimeOut(sid, tableName, lockTime)) {
                    isTimeOut = true;
                    metadataDbHandler.releaseTableLock(sid, tableName);
                }
            }

            if (!isEditingByCurrentUser && (isTableLocked && !isTimeOut)) {
                
            } else {
                if (!isEditingByCurrentUser) {
                    metadataDbHandler.createEditLog(sid, tableName, userId);
                }
    
                ColFusionOpenRefineProjectManager colFusionOpenRefineProjectManager = new ColFusionOpenRefineProjectManager();
                result.put("openrefineURL", colFusionOpenRefineProjectManager.createProjectToOpenRefine(sid, tableName));
            }
            result.put("isEditing", isTableLocked && !isEditingByCurrentUser);
            result.put("isTimeOut", isTimeOut);
            if(isTableLocked && !isEditingByCurrentUser && !isTimeOut) {
                result.put("msg", "Table is being edited by User: " + metadataDbHandler.getUserLoginById(metadataDbHandler.getOperatingUserId(sid, tableName)));
            }
            result.put("successful", true);
        
            response.setCharacterEncoding("UTF-8");
            response.setHeader("Content-Type", "application/json");
            respond(response, result.toString());
        } catch (Exception e) {
            respondException(response, e);
        }
    }
}

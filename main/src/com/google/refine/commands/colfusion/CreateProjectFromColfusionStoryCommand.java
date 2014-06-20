
package com.google.refine.commands.colfusion;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONException;
import org.json.JSONObject;

import com.google.refine.commands.Command;
import com.google.refine.util.ParsingUtilities;

import edu.pitt.sis.exp.colfusion.ColFusionOpenRefineProjectManager;
import edu.pitt.sis.exp.colfusion.dao.MetadataDbHandler;
import edu.pitt.sis.exp.colfusion.dao.TargetDatabaseHandlerFactory;

/**
 * @author Xiaolong Xu
 * 
 */
public class CreateProjectFromColfusionStoryCommand extends Command {

    @Override
    public void doGet(final HttpServletRequest request, final HttpServletResponse response)
            throws ServletException, IOException {

        Properties parameters = ParsingUtilities.parseUrlParameters(request);
        /*
         * Get "sid" and "tableName" from "request"
         */
        int sid = Integer.valueOf(parameters.getProperty("sid"));
        String tableName = parameters.getProperty("tableName");
        int userId = Integer.valueOf(parameters.getProperty("userId"));
        
        try {
//            DatabaseHandler databaseHandler = TargetDatabaseHandlerFactory.getTargetDatabaseHandler(sid); // other db
            MetadataDbHandler metadataDbHandler = TargetDatabaseHandlerFactory.getMetadataDbHandler(); // colfusion db
            
            JSONObject result = new JSONObject();
    
            boolean isTimeOut = false;
            boolean isTableLocked = metadataDbHandler.isTableLocked(sid, tableName);
            boolean isEditingByCurrentUser = false;
            
    
            if (isTableLocked) {
                isEditingByCurrentUser = metadataDbHandler.isBeingEditedByCurrentUser(sid, tableName, userId);
                if (metadataDbHandler.isTimeOut(sid, tableName, 30)) {
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
            result.put("msg", "Table is being edited by User: " + metadataDbHandler.getUserLoginById(metadataDbHandler.getOperatingUserId(sid, tableName)));
            result.put("successful", true);
        
            response.setCharacterEncoding("UTF-8");
            response.setHeader("Content-Type", "application/json");
            respond(response, result.toString());
        } catch (JSONException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } 
    }

}


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

import edu.pitt.sis.exp.colfusion.dal.databaseHandlers.DatabaseHandlerFactory;
import edu.pitt.sis.exp.colfusion.dal.databaseHandlers.MetadataDbHandler;
import edu.pitt.sis.exp.colfusion.utils.ConfigManager;
import edu.pitt.sis.exp.colfusion.utils.PropertyKeys;


/**
 * @author xxl
 *
 */
public class IsTableLockedCommand extends Command {

    @Override
    public void doGet(final HttpServletRequest request, final HttpServletResponse response)
            throws IOException, ServletException {

        int lockTime = Integer.valueOf(ConfigManager.getInstance().getProperty(PropertyKeys.COLFUSION_OPENREFINE_LOCK_TIME));
        
        Properties parameters = ParsingUtilities.parseUrlParameters(request);

        MetadataDbHandler metadataDbHandler = DatabaseHandlerFactory.getMetadataDbHandler(); // colfusion
                                                                                                   // db

        int sid = Integer.valueOf(parameters.getProperty("sid"));
        String tableName = parameters.getProperty("tableName");
        int userId = Integer.valueOf(parameters.getProperty("userId"));
        try {
            boolean isTimeOut = false;
            boolean isTableBeingEditing = metadataDbHandler.isTableBeingEditing(sid, tableName);
            boolean isEditingByCurrentUser = false;
            boolean isTableLocked = false;

            JSONObject result = new JSONObject();

            String userLogin = "";

            if (isTableBeingEditing) {
                isEditingByCurrentUser = metadataDbHandler.isBeingEditedByCurrentUser(sid, tableName, userId);
                if (metadataDbHandler.isTimeOut(sid, tableName, lockTime)) {
                    isTimeOut = true;
                }
            }
            if (!isEditingByCurrentUser && (isTableBeingEditing && !isTimeOut)) {
                isTableLocked = true;
                userLogin = metadataDbHandler.getUserLoginById(metadataDbHandler.getOperatingUserId(sid, tableName));
            }

            result.put("userLogin", userLogin);
            result.put("isTableLocked", isTableLocked);
            result.put("successful", true);

            response.setCharacterEncoding("UTF-8");
            response.setHeader("Content-Type", "application/json");
            respond(response, result.toString());
        } catch (JSONException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}

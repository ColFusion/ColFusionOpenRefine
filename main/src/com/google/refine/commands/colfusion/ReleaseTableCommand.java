
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


import edu.pitt.sis.exp.colfusion.dal.databaseHandlers.MetadataDbHandler;
import edu.pitt.sis.exp.colfusion.dal.databaseHandlers.DatabaseHandlerFactory;


/**
 * @author xxl
 *
 */
public class ReleaseTableCommand extends Command {

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException {

        Properties parameters = ParsingUtilities.parseUrlParameters(request);

        int sid = Integer.valueOf(parameters.getProperty("sid"));
        String tableName = parameters.getProperty("tableName");
        int colfusionUserId = Integer.valueOf(parameters.getProperty("userId"));
        
        MetadataDbHandler metadataDbHandler = DatabaseHandlerFactory.getMetadataDbHandler();
        
        JSONObject result = new JSONObject();

        try {
            if(metadataDbHandler.isInCurrentUserSession(sid, tableName, colfusionUserId)) {
                metadataDbHandler.releaseTableLock(sid, tableName);
            }
            result.put("successful", true);
        } catch (JSONException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        response.setCharacterEncoding("UTF-8");
        response.setHeader("Content-Type", "application/json");
        respond(response, result.toString());

    }
}

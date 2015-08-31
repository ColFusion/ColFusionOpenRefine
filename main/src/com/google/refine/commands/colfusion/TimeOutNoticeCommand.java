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
public class TimeOutNoticeCommand extends Command {
    @Override
    public void doGet(final HttpServletRequest request, final HttpServletResponse response)
            throws IOException, ServletException {

        int noticeTime = Integer.valueOf(ConfigManager.getInstance().getProperty(PropertyKeys.COLFUSION_OPENREFINE_NOTICE_TIME));
        
        Properties parameters = ParsingUtilities.parseUrlParameters(request);

        int sid = Integer.valueOf(parameters.getProperty("sid"));
        String tableName = parameters.getProperty("tableName");
//        int colfusionUserId = Integer.valueOf(parameters.getProperty("userId"));
        
        MetadataDbHandler metadataDbHandler = DatabaseHandlerFactory.getMetadataDbHandler();
        
        JSONObject result = new JSONObject();

        try {
            boolean isTimeOuting = false;
            if(metadataDbHandler.isTimeOut(sid, tableName, noticeTime)) {
                isTimeOuting = true;
            }
            result.put("isTableLocked", metadataDbHandler.isTableLocked(sid, tableName));
            result.put("isTimeOuting", isTimeOuting);
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

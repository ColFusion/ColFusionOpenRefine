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

import edu.pitt.sis.exp.colfusion.dal.dataModels.tableDataModel.RelationKey;
import edu.pitt.sis.exp.colfusion.dal.databaseHandlers.DatabaseHandlerFactory;
import edu.pitt.sis.exp.colfusion.dal.databaseHandlers.MetadataDbHandler;
import edu.pitt.sis.exp.colfusion.dal.managers.ColumnTableInfoManager;
import edu.pitt.sis.exp.colfusion.dal.managers.ColumnTableInfoManagerImpl;
import edu.pitt.sis.exp.colfusion.dal.orm.ColfusionColumnTableInfo;
import edu.pitt.sis.exp.colfusion.utils.ConfigManager;
import edu.pitt.sis.exp.colfusion.utils.PropertyKeys;

/**
 *
 */
public class TimeOutNoticeCommand extends Command {
	@Override
	public void doGet(final HttpServletRequest request, final HttpServletResponse response)
			throws IOException, ServletException {

		final int noticeTime = Integer.valueOf(ConfigManager.getInstance().getProperty(PropertyKeys.COLFUSION_OPENREFINE_NOTICE_TIME));

		final Properties parameters = ParsingUtilities.parseUrlParameters(request);

		final int sid = Integer.valueOf(parameters.getProperty("sid"));
		final String tableName = parameters.getProperty("tableName");

		final ColumnTableInfoManager columnTableMng = new ColumnTableInfoManagerImpl();
		final ColfusionColumnTableInfo columnTable = columnTableMng.findBySidAndOriginalTableName(sid, tableName);
		final RelationKey relationKey = new RelationKey(tableName, columnTable.getDbTableName());

		//        int colfusionUserId = Integer.valueOf(parameters.getProperty("userId"));

		final MetadataDbHandler metadataDbHandler = DatabaseHandlerFactory.getMetadataDbHandler();

		final JSONObject result = new JSONObject();

		try {
			boolean isTimeOuting = false;
			if(metadataDbHandler.isTimeOut(sid, relationKey, noticeTime)) {
				isTimeOuting = true;
			}
			result.put("isTableLocked", metadataDbHandler.isTableLocked(sid, relationKey));
			result.put("isTimeOuting", isTimeOuting);
		} catch (final JSONException e) {
			e.printStackTrace();
		} catch (final SQLException e) {
			e.printStackTrace();
		}

		response.setCharacterEncoding("UTF-8");
		response.setHeader("Content-Type", "application/json");
		respond(response, result.toString());
	}
}

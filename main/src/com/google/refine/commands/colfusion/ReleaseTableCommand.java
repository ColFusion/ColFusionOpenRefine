
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


/**
 * @author xxl
 *
 */
public class ReleaseTableCommand extends Command {

	@Override
	public void doGet(final HttpServletRequest request, final HttpServletResponse response)
			throws IOException, ServletException {

		final Properties parameters = ParsingUtilities.parseUrlParameters(request);

		final int sid = Integer.valueOf(parameters.getProperty("sid"));
		final String tableName = parameters.getProperty("tableName");
		final int colfusionUserId = Integer.valueOf(parameters.getProperty("userId"));

		final MetadataDbHandler metadataDbHandler = DatabaseHandlerFactory.getMetadataDbHandler();

		final JSONObject result = new JSONObject();

		try {
			final ColumnTableInfoManager columnTableMng = new ColumnTableInfoManagerImpl();
			final ColfusionColumnTableInfo columnTable = columnTableMng.findBySidAndOriginalTableName(sid, tableName);
			final RelationKey relationKey = new RelationKey(tableName, columnTable.getDbTableName());

			if(metadataDbHandler.isInCurrentUserSession(sid, relationKey, colfusionUserId)) {
				metadataDbHandler.releaseTableLock(sid, relationKey);
			}
			result.put("successful", true);
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

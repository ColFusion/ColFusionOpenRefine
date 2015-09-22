
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
public class IsTableLockedCommand extends Command {

	@Override
	public void doGet(final HttpServletRequest request, final HttpServletResponse response)
			throws IOException, ServletException {

		final int lockTime = Integer.valueOf(ConfigManager.getInstance().getProperty(PropertyKeys.COLFUSION_OPENREFINE_LOCK_TIME));

		final Properties parameters = ParsingUtilities.parseUrlParameters(request);

		final MetadataDbHandler metadataDbHandler = DatabaseHandlerFactory.getMetadataDbHandler(); // colfusion
		// db

		final int sid = Integer.valueOf(parameters.getProperty("sid"));
		final String tableName = parameters.getProperty("tableName");
		final int userId = Integer.valueOf(parameters.getProperty("userId"));
		try {

			final ColumnTableInfoManager columnTableMng = new ColumnTableInfoManagerImpl();
			final ColfusionColumnTableInfo columnTable = columnTableMng.findBySidAndOriginalTableName(sid, tableName);
			final RelationKey relationKey = new RelationKey(tableName, columnTable.getDbTableName());

			boolean isTimeOut = false;
			final boolean isTableBeingEditing = metadataDbHandler.isTableBeingEditing(sid, tableName);
			boolean isEditingByCurrentUser = false;
			boolean isTableLocked = false;

			final JSONObject result = new JSONObject();

			String userLogin = "";

			if (isTableBeingEditing) {
				isEditingByCurrentUser = metadataDbHandler.isBeingEditedByCurrentUser(sid, relationKey, userId);
				if (metadataDbHandler.isTimeOut(sid, relationKey, lockTime)) {
					isTimeOut = true;
				}
			}
			if (!isEditingByCurrentUser && (isTableBeingEditing && !isTimeOut)) {
				isTableLocked = true;
				userLogin = metadataDbHandler.getUserLoginById(metadataDbHandler.getOperatingUserId(sid, relationKey));
			}

			result.put("userLogin", userLogin);
			result.put("isTableLocked", isTableLocked);
			result.put("successful", true);

			response.setCharacterEncoding("UTF-8");
			response.setHeader("Content-Type", "application/json");
			respond(response, result.toString());
		} catch (final JSONException e) {
			e.printStackTrace();
		} catch (final SQLException e) {
			e.printStackTrace();
		}
	}
}

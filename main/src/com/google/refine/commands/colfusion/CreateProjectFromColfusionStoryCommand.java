
package com.google.refine.commands.colfusion;

import java.io.IOException;
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONObject;

import com.google.refine.commands.Command;
import com.google.refine.util.ParsingUtilities;

import edu.pitt.sis.exp.colfusion.ColFusionOpenRefineProjectManager;
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
public class CreateProjectFromColfusionStoryCommand extends Command {

	@Override
	public void doGet(final HttpServletRequest request, final HttpServletResponse response) throws IOException, ServletException {

		final Properties parameters = ParsingUtilities.parseUrlParameters(request);

		final int sid = Integer.valueOf(parameters.getProperty("sid"));
		final String tableName = parameters.getProperty("tableName"); //Expecting the original (user friendly table name)
		final int userId = Integer.valueOf(parameters.getProperty("userId"));

		try {
			final ColumnTableInfoManager columnTableMng = new ColumnTableInfoManagerImpl();
			final ColfusionColumnTableInfo columnTable = columnTableMng.findBySidAndOriginalTableName(sid, tableName);
			final RelationKey relationKey = new RelationKey(tableName, columnTable.getDbTableName());

			//            DatabaseHandler databaseHandler = TargetDatabaseHandlerFactory.getTargetDatabaseHandler(sid); // other db
			final MetadataDbHandler metadataDbHandler = DatabaseHandlerFactory.getMetadataDbHandler(); // colfusion db

			final int count = metadataDbHandler.getCountFromOpenRefineHistoryHelper(sid, relationKey);

			if(count < 0) {
				metadataDbHandler.insertIntoOpenRefineHistoryHelper(sid, relationKey);
			} else {
				final int isSaved = metadataDbHandler.getIsSavedFromOpenRefineHistoryHelper(sid, relationKey);
				metadataDbHandler.updateOpenRefineHistoryHelper(sid, relationKey, 0, isSaved);
			}

			final JSONObject result = new JSONObject();

			boolean isTimeOut = false;
			final boolean isTableLocked = metadataDbHandler.isTableLocked(sid, relationKey);
			boolean isEditingByCurrentUser = false;

			if (isTableLocked) {
				isEditingByCurrentUser = metadataDbHandler.isBeingEditedByCurrentUser(sid, relationKey, userId);
				final int lockTime = Integer.valueOf(ConfigManager.getInstance().getProperty(PropertyKeys.COLFUSION_OPENREFINE_LOCK_TIME));
				if (metadataDbHandler.isTimeOut(sid, relationKey, lockTime)) {
					isTimeOut = true;
					metadataDbHandler.releaseTableLock(sid, relationKey);
				}
			}

			if (!isEditingByCurrentUser && (isTableLocked && !isTimeOut)) {

			} else {
				if (!isEditingByCurrentUser) {
					metadataDbHandler.createEditLog(sid, relationKey, userId);
				}

				final ColFusionOpenRefineProjectManager colFusionOpenRefineProjectManager = new ColFusionOpenRefineProjectManager();
				result.put("openrefineURL", colFusionOpenRefineProjectManager.createProjectToOpenRefine(sid, relationKey));
			}
			result.put("isEditing", isTableLocked && !isEditingByCurrentUser);
			result.put("isTimeOut", isTimeOut);
			if(isTableLocked && !isEditingByCurrentUser && !isTimeOut) {
				result.put("msg", "Table is being edited by User: " + metadataDbHandler.getUserLoginById(metadataDbHandler.getOperatingUserId(sid, relationKey)));
			}
			result.put("successful", true);

			response.setCharacterEncoding("UTF-8");
			response.setHeader("Content-Type", "application/json");
			respond(response, result.toString());
		} catch (final Exception e) {
			respondException(response, e);
		}
	}
}

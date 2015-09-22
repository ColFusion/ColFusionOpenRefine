package com.google.refine.commands.colfusion;

import java.io.File;
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
public class IsChangesSavedCommand extends Command {
	@Override
	public void doGet(final HttpServletRequest request, final HttpServletResponse response)
			throws IOException, ServletException {

		final Properties parameters = ParsingUtilities.parseUrlParameters(request);

		final int sid = Integer.valueOf(parameters.getProperty("sid"));
		final String tableName = parameters.getProperty("tableName"); //Expecting original (user friendly table name)

		final MetadataDbHandler metadataDbHandler = DatabaseHandlerFactory.getMetadataDbHandler();

		String projectId;
		final JSONObject result = new JSONObject();

		boolean isChangesSaved = true;

		try {
			final ColumnTableInfoManager columnTableMng = new ColumnTableInfoManagerImpl();
			final ColfusionColumnTableInfo columnTable = columnTableMng.findBySidAndOriginalTableName(sid, tableName);
			final RelationKey relationKey = new RelationKey(tableName, columnTable.getDbTableName());

			projectId = metadataDbHandler.getProjectId(sid, relationKey);

			final ConfigManager configMng = ConfigManager.getInstance();
			final String dir = configMng.getProperty(PropertyKeys.COLFUSION_OPENREFINE_FOLDER);
			final String tempFolder = configMng.getProperty(PropertyKeys.COLFUSION_OPENREFINE_FOLDER_TEMP);

			final String tempDir = dir + tempFolder + File.separator;
			final String projectDir = projectId + ".project" + File.separator;

			final File savedHistory = new File(tempDir + projectDir + "history");
			final String[] savedChanges = savedHistory.list();
			int savedChangesLength = 0;
			if(savedHistory.list() != null) {
				savedChangesLength = savedChanges.length;
			}

			final File notSavedHistory = new File(dir + projectDir + "history");
			final String[] notSavedChanges = notSavedHistory.list();
			int notSavedChangesLength = 0;
			if(notSavedHistory.list() != null) {
				notSavedChangesLength = notSavedChanges.length;
			}

			if(savedChangesLength != notSavedChangesLength) {
				isChangesSaved = false;
			} else if(metadataDbHandler.getCountFromOpenRefineHistoryHelper(sid, relationKey) > 1 && metadataDbHandler.getIsSavedFromOpenRefineHistoryHelper(sid, relationKey) != 1) {
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

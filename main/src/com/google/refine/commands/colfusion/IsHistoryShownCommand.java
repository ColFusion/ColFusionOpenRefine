package com.google.refine.commands.colfusion;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.sql.SQLException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONException;
import org.json.JSONWriter;

import com.google.refine.commands.Command;

import edu.pitt.sis.exp.colfusion.dal.dataModels.tableDataModel.RelationKey;
import edu.pitt.sis.exp.colfusion.dal.databaseHandlers.DatabaseHandlerFactory;
import edu.pitt.sis.exp.colfusion.dal.databaseHandlers.MetadataDbHandler;
import edu.pitt.sis.exp.colfusion.dal.managers.ColumnTableInfoManager;
import edu.pitt.sis.exp.colfusion.dal.managers.ColumnTableInfoManagerImpl;
import edu.pitt.sis.exp.colfusion.dal.orm.ColfusionColumnTableInfo;
import edu.pitt.sis.exp.colfusion.utils.ConfigManager;
import edu.pitt.sis.exp.colfusion.utils.PropertyKeys;

public class IsHistoryShownCommand extends Command {
	@Override
	public void doPost(final HttpServletRequest request, final HttpServletResponse response)
			throws ServletException, IOException {

		boolean isHistoryShown = false;

		final String openrefineUrl = request.getParameter("openrefineUrl");
		final int sid = Integer.valueOf(request.getParameter("sid"));
		final String tableName = request.getParameter("tableName");
		final String projectId = getProjectId(openrefineUrl);

		final ColumnTableInfoManager columnTableMng = new ColumnTableInfoManagerImpl();
		final ColfusionColumnTableInfo columnTable = columnTableMng.findBySidAndOriginalTableName(sid, tableName);
		final RelationKey relationKey = new RelationKey(tableName, columnTable.getDbTableName());

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

		final MetadataDbHandler metadataDbHandler = DatabaseHandlerFactory.getMetadataDbHandler(); // colfusion db
		int count = -1;

		try {
			count = metadataDbHandler.getCountFromOpenRefineHistoryHelper(sid, relationKey);
		} catch (final SQLException e1) {
			e1.printStackTrace();
		}

		if(savedChangesLength != notSavedChangesLength) {
			isHistoryShown = true;
		} else if(count >= 1) {
			isHistoryShown = true;
		}

		try {
			metadataDbHandler.updateOpenRefineHistoryHelper(sid, relationKey, count + 1, 0);
		} catch (final SQLException e1) {
			e1.printStackTrace();
		}

		final Writer w = response.getWriter();
		final JSONWriter writer = new JSONWriter(w);
		try {
			writer.object();

			writer.key("isHistoryShown");
			writer.value(isHistoryShown);
			writer.key("msg");
			writer.value(projectId + " Hey! It works!");
			writer.endObject();
		} catch (final JSONException e) {
			throw new ServletException(e);
		} finally {
			w.flush();
			w.close();
		}
	}

	private String getProjectId(final String url) {
		// 13 is the length of the projectId
		//TODO: maybe we should put "13" into the .properties file
		// FIXME the number 13 is not good, no number should be used
		return url.substring(url.length() - 13);
	}
}

/*

Copyright 2014, xxl
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

 * Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
 * Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following disclaimer
in the documentation and/or other materials provided with the
distribution.
 * Neither the name of Google Inc. nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

 */

package edu.pitt.sis.exp.colfusion;

import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.refine.ProjectManager;
import com.google.refine.ProjectMetadata;
import com.google.refine.io.FileProjectManager;
import com.google.refine.model.Cell;
import com.google.refine.model.Column;
import com.google.refine.model.ModelException;
import com.google.refine.model.Project;
import com.google.refine.model.Row;

import edu.pitt.sis.exp.colfusion.dal.dataModels.tableDataModel.RelationKey;
//import edu.pitt.sis.exp.colfusion.dal.databaseHandlers.DatabaseHandler;
import edu.pitt.sis.exp.colfusion.dal.databaseHandlers.DatabaseHandlerBase;
import edu.pitt.sis.exp.colfusion.dal.databaseHandlers.DatabaseHandlerFactory;
//import edu.pitt.sis.exp.colfusion.dal.databaseHandlers.TargetDatabaseHandlerFactory;
import edu.pitt.sis.exp.colfusion.dal.databaseHandlers.MetadataDbHandler;
import edu.pitt.sis.exp.colfusion.utils.ConfigManager;
import edu.pitt.sis.exp.colfusion.utils.PropertyKeys;


/**
 *
 */
public class ColFusionOpenRefineProjectManager {

	final static Logger logger = LoggerFactory.getLogger(ColFusionOpenRefineProjectManager.class.getName());

	public String createProjectToOpenRefine(final int sid, final RelationKey tableInfo)
			throws Exception {
		ProjectManager.singleton.setBusy(true);
		String url = "";

		final String fileDir = ConfigManager.getInstance().getProperty(PropertyKeys.COLFUSION_OPENREFINE_FOLDER);
		final String openrefineUrl = ConfigManager.getInstance().getProperty(PropertyKeys.COLFUSION_OPENREFINE_URL);
		System.out.println("************************************************");
		System.out.println("OpenRefineUrl PRINT: " + openrefineUrl);
		System.out.println("************************************************");

		System.out.println("BEFORE GET METADATA HANDLER");

		// DatabaseConnectionInfo databaseConnectionInfo = new
		// DatabaseConnectionInfo("127.0.0.1", 3306, "root", "",
		// "colfusion_filetodb_")
		final DatabaseHandlerBase databaseHandler = DatabaseHandlerFactory.getTargetDatabaseHandler(sid);

		final MetadataDbHandler metadataDbHandler = DatabaseHandlerFactory.getMetadataDbHandler();;

		System.out.println("AFTER GET METADATA HANDLER");

		try {
			/*
			 * If there is no temp table, then backup original table to a temp_
			 * table
			 */
			if (databaseHandler.tempTableExist(sid, tableInfo)) {
				databaseHandler.removeTable("temp_" + tableInfo.getDbTableName());
			}

			databaseHandler.createTableFromTable("temp_" + tableInfo.getDbTableName(), tableInfo.getDbTableName());//backupOriginalTable(sid, tableName);
			/*
			 * Create a new project for the table in the database
			 */
			String projectLink = "";

			if (metadataDbHandler.isMapExist(sid, tableInfo)) {
				projectLink = metadataDbHandler.getProjectId(sid, tableInfo);
			} else {
				final Project project = new Project();
				final ProjectMetadata ppm = new ProjectMetadata();

				ppm.setName(tableInfo.getTableName());
				ppm.setEncoding("UTF-8");
				this.setProject(sid, project, databaseHandler, metadataDbHandler, tableInfo);

				//  dataDir); //   String dataDir = Configurations.get("refine.data_dir");

				final File dir = new File(fileDir);


				FileProjectManager.initialize(dir);

				project.update();
				FileProjectManager.singleton.registerProject(project, ppm);
				FileProjectManager.singleton.ensureProjectSaved(project.id);
				/*
				 * ********************************************
				 * The following two lines solve the problem:
				 *
				 * "Only the first time after rerun ColFusionServer, click the "
				 * Edit " button can create a project correctly" I guess the
				 * reason is because without the "save()", some stuff will exist
				 * in cache so that the new created project cannot be stored
				 * correctly
				 */
				project.dispose();
				FileProjectManager.singleton.save(true);
				/*
				 * ********************************************
				 */

				metadataDbHandler.saveRelation(project.id, sid, tableInfo);

				projectLink = project.id + "";
			}

			url = openrefineUrl + projectLink;

			System.out.println(String.format("Turl that will be returned is %s", url));

		} catch (final Exception e) {
			System.out.println(String.format("Something happened"));
			e.printStackTrace();
			throw e;
		} finally {
			ProjectManager.singleton.setBusy(false);
		}
		return url;
	}

	private void setProject(final int sid, final Project project, final DatabaseHandlerBase dbHandler,
			final MetadataDbHandler metadataDbHandler, final RelationKey tableInfo)
					throws SQLException {
		// Get and set column names

		setProjectCol(project, metadataDbHandler.getColumnNames(sid));

		// Get and set rows
		System.out.println(String.format("Table name in ColFusionOpenRefineProjectManager.setProject method is %s", tableInfo.getDbTableName()));

		final int colCount = dbHandler.getColCount(sid, tableInfo);

		System.out.println(String.format("colCount in ColFusionOpenRefineProjectManager.setProject method is %d", colCount));

		setProjectRow(project, dbHandler.getRows(tableInfo, colCount));
	}

	public static void setProjectCol(final Project project, final ArrayList<String> columnNames) {
		for (int i = 0; i < columnNames.size(); i++) {
			final Column column = new Column(i, columnNames.get(i));
			try {
				project.columnModel.addColumn(i, column, true);
			} catch (final ModelException e) {
				e.printStackTrace();
			}
		}
	}

	public static void setProjectRow(final Project project, final ArrayList<ArrayList<String>> rows) {
		for (int j = 0; j < rows.size(); j++) {
			final Row row = new Row(rows.get(j).size());
			for (int k = 0; k < rows.get(j).size(); k++) {
				final Cell cell = new Cell(rows.get(j).get(k), null);

				System.out.println(String.format("Setting row %d, cell %d to value %s", j, k, cell.value.toString()));

				row.setCell(k, cell);

			}
			project.rows.add(row);
		}
	}
}


package com.google.refine.commands.colfusion;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Writer;
import java.util.ArrayList;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONWriter;

import com.google.refine.ProjectManager;
import com.google.refine.commands.Command;
import com.google.refine.model.Project;

import edu.pitt.sis.exp.colfusion.dal.dataModels.tableDataModel.RelationKey;
//import edu.pitt.sis.exp.colfusion.dal.databaseHandlers.DatabaseHandler;
import edu.pitt.sis.exp.colfusion.dal.databaseHandlers.DatabaseHandlerBase;
import edu.pitt.sis.exp.colfusion.dal.databaseHandlers.DatabaseHandlerFactory;
import edu.pitt.sis.exp.colfusion.dal.databaseHandlers.MetadataDbHandler;
import edu.pitt.sis.exp.colfusion.dal.managers.ColumnTableInfoManager;
import edu.pitt.sis.exp.colfusion.dal.managers.ColumnTableInfoManagerImpl;
import edu.pitt.sis.exp.colfusion.dal.orm.ColfusionColumnTableInfo;
//import edu.pitt.sis.exp.colfusion.dal.databaseHandlers.TargetDatabaseHandlerFactory;
import edu.pitt.sis.exp.colfusion.utils.CSVUtils;
import edu.pitt.sis.exp.colfusion.utils.ConfigManager;
import edu.pitt.sis.exp.colfusion.utils.PropertyKeys;

/**
 *
 */
public class SaveProjectDataToDatabaseCommand extends Command  {

	/*
	 * By using this function, the
	 * com.google.refine.myDatabase.DatabaseOperation will not be used anymore,
	 * but just keep that file in case of future use.
	 */
	@Override
	public void doPost(final HttpServletRequest request, final HttpServletResponse response)
			throws ServletException, IOException {
		try {

			final ConfigManager configMng = ConfigManager.getInstance();

			final int lockTime = Integer.valueOf(configMng.getProperty(PropertyKeys.COLFUSION_OPENREFINE_LOCK_TIME));

			final long projectId = Long.parseLong(request.getParameter("projectId"));
			final String colfusionUserId = request.getParameter("colfusionUserId");

			final Project project = ProjectManager.singleton.getProject(projectId);

			int sid = -1;

			String msg = "";

			final MetadataDbHandler metadataDbHandler = DatabaseHandlerFactory.getMetadataDbHandler();
			DatabaseHandlerBase databaseHandler = null;

			sid = metadataDbHandler.getSid(String.valueOf(projectId));

			final ColumnTableInfoManager columnTableMng = new ColumnTableInfoManagerImpl();
			final ColfusionColumnTableInfo columnTable = columnTableMng.findBySidAndOriginalTableName(sid, project.getMetadata().getName());
			final RelationKey relationKey = new RelationKey(project.getMetadata().getName(), columnTable.getDbTableName());

			databaseHandler = DatabaseHandlerFactory.getTargetDatabaseHandler(sid);
			//			tableName = metadataDbHandler.getTableName(sid);

			if (metadataDbHandler.isTimeOutForCurrentUser(sid, relationKey, Integer.valueOf(colfusionUserId), lockTime)) {
				msg = "Time is out, cannot save!";
			}
			else {
				final int count = metadataDbHandler.getCountFromOpenRefineHistoryHelper(sid, relationKey);
				metadataDbHandler.updateOpenRefineHistoryHelper(sid, relationKey, count, 1);
				/*
				 * ***********update checkpoint************************
				 * Only the "Save" is valid, copy files to temp folder
				 */

				final String dir = configMng.getProperty(PropertyKeys.COLFUSION_OPENREFINE_FOLDER);
				final String tempFolder = configMng.getProperty(PropertyKeys.COLFUSION_OPENREFINE_FOLDER_TEMP);

				final String tempDir = dir + tempFolder + File.separator;
				final String projectDir = projectId + ".project" + File.separator;

				final File folderPath = new File(tempDir + projectDir);
				deleteAllFilesOfDir(folderPath);

				copyFolder(dir + projectDir, tempDir + projectDir);
				/*
				 * *****************************************************
				 */
				// Get column names from project
				final ArrayList<String> columnNames = new ArrayList<>();
				for (final String s : project.columnModel.getColumnNames()) {
					columnNames.add(s);
				}

				/*
				 * ****************Save data into database*****************
				 */
				// 0. Remove original table

				databaseHandler.removeTable(relationKey.getDbTableName());

				//                // 1. Create table
				//                String tableCreateQuery = "CREATE TABLE " + tableName + " (";
				//                for (int i = 0; i < columnNames.size(); i++) {
				//                    if (i < columnNames.size() - 1) {
				//                        tableCreateQuery += headlineControl(columnNames.get(i)) + " VARCHAR(255), ";
				//                    } else {
				//                        tableCreateQuery += headlineControl(columnNames.get(i)) + " VARCHAR(255))";
				//                    }
				//                }

				databaseHandler.createTableIfNotExist(relationKey, columnNames);

				//                databaseHandler.createOriginalTable(tableCreateQuery, sid, tableName);

				// Get each row from project
				ArrayList<ArrayList<String>> rows = new ArrayList<>();
				rows = getReorderedRows(project);

				/*
				 * *************test csv begin**********************
				 */
				final ArrayList<String> fileRows = new ArrayList<>();
				for (int j = 0; j < rows.size(); j++) {
					String tempRow = "";
					for (int k = 0; k < columnNames.size(); k++) {
						if(k != columnNames.size() - 1) {
							tempRow += rows.get(j).get(k) + ",";
						} else {
							tempRow += rows.get(j).get(k);
						}
					}
					fileRows.add(tempRow);
				}

				final String csvDir = ConfigManager.getInstance().getProperty(PropertyKeys.COLFUSION_OPENREFINE_CSV_FILE_DIR);
				final String csvFileName = ConfigManager.getInstance().getProperty(PropertyKeys.COLFUSION_OPENREFINE_CSV_FILE_NAME);

				final boolean isSuccess = CSVUtils.exportCsv(new File(dir + projectDir + csvFileName), fileRows);

				if(isSuccess) {
					databaseHandler.importCsvToTable(csvDir + projectId + ".project" + File.separator + csvFileName, relationKey);
				} else {
					logger.info("csv saving failed!");
				}

				final ArrayList<Integer> cids;
				msg = "Changes have been saved!";
			}

			final Writer w = response.getWriter();
			final JSONWriter writer = new JSONWriter(w);

			writer.object();

			writer.key("msg");
			writer.value(msg);
			writer.endObject();
		} catch (final Exception e) {
			respondException(response, e);
		}
		//        finally {
		//            w.flush();
		//            w.close();
		//        }
	}

	public static ArrayList<ArrayList<String>> getReorderedRows(final Project project) {
		final ArrayList<ArrayList<String>> rows = new ArrayList<ArrayList<String>>();
		if (project.columnModel.columns.isEmpty() || project.rows.isEmpty()) {
			return rows;
		} else {
			for (int i = 0; i < project.rows.size(); i++) {
				rows.add(new ArrayList<String>());
				for (int j = 0; j < project.columnModel.columns.size(); j++) {
					if (project.columnModel.columns.get(j).getCellIndex() > (project.rows.get(i).cells.size() - 1)) {
						rows.get(i).add("");
					} else {
						if (project.rows.get(i).cells.get(project.columnModel.columns.get(j).getCellIndex()) == null) {
							rows.get(i).add("");
						} else {
							rows.get(i).add(
									project.rows.get(i).cells.get(project.columnModel.columns.get(j).getCellIndex())
									.toString());
						}
					}
				}
			}
			return rows;
		}

	}

	private void copyFile(final String sourceFile, final String targetFile) {
		try {
			int byteread = 0;
			final File oldfile = new File(sourceFile);
			if (oldfile.exists()) {
				final InputStream inStream = new FileInputStream(sourceFile);
				@SuppressWarnings("resource")
				final
				FileOutputStream fs = new FileOutputStream(targetFile);
				final byte[] buffer = new byte[1444];
				while ((byteread = inStream.read(buffer)) != -1) {
					fs.write(buffer, 0, byteread);
				}
				inStream.close();
			}
		} catch (final Exception e) {
			System.out.println("Error happens when copying files");
			e.printStackTrace();

		}
	}

	private void copyFolder(final String sourceDir, final String targetDir) {
		try {
			(new File(targetDir)).mkdirs();
			final File a = new File(sourceDir);
			final String[] file = a.list();
			File temp = null;
			for (int i = 0; i < file.length; i++) {
				if (sourceDir.endsWith(File.separator)) {
					temp = new File(sourceDir + file[i]);
				} else {
					temp = new File(sourceDir + File.separator + file[i]);
				}

				if (temp.isFile()) {
					final FileInputStream input = new FileInputStream(temp);
					final FileOutputStream output = new FileOutputStream(targetDir + "/" + (temp.getName()).toString());
					final byte[] b = new byte[1024 * 5];
					int len;
					while ((len = input.read(b)) != -1) {
						output.write(b, 0, len);
					}
					output.flush();
					output.close();
					input.close();
				}
				if (temp.isDirectory()) {
					copyFolder(sourceDir + "/" + file[i], targetDir + "/" + file[i]);
				}
			}
		} catch (final Exception e) {
			System.out.println("Error happens when copying files in folder");
			e.printStackTrace();

		}
	}

	private void copyZipFile(final String sourceZipFile, final String targetZipFile) {
		InputStream inStream = null;
		try {
			inStream = new FileInputStream(sourceZipFile);
		} catch (final FileNotFoundException e) {
			System.err.println("" + sourceZipFile + "" + "\r\n" + e.getCause());
			return;
		}
		final File targetFile = new File(targetZipFile);
		OutputStream outStream = null;
		try {
			targetFile.createNewFile();
			outStream = new FileOutputStream(targetFile);
			final byte[] by = new byte[1024];
			while (inStream.available() > 0) {
				inStream.read(by);
				outStream.write(by);
			}
		} catch (final IOException e) {
			System.err.println("Error happens when create file [" + targetZipFile + "]" + "\r\n" + e.getCause());
		} finally {
			if (null != inStream) {
				try {
					inStream.close();
				} catch (final IOException e) {
					System.err.println(e.getCause());
				}
			}
			if (null != outStream) {
				try {
					outStream.flush();
				} catch (final IOException e) {
					System.err.println(e.getCause());
				}
				try {
					outStream.close();
				} catch (final IOException e) {
					System.err.println(e.getCause());
				}
			}
		}
	}

	public void deleteAllFilesOfDir(final File path) {
		if (!path.exists()) {
			return;
		}
		if (path.isFile()) {
			path.delete();
			return;
		}
		final File[] files = path.listFiles();
		for (int i = 0; i < files.length; i++) {
			deleteAllFilesOfDir(files[i]);
		}
		path.delete();
	}

	private String removeSpace(final String str) {
		String result = "";
		for(int i = 0; i < str.length(); i++) {
			if(str.charAt(i) != ' ') {
				result += str.charAt(i);
			}
		}
		return result;
	}
}

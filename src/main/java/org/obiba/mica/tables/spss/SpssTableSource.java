/*
 * Copyright (c) 2022 OBiBa. All rights reserved.
 *
 * This program and the accompanying materials
 * are made available under the terms of the GNU Public License v3.0.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.obiba.mica.tables.spss;

import com.google.common.base.Strings;
import org.obiba.core.util.FileUtil;
import org.obiba.magma.Disposable;
import org.obiba.magma.ValueTable;
import org.obiba.magma.datasource.spss.SpssDatasource;
import org.obiba.magma.datasource.spss.support.SpssDatasourceFactory;
import org.obiba.magma.support.Initialisables;
import org.obiba.mica.spi.tables.AbstractStudyTableSource;
import org.obiba.mica.spi.tables.IVariable;
import org.obiba.mica.spi.tables.StudyTableFileSource;
import org.obiba.mica.spi.tables.StudyTableFileStreamProvider;
import org.obiba.mica.web.model.Mica;

import javax.validation.constraints.NotNull;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

public class SpssTableSource extends AbstractStudyTableSource implements StudyTableFileSource, Disposable {

  @NotNull
  private String path;

  private String table;

  private Properties properties;

  private boolean initialized;

  private SpssDatasource spssDatasource;

  private StudyTableFileStreamProvider fileStreamProvider;

  private Path tmpdir;

  public static SpssTableSource fromURN(String source) {
    if (Strings.isNullOrEmpty(source) || !source.startsWith("urn:file:"))
      throw new IllegalArgumentException("Not a valid SPSS table source URN: " + source);

    String fullName = source.replace("urn:file:", "");
    int sep = fullName.lastIndexOf(":");
    String file = sep > 0 ? fullName.substring(0, sep) : fullName;
    String table = sep > 0 ? fullName.substring(sep + 1) : null;
    return SpssTableSource.newSource(file, table);
  }

  private static SpssTableSource newSource(String path, String table) {
    SpssTableSource source = new SpssTableSource();
    source.path = path;
    source.table = table;
    return source;
  }

  public void configure(Properties properties) {
    this.properties = properties;
  }


  @Override
  public String getPath() {
    return path;
  }

  @Override
  public void setStudyTableFileStreamProvider(StudyTableFileStreamProvider in) {
    this.fileStreamProvider = in;
    // deferred init
    this.initialized = false;
  }

  @Override
  public ValueTable getValueTable() {
    ensureInitialized();
    return Strings.isNullOrEmpty(table) ? spssDatasource.getValueTables().stream().findFirst().get() : spssDatasource.getValueTable(table);
  }

  @Override
  public Mica.DatasetVariableContingencyDto getContingency(IVariable variable, IVariable crossVariable) {
    throw new UnsupportedOperationException("Contingency search not available from a SPSS file");
  }

  @Override
  public Mica.DatasetVariableAggregationDto getVariableSummary(String variableName) {
    throw new UnsupportedOperationException("Summary statistics not available from a SPSS file");
  }

  @Override
  public String getURN() {
    return Strings.isNullOrEmpty(table) ? String.format("urn:file:%s", path) : String.format("urn:file:%s:%s", path, table);
  }

  private void ensureInitialized() {
    if (!initialized) {
      SpssDatasourceFactory spssDatasourceFactory = new SpssDatasourceFactory();
      spssDatasourceFactory.setName(path);
      spssDatasourceFactory.setEntityType(properties.getProperty("entity_type", "Participant"));
      spssDatasourceFactory.setCharacterSet(properties.getProperty("charset", "ISO-8859-1"));
      spssDatasourceFactory.setLocale(properties.getProperty("locale", "en"));
      spssDatasourceFactory.setIdVariable(properties.getProperty("id_variable", ""));
      try (InputStream inputStream = fileStreamProvider.getInputStream()) {
        tmpdir = Files.createTempDirectory(Paths.get(properties.getProperty("work.dir")), "");
        tmpdir.toFile().deleteOnExit();
        File targetFile = new File(tmpdir.toFile(), table + ".sav");
        Files.copy(inputStream, targetFile.toPath());
        spssDatasourceFactory.setFile(targetFile);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      this.spssDatasource = (SpssDatasource) spssDatasourceFactory.create();

      Initialisables.initialise(spssDatasource);
      initialized = true;
    }
  }

  @Override
  public void dispose() {
    if (tmpdir != null) {
      File tmpFile = tmpdir.toFile();
      if (tmpFile.exists()) {
        try {
          FileUtil.delete(tmpdir.toFile());
        } catch (IOException e) {
          // ignore
        }
      }
    }
  }
}

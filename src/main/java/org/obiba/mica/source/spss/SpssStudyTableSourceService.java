/*
 * Copyright (c) 2022 OBiBa. All rights reserved.
 *
 * This program and the accompanying materials
 * are made available under the terms of the GNU Public License v3.0.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.obiba.mica.source.spss;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import org.obiba.mica.spi.source.AbstractStudyTableSourceService;
import org.obiba.mica.spi.source.StudyTableSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


public class SpssStudyTableSourceService extends AbstractStudyTableSourceService {

  private static final Logger log = LoggerFactory.getLogger(SpssStudyTableSourceService.class);

  @Override
  public boolean isFor(String source) {
    if (Strings.isNullOrEmpty(source) || !source.startsWith("urn:file:"))
      return false;
    List<String> tokens = Splitter.on(":").splitToList(source);
    return tokens.size() > 2 && tokens.get(2).toLowerCase().endsWith(".sav");
  }

  @Override
  public StudyTableSource makeSource(String source) {
    SpssTableSource tableSource = SpssTableSource.fromURN(source);
    tableSource.configure(properties);
    return tableSource;
  }

  @Override
  public String getName() {
    return "mica-source-spss";
  }

}

package com.slack.kaldb.logstore;

import static org.assertj.core.api.Assertions.assertThat;

import com.slack.kaldb.logstore.LogMessage.ReservedField;
import com.slack.kaldb.logstore.LogMessage.SystemField;
import org.junit.Test;

public class LogMessageTest {

  @Test
  public void testSystemField() {
    assertThat(SystemField.values().length).isEqualTo(5);
    assertThat(SystemField.systemFieldNames.size()).isEqualTo(5);
    assertThat(SystemField.isSystemField("_source")).isTrue();
    assertThat(SystemField.TIME_SINCE_EPOCH.fieldName).isEqualTo("_timesinceepoch");
    assertThat(SystemField.ALL.fieldName).isEqualTo("_all");
    assertThat(SystemField.ID.fieldName).isEqualTo("_id");
    assertThat(SystemField.INDEX.fieldName).isEqualTo("_index");
    for (SystemField f : SystemField.values()) {
      String lowerCaseName = f.fieldName.toLowerCase();
      if (!f.equals(SystemField.TIME_SINCE_EPOCH))
        assertThat(f.fieldName.equals(lowerCaseName) || f.fieldName.equals("_" + lowerCaseName))
            .isTrue();
    }
  }

  @Test
  public void testReservedField() {
    assertThat(ReservedField.values().length).isEqualTo(13);
    assertThat(ReservedField.reservedFieldNames.size()).isEqualTo(13);
    assertThat(ReservedField.isReservedField("hostname")).isTrue();
    assertThat(ReservedField.TIMESTAMP.fieldName).isEqualTo("@timestamp");
    for (LogMessage.ReservedField f : LogMessage.ReservedField.values()) {
      if (!f.equals(LogMessage.ReservedField.TIMESTAMP)) {
        assertThat(f.name().toLowerCase()).isEqualTo(f.fieldName);
      }
    }
    assertThat(LogMessage.ReservedField.isReservedField("test")).isFalse();
  }
}

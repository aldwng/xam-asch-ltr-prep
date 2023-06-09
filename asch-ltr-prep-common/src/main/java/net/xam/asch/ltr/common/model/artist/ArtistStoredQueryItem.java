package net.xam.asch.ltr.common.model.artist;

import java.io.Serializable;
import lombok.Data;
import lombok.AllArgsConstructor;

@Data
@AllArgsConstructor
public class ArtistStoredQueryItem implements Serializable {

  private String songSlot;
  private String artistSlot;
  private long queryCount;
}

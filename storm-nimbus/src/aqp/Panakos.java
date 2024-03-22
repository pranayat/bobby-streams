package aqp;

import java.util.Arrays;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Panakos {
  CountMinSketch cmSketch;
  Integer bitmapLen;
  private int[] bitmap;
  
  public Panakos() {
    this.cmSketch = new CountMinSketch(10, 3); // TODO decide these
    this.bitmapLen = 10000;
    this.bitmap = new int[bitmapLen];
    Arrays.fill(bitmap, 0);
  }

  public void remove(Object x, double value) throws NoSuchAlgorithmException {

    int bitPosition = _hash(x);
    if (bitmap[bitPosition] > 0) {
        bitmap[bitPosition]--;
        return;
    }

    // bitmap is reset to 0, remove from cmSketch now
    cmSketch.add(x, -value);
  }

  public void add(Object x, double value) throws NoSuchAlgorithmException {
    int bitPosition = _hash(x);
    if (bitmap[bitPosition] < 2) {
        bitmap[bitPosition]++;
        return;
    }

    if (bitmap[bitPosition] == 2) {
        bitmap[bitPosition]++; // bitmap now 3, indicating overflow
        return;
    }

    // bitmap already overflowed, add to cmSketch now
    cmSketch.add(x, value);
  }

  public int query(Object x) throws NoSuchAlgorithmException {
    int bitPosition = _hash(x);
    if (bitmap[bitPosition] < 3) {
        return bitmap[bitPosition];
    }
    return 2 + cmSketch.query(x);
  }

  private int _hash(Object x) {
      try {
          MessageDigest md = MessageDigest.getInstance("MD5");
          byte[] messageDigest = md.digest(BigInteger.valueOf(x.hashCode()).toByteArray());
          BigInteger no = new BigInteger(1, messageDigest);
          return no.mod(BigInteger.valueOf(bitmapLen)).intValue();
      } catch (NoSuchAlgorithmException e) {
          e.printStackTrace();
          return -1; // Handle error appropriately
      }
  }  
}

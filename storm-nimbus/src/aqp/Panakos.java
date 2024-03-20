package aqp;

import java.security.NoSuchAlgorithmException;

public class Panakos {
  CountMinSketch cmSketch;
  
  public Panakos() {
    this.cmSketch = new CountMinSketch(10, 3); // TODO decide these
  }

  public void add(Object x, double value) throws NoSuchAlgorithmException {
    this.cmSketch.add(x, value);
  }

  public void add(Object x) throws NoSuchAlgorithmException {
    this.cmSketch.add(x, 1);
  }

  public int query(Object x) throws NoSuchAlgorithmException {
    return this.cmSketch.query(x);
  }
}

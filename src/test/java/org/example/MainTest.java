package org.example;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MainTest {

  @Test
  public void test() {
    Assertions.assertTrue(true);
  }

  @Test
  public void test2() {
    Assertions.assertFalse(false);
  }


  @Test
  public void test3() {
    Assertions.assertEquals(0, 0);
  }

  @Test
  public void test4() {
    Assertions.assertEquals(1, 1);
  }

  @Test
  public void failing() {
    Assertions.fail("Fails on purpose");
  }
}

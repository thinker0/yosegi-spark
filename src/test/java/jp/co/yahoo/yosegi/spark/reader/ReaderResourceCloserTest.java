/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jp.co.yahoo.yosegi.spark.reader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

public class ReaderResourceCloserTest {

  @Test
  public void executesCleanupWhenThereIsAlreadyAFailure() {
    final Exception primary = new IOExceptionForTest("read failed");
    final AtomicInteger calls = new AtomicInteger();

    final Exception result =
        ReaderResourceCloser.close(primary, () -> calls.incrementAndGet());

    assertSame(primary, result);
    assertEquals(1, calls.get());
    assertEquals(0, primary.getSuppressed().length);
  }

  @Test
  public void preservesPrimaryFailureAndSuppressesCloseFailure() {
    final Exception primary = new IOExceptionForTest("read failed");
    final Exception closeFailure = new IOExceptionForTest("close failed");

    final Exception result =
        ReaderResourceCloser.close(
            primary,
            () -> {
              throw closeFailure;
            });

    assertSame(primary, result);
    assertEquals(1, primary.getSuppressed().length);
    assertSame(closeFailure, primary.getSuppressed()[0]);
  }

  @Test
  public void returnsCloseFailureWhenThereIsNoPrimaryFailure() {
    final Exception closeFailure = new IOExceptionForTest("close failed");

    final Exception result =
        ReaderResourceCloser.close(
            null,
            () -> {
              throw closeFailure;
            });

    assertSame(closeFailure, result);
  }

  private static class IOExceptionForTest extends Exception {
    IOExceptionForTest(final String message) {
      super(message);
    }
  }
}

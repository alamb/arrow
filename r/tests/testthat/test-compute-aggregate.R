# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

context("compute: aggregation")

test_that("sum.Array", {
  ints <- 1:5
  a <- Array$create(ints)
  expect_is(sum(a), "Scalar")
  expect_identical(as.integer(sum(a)), sum(ints))

  floats <- c(1.3, 2.4, 3)
  f <- Array$create(floats)
  expect_identical(as.numeric(sum(f)), sum(floats))

  floats <- c(floats, NA)
  na <- Array$create(floats)
  expect_identical(as.numeric(sum(na)), sum(floats))
  expect_is(sum(na, na.rm = TRUE), "Scalar")
  expect_identical(as.numeric(sum(na, na.rm = TRUE)), sum(floats, na.rm = TRUE))

  bools <- c(TRUE, NA, TRUE, FALSE)
  b <- Array$create(bools)
  expect_identical(as.integer(sum(b)), sum(bools))
  expect_identical(as.integer(sum(b, na.rm = TRUE)), sum(bools, na.rm = TRUE))
})

test_that("sum.ChunkedArray", {
  a <- ChunkedArray$create(1:4, c(1:4, NA), 1:5)
  expect_is(sum(a), "Scalar")
  expect_true(is.na(as.vector(sum(a))))
  expect_identical(as.numeric(sum(a, na.rm = TRUE)), 35)
})

test_that("sum dots", {
  a1 <- Array$create(1:4)
  a2 <- ChunkedArray$create(1:4, c(1:4, NA), 1:5)
  expect_identical(as.numeric(sum(a1, a2, na.rm = TRUE)), 45)
})

test_that("sum.Scalar", {
  skip("No sum method in arrow for Scalar: ARROW-9056")
  s <- Scalar$create(4)
  expect_identical(as.numeric(s), as.numeric(sum(s)))
})

test_that("mean.Array", {
  ints <- 1:4
  a <- Array$create(ints)
  expect_is(mean(a), "Scalar")
  expect_identical(as.vector(mean(a)), mean(ints))

  floats <- c(1.3, 2.4, 3)
  f <- Array$create(floats)
  expect_identical(as.vector(mean(f)), mean(floats))

  floats <- c(floats, NA)
  na <- Array$create(floats)
  expect_identical(as.vector(mean(na)), mean(floats))
  expect_is(mean(na, na.rm = TRUE), "Scalar")
  expect_identical(as.vector(mean(na, na.rm = TRUE)), mean(floats, na.rm = TRUE))

  bools <- c(TRUE, NA, TRUE, FALSE)
  b <- Array$create(bools)
  expect_identical(as.vector(mean(b)), mean(bools))
  expect_identical(as.integer(sum(b, na.rm = TRUE)), sum(bools, na.rm = TRUE))
})

test_that("mean.ChunkedArray", {
  a <- ChunkedArray$create(1:4, c(1:4, NA), 1:5)
  expect_is(mean(a), "Scalar")
  expect_true(is.na(as.vector(mean(a))))
  expect_identical(as.vector(mean(a, na.rm = TRUE)), 35/13)
})

test_that("mean.Scalar", {
  skip("No mean method in arrow for Scalar: ARROW-9056")
  s <- Scalar$create(4)
  expect_identical(as.vector(s), mean(s))
})

test_that("Bad input handling of call_function", {
  expect_error(
    call_function("sum", 2, 3),
    'Argument 1 is of class numeric but it must be one of "Array", "ChunkedArray", "RecordBatch", "Table", or "Scalar"'
  )
})

test_that("min.Array", {
  ints <- 1:4
  a <- Array$create(ints)
  expect_is(min(a), "Scalar")
  expect_identical(as.vector(min(a)), min(ints))

  floats <- c(1.3, 3, 2.4)
  f <- Array$create(floats)
  expect_identical(as.vector(min(f)), min(floats))

  floats <- c(floats, NA)
  na <- Array$create(floats)
  expect_identical(as.vector(min(na)), min(floats))
  expect_is(min(na, na.rm = TRUE), "Scalar")
  expect_identical(as.vector(min(na, na.rm = TRUE)), min(floats, na.rm = TRUE))

  bools <- c(TRUE, TRUE, FALSE)
  b <- Array$create(bools)
  # R is inconsistent here: typeof(min(NA)) == "integer", not "logical"
  expect_identical(as.vector(min(b)), as.logical(min(bools)))
})

test_that("max.Array", {
  ints <- 1:4
  a <- Array$create(ints)
  expect_is(max(a), "Scalar")
  expect_identical(as.vector(max(a)), max(ints))

  floats <- c(1.3, 3, 2.4)
  f <- Array$create(floats)
  expect_identical(as.vector(max(f)), max(floats))

  floats <- c(floats, NA)
  na <- Array$create(floats)
  expect_identical(as.vector(max(na)), max(floats))
  expect_is(max(na, na.rm = TRUE), "Scalar")
  expect_identical(as.vector(max(na, na.rm = TRUE)), max(floats, na.rm = TRUE))

  bools <- c(TRUE, TRUE, FALSE)
  b <- Array$create(bools)
  # R is inconsistent here: typeof(max(NA)) == "integer", not "logical"
  expect_identical(as.vector(max(b)), as.logical(max(bools)))
})

test_that("min.ChunkedArray", {
  ints <- 1:4
  a <- ChunkedArray$create(ints)
  expect_is(min(a), "Scalar")
  expect_identical(as.vector(min(a)), min(ints))

  floats <- c(1.3, 3, 2.4)
  f <- ChunkedArray$create(floats)
  expect_identical(as.vector(min(f)), min(floats))

  floats <- c(floats, NA)
  na <- ChunkedArray$create(floats)
  expect_identical(as.vector(min(na)), min(floats))
  expect_is(min(na, na.rm = TRUE), "Scalar")
  expect_identical(as.vector(min(na, na.rm = TRUE)), min(floats, na.rm = TRUE))

  bools <- c(TRUE, TRUE, FALSE)
  b <- ChunkedArray$create(bools)
  # R is inconsistent here: typeof(min(NA)) == "integer", not "logical"
  expect_identical(as.vector(min(b)), as.logical(min(bools)))
})

test_that("max.ChunkedArray", {
  ints <- 1:4
  a <- ChunkedArray$create(ints)
  expect_is(max(a), "Scalar")
  expect_identical(as.vector(max(a)), max(ints))

  floats <- c(1.3, 3, 2.4)
  f <- ChunkedArray$create(floats)
  expect_identical(as.vector(max(f)), max(floats))

  floats <- c(floats, NA)
  na <- ChunkedArray$create(floats)
  expect_identical(as.vector(max(na)), max(floats))
  expect_is(max(na, na.rm = TRUE), "Scalar")
  expect_identical(as.vector(max(na, na.rm = TRUE)), max(floats, na.rm = TRUE))

  bools <- c(TRUE, TRUE, FALSE)
  b <- ChunkedArray$create(bools)
  # R is inconsistent here: typeof(max(NA)) == "integer", not "logical"
  expect_identical(as.vector(max(b)), as.logical(max(bools)))
})

test_that("Edge cases", {
  skip("ARROW-9054")
  a <- Array$create(NA)
  for (type in c(int32(), float64(), bool())) {
    expect_equal(as.vector(sum(a$cast(type), na.rm = TRUE)), sum(NA, na.rm = TRUE))
    expect_equal(as.vector(mean(a$cast(type), na.rm = TRUE)), mean(NA, na.rm = TRUE))
    expect_equal(as.vector(min(a$cast(type), na.rm = TRUE)), min(NA, na.rm = TRUE))
    expect_equal(as.vector(max(a$cast(type), na.rm = TRUE)), max(NA, na.rm = TRUE))
  }
})

test_that("unique.Array", {
  a <- Array$create(c(1, 4, 3, 1, 1, 3, 4))
  expect_equal(unique(a), Array$create(c(1, 4, 3)))
  ca <- ChunkedArray$create(a, a)
  expect_equal(unique(ca), Array$create(c(1, 4, 3)))
})

test_that("match_arrow", {
  a <- Array$create(c(1, 4, 3, 1, 1, 3, 4))
  tab <- c(4, 3, 2, 1)
  expect_equal(match_arrow(a, tab), Array$create(c(3L, 0L, 1L, 3L, 3L, 1L, 0L)))

  ca <- ChunkedArray$create(c(1, 4, 3, 1, 1, 3, 4))
  expect_equal(match_arrow(ca, tab), ChunkedArray$create(c(3L, 0L, 1L, 3L, 3L, 1L, 0L)))
})

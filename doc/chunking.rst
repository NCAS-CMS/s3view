.. _chunking:

Determining Optimal Chunk Shapes
===========================================

``cfs3`` provides a simple algorithm to determines suitable chunkshapes for rechunking
an existing :class:`cf.Field`.

It consists of two main stages:

1. A **generic chunk sizing** phase implemented in :func:`get_chunkshape`.
2. A **time-aware optimization** phase implemented in :func:`get_optimal_chunkshape`.

(The first of these can be used in any situation, not just with CF fields.)

The aim is to produce chunk shapes that:
- Closely match a desired target volume (in bytes),
- Divide the dataset dimensions evenly,
- Respect domain-specific time patterns (e.g. daily, monthly, hourly data).

The pair of functions start by deriving a mathematically reasonable chunking pattern based on
data shape and desired volume, then tunes the pattern to respect semantic knowledge of temporal
data. The result is a storage-efficient, analysis-friendly chunk structure suitable for
array formats such as HDF5, Zarr, or NetCDF.

This algorithm is implemented by default as part of :class:`cfs3.cfuploader`.

---

get_chunkshape
---------------

.. function:: get_chunkshape(shape, volume, word_size=4, logger=None, scale_tol=0.8)

   Compute a generic chunk shape for a given multidimensional dataset shape and desired
   chunk volume (in bytes).

   **Purpose**

   This function calculates divisors of each dimension in ``shape`` such that the product of
   the resulting chunk shape approximates the requested ``volume``. It balances proportional
   scaling between dimensions while ensuring that each chunk divides the dataset evenly.

   **Parameters**

   :param tuple shape: The full data shape (e.g. ``(z, y, x)``).
   :param int volume: Desired chunk size in bytes.
   :param int word_size: Byte size of one data element (default: 4).
   :param logger: Optional logging object for diagnostics.
   :param float scale_tol: Tolerance for undershooting the target volume (default: 0.8).

   **Algorithm Overview**

   1. **Compute volume per element** and the number of chunks implied by the dataset size.
   2. **Estimate initial scaling**: derive an approximate “root” dimension size such that
      the total chunk volume ≈ ``volume``.
   3. **Proportionally scale dimensions** using their relative aspect ratios.
   4. **Iteratively refine guesses**:
      - For each dimension, round down to the nearest integer divisor of that dimension.
      - Adjust scaling weights for remaining dimensions to compensate.
   5. **Final adjustment**: if the resulting chunk is smaller than desired beyond the
      tolerance ``scale_tol``, scale up the last dimension slightly.
   6. **Return the computed chunk shape** as a list of integers.

   **Helper Functions**

   - ``constrained_largest_divisor(number, constraint)``  
     Finds the largest divisor of ``number`` less than ``constraint``.
   - ``revise(dimension, guess)``  
     Adjusts a guessed chunk size downward to a valid divisor and provides a scale factor
     for subsequent dimension adjustments.

   **Returns**

   :return: List of integers representing the chunk shape.
   :rtype: list[int]

   **Example**

   .. code-block:: python

      shape = (512, 512, 256)
      volume = 1024 * 1024  # 1 MB target
      chunkshape = get_chunkshape(shape, volume)
      print(chunkshape)
      # e.g. [64, 64, 64]

---

get_optimal_chunkshape
----------------------

.. function:: get_optimal_chunkshape(f, volume, word_size=4, logger=None)

   Refine the generic chunk shape produced by :func:`get_chunkshape` using temporal metadata
   from a CF-compliant field.

   **Purpose**

   Many climate and forecast (CF) datasets contain a *time* dimension with known frequency
   (e.g., hourly, daily, monthly). This routine adjusts the time dimension’s chunk length to
   align with natural temporal multiples — improving performance for time-based subsetting
   and reducing redundant chunk reads.

   **Parameters**

   :param f: CF field object providing data and coordinate metadata.
   :param int volume: Target chunk size in bytes.
   :param int word_size: Byte size of each data element (default: 4).
   :param logger: Optional logging object for reporting adjustments.

   **Time Interval Rules**

   Based on the time coordinate spacing, the algorithm assumes:

   - **Hourly data** → use chunk sizes that are multiples of 12  
     (e.g. 12, 24, 48 for common synoptic intervals)
   - **Sub-daily data** → divide 24 by the interval and use that multiple
   - **Daily data** → use multiples of 10
   - **Monthly data** → use multiples of 12

   **Algorithm Steps**

   1. Compute a *default* chunk shape via :func:`get_chunkshape`.
   2. Identify the time coordinate (raises :class:`ValueError` if none is found).
   3. Determine the time interval (hourly, daily, monthly, etc.) from the coordinate spacing
      and units.
   4. Adjust the chunk size along the time axis according to the rules above.
   5. Log any changes and return the modified chunk shape.

   **Returns**

   :return: List of integers representing the time-aware chunk shape.
   :rtype: list[int]

   **Example**

   .. code-block:: python

      # CF field with daily time coordinate
      optimal = get_optimal_chunkshape(field, volume=1_048_576)
      print(optimal)
      # e.g. [10, 64, 64]

---


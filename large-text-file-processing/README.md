# Problem overview

Develop a program to solve below task using Java or your preferred programing language, and provide your sample input file and output file together with your source code.

> Given a very large text file that may not fit in available memory, create a file that contains the distinct words from the original file sorted in the ascending order.

Note: feel free to make assumptions on the information not provided.

# Solution overview

Such a problem is an excellent fit for solving using Apache Spark, a cluster computing platform designed to be fast and general purpose. The solution is a Python Spark application that can be run using `spark-submit`.

# Prerequisites

* Download the Spark 2.2.1 distribution from https://spark.apache.org/downloads.html and untar it.
* Update the PATH variable to point to the bin directory of the distro.
* Java is installed and `JAVA_HOME` environment variable is set.
* Python 2.7 is installed and all packages used by the solution are installed.

# Running the solution

From this directory run the solution as

```
spark-submit distinct_sorted_words.py input/<input-file>
```

The output file will be available under `input/<input-file>_sorted/part-00000`.

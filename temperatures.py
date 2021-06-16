# Databricks notebook source
# MAGIC %md
# MAGIC **NOTE**: the following cell _**should**_ fail.
# MAGIC 
# MAGIC Relative imports of Python libraries are currently not supported. ([Custom libraries can be uploaded to the workspace or installed from PyPi](https://docs.databricks.com/libraries/workspace-libraries.html).)

# COMMAND ----------

from my_lib.my_funcs import *

# COMMAND ----------

# MAGIC %md
# MAGIC Using `%run` allows you to execute a Databricks notebook in the current SparkSession, bringing any imported modules, declared variables, or defined functions into the current scope.
# MAGIC 
# MAGIC Note that Databricks Python notebooks are stored as normal Python files with the first line
# MAGIC 
# MAGIC ```
# MAGIC # Databricks notebook source
# MAGIC ```
# MAGIC 
# MAGIC The Databricks web app searches for this line when syncing changes with the remote repository and will render Python scripts as single cell notebooks automatically.

# COMMAND ----------

# MAGIC %run ./my_lib/my_funcs

# COMMAND ----------

# MAGIC %md
# MAGIC The remainder of this notebook demonstrates using the functions declared in the referenced `my_funcs` Python file.

# COMMAND ----------

import numpy as np
import pandas as pd

Fdf = pd.DataFrame(np.random.normal(55, 25, 10000), columns=["temp"])
Fdf["unit"] = "F"

Cdf = pd.DataFrame(np.random.normal(10, 10, 10000), columns=["temp"])
Cdf["unit"] = "C"

df = spark.createDataFrame(pd.concat([Fdf, Cdf]))

display(df)

# COMMAND ----------

display(df.select(roundedTemp("unit", "temp")))

# COMMAND ----------

display(df.select(convertFtoC("unit", "temp")))

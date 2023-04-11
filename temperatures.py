# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction to Databricks Repos

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC This repo is created to demonstrate Git integration with Databricks Repos. Databricks Repos is a visual Git client in Databricks. **It supports common Git operations such a cloning a repository, committing and pushing, pulling, branch management, and visual comparison of diffs when committing.**
# MAGIC 
# MAGIC Within Repos you can develop code in notebooks or other files and follow data science and engineering code development best practices using Git for version control, collaboration, and CI/CD.
# MAGIC 
# MAGIC *In Databricks Repos*, you can use Git functionality to:
# MAGIC - Clone, push to, and pull from a remote Git repository.
# MAGIC - Create and manage branches for development work.
# MAGIC - Create notebooks, and edit notebooks and other files.
# MAGIC - Visually compare differences upon commit.
# MAGIC 
# MAGIC For following tasks, *work in your Git provider*:
# MAGIC - Create a pull request.
# MAGIC - Resolve merge conflicts.
# MAGIC - Merge or delete branches.
# MAGIC - Rebase a branch.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Importing Notebooks/Modules
# MAGIC 
# MAGIC You can import Databricks notebooks as well as python or R modules.
# MAGIC 
# MAGIC ### Import Python Modules  
# MAGIC 
# MAGIC The current working directory of your repo and notebook are automatically added to the Python path. When you work in the repo root, you can import modules from the root directory and all subdirectories.

# COMMAND ----------

from my_lib.my_funcs import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import Databricks Notebooks
# MAGIC 
# MAGIC Using `%run` allows you to execute a Databricks notebook in the current SparkSession, bringing any imported modules, declared variables, or defined functions into the current scope.
# MAGIC 
# MAGIC Note that Databricks Python notebooks are stored as normal Python files with the first line
# MAGIC 
# MAGIC ```
# MAGIC # Databricks notebook source
# MAGIC ```
# MAGIC 
# MAGIC When you import the notebook, Databricks recognizes it and imports it as a notebook, not as a Python module.
# MAGIC 
# MAGIC If you want to import the notebook as a Python module, you must edit the notebook in a code editor and remove the line `# Databricks Notebook source`. Removing that line converts the notebook to a regular Python file.
# MAGIC 
# MAGIC In the following cell, we are importing an Databricks notebook and run it.

# COMMAND ----------

# MAGIC %run ./my_lib/db-notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ## Using Imported Modules
# MAGIC 
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

# COMMAND ----------

# MAGIC %md ###adding this cell simply to follow instructions and commit a change to the code

# COMMAND ----------

df.count()

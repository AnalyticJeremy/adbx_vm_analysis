# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 2: Analyze Data
# MAGIC
# MAGIC Now that we have data on Databricks VM usage, we can analyze that data to understand how many VM's are being used and when they are being used.
# MAGIC With this data, we can see how we might optimize our compute configuration. We can also estimate how big our VM instance pools should be to
# MAGIC statisfy the compute requirements of our jobs.

# COMMAND ----------

import pyspark.sql.functions as F

delta_location = "dbfs:/vm-observation"

observations = spark.read.format("delta").load(delta_location + "/gold/observations")

# COMMAND ----------

# DBTITLE 1,Workspaces With Most Usage
display(observations.groupBy("subscriptionId", "workspaceName").agg(F.sum("count").alias("total_vm_minutes")).orderBy(F.col("total_vm_minutes").desc()))

# COMMAND ----------

# DBTITLE 1,Databricks Jobs With Most Usage
display(observations.groupBy("subscriptionId", "workspaceName", "JobName").agg(F.sum("count").alias("total_vm_minutes")).orderBy(F.col("total_vm_minutes").desc()))

# COMMAND ----------

# DBTITLE 1,VM Usage by VM SKU
display(observations.groupBy("subscriptionId", "workspaceName", "vmSize").agg(F.sum("count").alias("total_vm_minutes")).orderBy(F.col("total_vm_minutes").desc()))

# COMMAND ----------

# DBTITLE 1,Usage Summary Report
summary = observations \
            .groupBy("subscriptionId", "workspaceName", "vmSize", "JobName") \
            .agg(F.sum("count").alias("count"), F.min("date").alias("start"), F.max("date").alias("end"), F.max("count").alias("count_max"), F.min("count").alias("count_min"), F.countDistinct("ClusterName").alias("cluster_count")) \
            .withColumn("minutes", F.expr("((unix_timestamp(end) - unix_timestamp(start)) / 60) + 1")) \
            .withColumn("vms_per_min", F.expr("count / minutes")) \
            .orderBy("subscriptionId", "workspaceName", "vmSize", F.col("count").desc())

data = summary.collect()

subscriptionId = ""
workspaceName = ""
vmSize = ""

htmlCode = """
<!DOCTYPE html>
<meta charset="utf-8">
<style>
body {font-family: -apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Helvetica Neue,Arial,Noto Sans,sans-serif,Apple Color Emoji,Segoe UI Emoji,Segoe UI Symbol,Noto Color Emoji,FontAwesome;}
table {border-collapse: collapse;}
th, td {font-size: 9pt; min-width: 18pt; padding: 2pt;}
td:nth-child(5), td:nth-child(6), td:nth-child(7), td:nth-child(8), td:nth-child(9), td:nth-child(10) {text-align: right; min-width: 54pt}
tr.s td {font-size: 12pt; font-weight: bold; background-color: #cccccc;}
tr.w td {font-size: 11pt; font-weight: bold; background-color: #dddddd;}
tr.v td {font-size: 10pt; background-color: #eeeeee;}
</style>
<body><div class="markdown"><table>
<thead><tr><th></th><th></th><th></th><th style=\"text-align: left\">Job Name</th><th>Total VM<br />Minutes</th><th>Concurrent<br/>VMs - Max</th><th>Concurrent<br/>VMs - Min</th><th>Total<br />Minutes</th><th>VMs per<br />Minute</th><th>Number<br />of Runs</th></tr></thead>
<tbody>
"""

for row in data:
    if row[0] != subscriptionId:
        subscriptionId = row[0]
        htmlCode += f"<tr class=\"s\"><td colspan=\"10\">Subscription: {subscriptionId}</td></tr>"

    if row[1] != workspaceName:
        workspaceName = row[1]
        htmlCode += f"<tr class=\"w\"><td></td><td colspan=\"9\">Workspace: {workspaceName}</td></tr>"

    if row[2] != vmSize:
        vmSize = row[2]
        htmlCode += f"<tr class=\"v\"><td></td><td></td><td colspan=\"8\">VM SKU: {vmSize}</td></tr>"

    jobName = row[3] if row[3] else "[non-job cluster]"
    run_count = format(row[9], ",") if row[3] else "-"
    htmlCode += f"<tr><td></td><td></td><td></td><td>{jobName}</td><td>{row[4]:,}</td><td>{row[7]:,}</td><td>{row[8]:,}</td><td>{int(row[10]):,}</td><td>{row[11]:.2f}</td><td>{run_count}</td></tr>"

htmlCode += "</tbody></table></div></body></html>"

displayHTML(htmlCode)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analyzing Usage for Frequently Occuring Jobs
# MAGIC Analysis above looks at all VM usage in Databricks, including VM's that aren't part of on-demand job clusters.  This is useful data
# MAGIC to understand Databricks compute consumption across an entire enterprise.  However, to make progress, we need to re-focus on our
# MAGIC core purpose here:  We want to increase the resiliency of our regularly occuring Databricks jobs.  We will do this by identifying
# MAGIC jobs that are currently powered by on-demand job clusters and move those to Instance Pools.  Therefore, we do not need to be concerned
# MAGIC about interactive clusters or jobs that are already running with Instance Pools.  We also don't need to worry about jobs that run
# MAGIC sporadically.
# MAGIC
# MAGIC To that end, the rest of the analysis in this notebook will focus only on "frequent jobs".  These are jobs that appear often in the
# MAGIC logs during the period of time for which we gathered usage data.

# COMMAND ----------

dates = observations.agg(F.min("date"), F.max("date")).collect()[0]
range_start = dates[0]
range_end   = dates[1]
range_minutes = (range_end - range_start).total_seconds() / 60

frequent_jobs = observations \
                    .groupBy("subscriptionId", "location", "workspaceName", "vmSize", "JobName") \
                    .agg(F.min("date").alias("start"), F.max("date").alias("end")) \
                    .withColumn("minutes", F.expr("((unix_timestamp(end) - unix_timestamp(start)) / 60) + 1")) \
                    .withColumn("percentage", F.col("minutes") / F.lit(range_minutes)) \
                    .filter("JobName IS NOT NULL AND vmSize IS NOT NULL AND percentage > 0.75")

# COMMAND ----------

# MAGIC %md
# MAGIC ### VM SKU Variety
# MAGIC Each Instance Pool in Databricks can only contain one type of VM.  If we are using many different types of VM's, then we will have to
# MAGIC create many instance pools.  That can create extra management overhead.  It can also reduce the efficiency of our pools because spare
# MAGIC VM's from one instance pool cannot be used to fill demand in another pool.
# MAGIC
# MAGIC Therefore, to make our instance pools more efficient, we should consider reducing the number of SKU's that we use as much as possible.
# MAGIC In some cases, you may have done very careful testing to select the exact VM SKU that optimizes your workload.  But in other cases
# MAGIC (and it seems to be the most common case), a data engineer may have just blindly selected a VM type without putting any thought into it.
# MAGIC For these jobs, we might consider changing the VM type and consolidating many jobs into just a few VM types.
# MAGIC
# MAGIC In the report, a red exclamation mark (❗) is used to indicate an older version of a VM type if you are also using a newer version of
# MAGIC that same type.  The report also highlights usage of the "Standard_DS3_v2" VM type.  There's nothing wrong with this VM type (it's great, actually!),
# MAGIC but it's the default option when creating a compute cluster in Azure Databricks.  It is highlighted simply to illustrate how often your data
# MAGIC engineers simply accept the default instead of carefully selecting a VM type.

# COMMAND ----------

# DBTITLE 1,VM SKU Variety
from pyspark.sql.window import Window

vm_skus = spark.read.format("delta").load(delta_location + "/silver/vm_skus")

sku_usage = observations \
                .join(frequent_jobs, on=["subscriptionId", "location", "workspaceName", "vmSize", "JobName"], how="inner") \
                .groupBy("subscriptionId", "location", "workspaceName", "vmSize") \
                .agg(F.countDistinct("JobName").alias("job_count"), F.sum("count").alias("count"))

order_by_cols = ["subscriptionId", "workspaceName", "series", "family", "number_of_cores", "vmSize"]
window_spec = Window.partitionBy("subscriptionId", "workspaceName", "series").orderBy(order_by_cols)

sku_usage = sku_usage \
                .join(vm_skus, (sku_usage.subscriptionId == vm_skus.subscription_id) & (sku_usage.location == vm_skus.location) & (sku_usage.vmSize == vm_skus.name), how="left_outer") \
                .withColumn("next_name_without_version", F.lead("name_without_version").over(window_spec)) \
                .orderBy(order_by_cols) \
                .selectExpr("subscriptionId", "workspaceName", "COALESCE(series, 'Unknown') AS series", "family", "vmSize", "next_name_without_version", "name_without_version", "description", "count", "job_count", "number_of_cores", "memory_in_gb", "has_premium_storage", "is_amd", "has_local_temp_disk")

data = sku_usage.collect()

subscriptionId = ""
workspaceName = ""
series = ""

htmlCode = """
<!DOCTYPE html>
<meta charset="utf-8">
<style>
body {font-family: -apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Helvetica Neue,Arial,Noto Sans,sans-serif,Apple Color Emoji,Segoe UI Emoji,Segoe UI Symbol,Noto Color Emoji,FontAwesome;}
table {border-collapse: collapse;}
th, td {font-size: 9pt; min-width: 18pt; padding: 2pt;}
td:nth-child(5), td:nth-child(6), td:nth-child(7), td:nth-child(8) {text-align: right; min-width: 54pt}
td:nth-child(9), td:nth-child(10), td:nth-child(11) {text-align: center; min-width: 54pt}
tr.s td {font-size: 12pt; font-weight: bold; background-color: #cccccc;}
tr.w td {font-size: 11pt; font-weight: bold; background-color: #dddddd;}
tr.v td {font-size: 10pt; background-color: #eeeeee;}
tr.highlight td {background-color: orange; color: white}
</style>
<body><div class="markdown"><table>
<thead><tr><th></th><th></th><th></th><th style=\"text-align: left\">VM SKU</th><th>Total VM<br />Minutes</th><th>Job<br/>Count</th><th>vCPUs</th><th>Memory</th><th>Premium<br/>Storage</th><th>AMD<br />processor</th><th>Local<br />Temp Disk</th></tr></thead>
<tbody>
"""

for row in data:
    row_dict = row.asDict()
    if row[0] != subscriptionId:
        subscriptionId = row[0]
        htmlCode += f"<tr class=\"s\"><td colspan=\"11\">Subscription: {subscriptionId}</td></tr>"

    if row[1] != workspaceName:
        workspaceName = row[1]
        htmlCode += f"<tr class=\"w\"><td></td><td colspan=\"10\">Workspace: {workspaceName}</td></tr>"

    if row[2] != series:
        series = row[2]
        htmlCode += f"<tr class=\"v\"><td></td><td></td><td colspan=\"9\">"
        htmlCode += f"<b>{series} series</b><br />"
        htmlCode += f"<span style=\"font-size: 8pt; font-weight: normal\">{row_dict['description']}</span></td></tr>"

    htmlCode += f"<tr{' class=''highlight''' if row_dict['vmSize'] == 'Standard_DS3_v2' else ''}><td></td><td></td><td></td>"
    htmlCode += f"<td>{row_dict['vmSize']}{' &#10071;' if row_dict['name_without_version'] == row_dict['next_name_without_version'] else ''}</td>"
    htmlCode += f"<td>{row_dict['count']:,}</td>"
    htmlCode += f"<td>{row_dict['job_count']:,}</td>"
    htmlCode += f"<td>{row_dict['number_of_cores']:,}</td>"
    htmlCode += f"<td>{int(row_dict['memory_in_gb'])}</td>"
    htmlCode += f"<td>{'&#9989;' if row_dict['has_premium_storage'] else ''}</td>"
    htmlCode += f"<td>{'&#9989;' if row_dict['is_amd'] else ''}</td>"
    htmlCode += f"<td>{'&#9989;' if row_dict['has_local_temp_disk'] else ''}</td>"
    htmlCode += f"</tr>"

htmlCode += "</tbody></table></div></body></html>"

displayHTML(htmlCode)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

from pyspark.sql.window import Window

order_by_cols = ["subscriptionId", "location", "workspaceName", "vmSize", "JobName", "start_date"]
window_spec = Window.partitionBy("subscriptionId", "location", "workspaceName", "vmSize", "JobName").orderBy(order_by_cols)

job_runs = observations \
            .filter("JobName IS NOT NULL AND vmSize IS NOT NULL") \
            .groupBy("subscriptionId", "location", "workspaceName", "vmSize", "JobName", "JobId", "ClusterName", "RunName") \
            .agg(F.sum("count").alias("total_count"), F.avg("count").alias("vms_per_minute"), F.min("date").alias("start_date"), F.max("date").alias("end_date")) \
            .withColumn("run_minutes", F.expr("(UNIX_TIMESTAMP(end_date) - UNIX_TIMESTAMP(start_date)) / 60")) \
            .withColumn("previous_start_date", F.lag("start_date").over(window_spec)) \
            .withColumn("hours_between_starts", F.expr("(UNIX_TIMESTAMP(start_date) - UNIX_TIMESTAMP(previous_start_date)) / 3600.0")) \
            .orderBy(order_by_cols)

display(job_runs)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

vm_counts = observations \
                .filter("JobName IS NOT NULL") \
                .groupBy("subscriptionId", "workspaceName", "vmSize", "JobName", "date") \
                .agg(F.sum("count").alias("count"))

all_dates = spark.range(0, range_minutes).withColumn("date", F.lit(range_start) + F.expr("MAKE_INTERVAL(0, 0, 0, 0, 0, id)")).drop("id").orderBy("date")

df = frequent_jobs.select("subscriptionId", "workspaceName", "vmSize", "JobName") \
        .crossJoin(all_dates)

df = df \
        .join(vm_counts.alias("v"), ["subscriptionId", "workspaceName", "vmSize", "JobName", "date"], "leftouter") \
        .withColumn("count", F.coalesce(F.col("count"), F.lit(0))) \
        .groupBy("subscriptionId", "workspaceName", "vmSize", "jobName", "date") \
        .agg(F.sum("count").alias("count")) \
        .withColumn("job_active", F.expr("CASE WHEN count > 0 THEN jobName ELSE NULL END")) \
        .cache()

display( df.groupBy("subscriptionId", "workspaceName", "vmSize").agg(F.min("count"), F.max("count").alias("max_count")) )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspection by VM SKU

# COMMAND ----------

subscription_id = "ee691273-18af-4600-bc24-eb6768bf9cfa"
workspace_name = "dde-prod-dbks-w1"
vm_size = "Standard_DS4_v2"

# COMMAND ----------

# DBTITLE 1,VM Usage Aggregated by Minute
subset = df.filter(f"subscriptionId == '{subscription_id}' AND workspaceName == '{workspace_name}' AND vmSize == '{vm_size}'")
subset = subset.withColumn("minute", F.minute("date"))
subset = subset.withColumn("job_active", F.expr("CASE WHEN count > 0 THEN jobName ELSE NULL END"))
subset = subset.groupBy("subscriptionId", "workspaceName", "vmSize", "minute").agg(F.sum("count").alias("count"), F.countDistinct("job_active").alias("job_count"))
display(subset)

# COMMAND ----------

# DBTITLE 1,Daily Usage Visualization
from datetime import timedelta
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

date_range = observations.agg(F.min("date"), F.max("date")).collect()[0]
first_full_day = date_range[0].date() + timedelta(days = 1)
last_full_day  = date_range[1].date() - timedelta(days = 1)
days_count = (last_full_day - first_full_day).days + 1
days = [first_full_day + timedelta(days=i) for i in range(0, days_count)]

daily = df \
            .withColumn("day", F.to_date("date")) \
            .withColumn("time", F.expr("date - MAKE_INTERVAL(0, 0, 0, DATEDIFF(date, '2000-01-01'))")) \
            .filter(f"subscriptionId == '{subscription_id}' AND workspaceName == '{workspace_name}' AND vmSize == '{vm_size}'") \
            .filter(f"day BETWEEN '{first_full_day}' AND '{last_full_day}'") \
            .groupBy("subscriptionId", "workspaceName", "vmSize", "date", "day", "time") \
            .agg(F.sum("count").alias("count")) \
            .cache()

max_count = daily.agg(F.max("count")).collect()[0][0]
fig, ax = plt.subplots(days_count, 1, figsize=(24, (4 * days_count) + 2))
hour_locator = mdates.HourLocator(interval=1)
hour_formatter = mdates.DateFormatter("%H")

for i, day in enumerate(days):
    data = daily.filter(f"day = '{day}'").orderBy("date").toPandas()
    X = data["time"]
    Y = data["count"]

    ax[i].plot(X, Y, color='black')
    ax[i].fill_between(X, Y, 0, color='tomato')

    ax[i].xaxis.set_major_locator(hour_locator)
    ax[i].xaxis.set_major_formatter(hour_formatter)
    ax[i].set_ylim([0, max_count + 1])
    ax[i].set_xlim(pd.Timestamp('2000-01-01'), pd.Timestamp('2000-01-01 23:59:59'))
    ax[i].set_ylabel(day, fontsize=20)

fig.tight_layout()
fig.suptitle(f"Daily VM Usage     Workspace: {workspace_name}     VM SKU: {vm_size}", fontsize=30)
fig.subplots_adjust(top=0.97)

# COMMAND ----------



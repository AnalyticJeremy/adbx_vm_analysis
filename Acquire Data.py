# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 1: Acquire Data
# MAGIC
# MAGIC This notebook will get data about your Azure Databricks VM usage.  It looks at the last 12 days of the
# MAGIC [Azure Activity Logs](https://learn.microsoft.com/en-us/azure/azure-monitor/essentials/activity-log) for the
# MAGIC Databricks managed resource groups to determine what VMs are created and how long they are used.
# MAGIC
# MAGIC Through this process, we will build a tiny little lakehouse to store the Databricks VM usage data in its
# MAGIC various stages of processing.  The Azure Activity Log data is saved in its raw form as JSON data.
# MAGIC This notebook then uses Spark to transform that raw data into Delta files in a Bronze, Silver, and Gold
# MAGIC layer.  Each of these steps further transforms the data and prepares it for analysis.

# COMMAND ----------

# MAGIC %md
# MAGIC The Azure Activity Log can be accessed through the Azure Management API.  Rather than making raw REST calls, we'll use the 
# MAGIC [Azure Libraries for Python](https://learn.microsoft.com/en-us/azure/developer/python/sdk/azure-sdk-overview).  We can use the `%pip`
# MAGIC magic command to install them in our current Python session.

# COMMAND ----------

# MAGIC %pip install azure-identity azure-mgmt-monitor azure-mgmt-resourcegraph azure-mgmt-resource azure-mgmt-compute

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC These are the configuration settings you will need to adjust to specify how the data acquisition process should operate.
# MAGIC
# MAGIC **`service_principal`** contains the information about the service principal that you will use to connect to the Azure Management API.
# MAGIC If you used the `az ad sp create-for-rbac` CLI command to create the service principal, then you can just straight copy and paste the
# MAGIC output of that command here.
# MAGIC
# MAGIC **`subscription_ids`** is a Python list of the subscription ID's for which you want to gather Databricks VM usage data.  If you leave
# MAGIC the list empty, then this process will pull Databricks VM usage data for all subscriptions to which the service principal has access.
# MAGIC
# MAGIC **`delta_location`** is the path where the Databricks VM usage data will be saved.  It can be a location on the DBFS or it can use
# MAGIC any standard HDFS path (*e.g.* an `abfss://` path)
# MAGIC
# MAGIC **`days_to_pull`** specifies how many days into the past the Azure Activity Log should be queried.  By default, the Azure Activity Log
# MAGIC retains 90 days of data.  Specifying a lower number will speed up the process because there will be fewer queries made.  However,
# MAGIC you will want to pull several days of data to get a clearer picture of the usage patterns.
# MAGIC
# MAGIC **`query_window_hours`** controls how many hours will be covered by each query to the Azure Activity Log.  Instead of issuing one, big
# MAGIC query to the Activity Log to pull all of the history at once, this notebook will break it up into multiple queries.  If you don't have
# MAGIC a lot of Databricks VM activity, you can set this number higher.  For example, setting it to "24" will issue one query per day.  If
# MAGIC you have a high volume of Databricks VM activity, set this number lower.  This will break the querying process into smaller, more
# MAGIC reliable chunks.  If you're not sure, just leave it at 4.

# COMMAND ----------

service_principal = {
  "appId": "<your App ID / Client ID (should be a guid)>",
  "displayName": "<display name for your SP (optional)>",
  "password": "<Password / Secret (probably a 34-byte string)>",
  "tenant": "<your Tenant ID (should be a guid)>"
}

subscription_ids = []

delta_location = "dbfs:/vm-observation"

days_to_pull = 12
query_window_hours = 4

# COMMAND ----------

# MAGIC %md
# MAGIC ## Raw Data
# MAGIC
# MAGIC Read the Azure Activity Logs and save the results as JSON files.

# COMMAND ----------

import azure.mgmt.resourcegraph as arg
from datetime import datetime, timedelta
from azure.identity import ClientSecretCredential, AzureCliCredential
from azure.mgmt.resource import SubscriptionClient
from azure.mgmt.monitor import MonitorManagementClient


credential = ClientSecretCredential(service_principal["tenant"], service_principal["appId"], service_principal["password"])
#credential = AzureCliCredential() # (Optional... if you want to use Azure CLI to login instead of using a service principal)

while delta_location.endswith("/"):
    delta_location = delta_location[:-1]

# If we were not given a list of subscriptions, just get all of the ones the SP has access to
if not subscription_ids:
    subscriptions_client = SubscriptionClient(credential)
    subscription_ids = [s.as_dict()['subscription_id'] for s in subscriptions_client.subscriptions.list()]


def get_adbx_workspaces():
    arg_client = arg.ResourceGraphClient(credential)
    query_options = arg.models.QueryRequestOptions(result_format = "objectArray")

    query_string = 'resources | where type == "microsoft.databricks/workspaces" | project workspaceId = id, name, subscriptionId, sku = sku.name, managedResourceGroupId = properties.managedResourceGroupId, location'
    query_request = arg.models.QueryRequest(subscriptions=subscription_ids, query=query_string, options=query_options)
    results = arg_client.resources(query_request)

    output = results.as_dict()["data"]
    for o in output:
        o["managedResourceGroupName"] = o["managedResourceGroupId"].split('/')[-1]
        
        monitor_client = MonitorManagementClient(credential, o["subscriptionId"])
        diagnostic_settings = monitor_client.diagnostic_settings.list(o["workspaceId"])
        o["diagnostic_settings"] = [s.as_dict() for s in diagnostic_settings]
    
    return output


def get_activity_log_for_workspace(workspace, date):
    print(f"{datetime.now()}\n - Getting activity log for workspace \"{workspace['name']}\"    {date.date()}  {date.hour}:00")
    monitor_client = MonitorManagementClient(credential, workspace["subscriptionId"])
    
    start_time = date - timedelta(hours=query_window_hours) + timedelta(microseconds=1)
    end_time = date
    filter = f"eventTimestamp ge '{start_time.isoformat()}' and eventTimestamp le '{end_time.isoformat()}' and resourceGroupName eq '{workspace['managedResourceGroupName']}' and resourceType eq 'Microsoft.Compute/virtualMachines'"
    
    log_entries = monitor_client.activity_logs.list(filter)
    return [i.as_dict() for i in log_entries]

# COMMAND ----------

import json

workspaces = get_adbx_workspaces()
dbutils.fs.put(f"{delta_location}/raw/workspaces/workspaces.json", json.dumps({"workspaces": workspaces, "capture_time": datetime.now().isoformat() + "Z"}))

ws_dict = {ws['managedResourceGroupName']: ws for ws in workspaces}
timestamp = datetime.now()

i = 0
for rg_name in ws_dict:
    ws = ws_dict[rg_name]
    i = i + 1

    print(f"\n*************************\nProcessing workspace {ws['name']}   ({i} of {len(ws_dict)})")
    json_path = f"{delta_location}/raw/activity_logs/subscription={ws['subscriptionId']}/rg={rg_name}"

    for h in range(0, 24 * days_to_pull, query_window_hours):
        date = timestamp - timedelta(hours=h)
        logs = get_activity_log_for_workspace(ws, date)

        print(f" - Parsing response body JSON")
        for log in logs:
            if "properties" in log:
                if "responseBody" in log["properties"]:
                    response_body = json.loads(log["properties"]["responseBody"])
                    log["properties"]["responseBody"] = response_body

        json_filename = f"{json_path}/{date.date()}-{date.hour}.json"
        print(f" - Saving log to file: {json_filename}")
        dbutils.fs.put(json_filename, json.dumps({"workspace": ws, "logs": logs, "capture_time": datetime.now().isoformat() + "Z"}))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get VM SKU Information
# MAGIC In addition to the VM usage logs, we also need to get some information about the VM SKU's.  We will pull it from the Azure API just to be sure
# MAGIC we always have the latest data.  The available VM types can vary by subscription and by region so we will have to query with multiple sets of
# MAGIC parameters.

# COMMAND ----------

from azure.mgmt.compute import ComputeManagementClient

sub_locs = [(ws['subscriptionId'], ws['location']) for ws in workspaces]
sub_locs = list(set(sub_locs))

for sub_loc in sub_locs:
    print(f"Getting VM SKUs for subscription {sub_loc[0]} in {sub_loc[1]} region")
    compute_client = ComputeManagementClient(credential, sub_loc[0])
    results = compute_client.virtual_machine_sizes.list(sub_loc[1])
    results = [i.as_dict() for i in results]

    for r in results:
        r["subscription_id"] = sub_loc[0]
        r["location"] = sub_loc[1]

    json_path = f"{delta_location}/raw/vm_skus/subscription={sub_loc[0]}"
    json_filename = f"{json_path}/{sub_loc[1]}.json"
    dbutils.fs.put(json_filename, json.dumps(results))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer
# MAGIC
# MAGIC This is the first step in transforming our raw data.  We will simply read the JSON files and apply only the bare minimum of transformations
# MAGIC before converting it to Delta files.

# COMMAND ----------

import pyspark.sql.functions as F

spark.conf.set("spark.sql.caseSensitive", "true")

df = spark.read \
        .format("json") \
        .load(delta_location + "/raw/activity_logs") \
        .selectExpr("workspace.workspaceId", "workspace.name AS workspaceName", "workspace.sku AS workspaceSku", "workspace.subscriptionId",
                    "workspace.managedResourceGroupId", "CAST(capture_time AS TIMESTAMP) AS capture_time", "explode(logs) AS log_entry") \
        .selectExpr("CAST(log_entry.event_timestamp AS TIMESTAMP) AS event_timestamp", "lower(split(log_entry.resource_id, '/')[8]) as vm_id", "*") \
        .cache()

# COMMAND ----------

# MAGIC %md
# MAGIC There are some columns that we definitely won't need for analysis and they have some challenging column names.  So we will make life
# MAGIC easier for us and just drop those columns.

# COMMAND ----------

claim_cols = df.select("log_entry.claims.*").columns

for col in claim_cols:
    if col.startswith("http") and ":/" in col:
        df = df.withColumn("log_entry", F.col("log_entry").dropFields("claims.`" + col + "`"))

df = df.withColumn("log_entry", F.col("log_entry").dropFields("properties.responseBody.identity"))

# COMMAND ----------

# MAGIC %md
# MAGIC Each of the Azure tags applied to a Databricks VM is represented as its own field in a nested column.  However, Delta does not allow the names of two
# MAGIC columns to vary only by case.  For example, Delta won't let you have a column called `project_name` and `Project_Name` in the same table.  Across a large
# MAGIC organization, it's easy for Azure tags to have inconsistent casing in their names.  Therefore, as we convert the raw data to Delta, we have to find
# MAGIC Azure tag columns that have the same name but different letter casing and combine those into one column.

# COMMAND ----------

tagnames = df.select("log_entry.properties.responseBody.tags.*").columns

instance_pool_tagname = "DatabricksInstancePoolId"
if instance_pool_tagname not in tagnames:
    print("Instance Pool ID column not in JSON schema.  Adding it to Delta file.")
    df = df.withColumn("log_entry", F.col("log_entry").withField("properties.responseBody.tags." + instance_pool_tagname, F.lit(None).astype("string"))) 


tagname_groups = {}
for tagname in tagnames:
    tagname_groups.setdefault(tagname.lower(), []).append(tagname)

for key in tagname_groups:
    values = tagname_groups[key]
    if len(values) > 1:
        keep = values[0]
        for i in range(1, len(values)):
            print(f"combine tag \"{values[i]}\" into tag \"{keep}\"")
            df = df.withColumn("log_entry",
                F.col("log_entry").withField("properties.responseBody.tags." + keep,
                                                F.coalesce(
                                                    F.col("log_entry.properties.responseBody.tags." + keep),
                                                    F.col("log_entry.properties.responseBody.tags." + values[i])
                                                )
                                            )
            ) 
            df = df.withColumn("log_entry", F.col("log_entry").dropFields("properties.responseBody.tags." + values[i]))

# COMMAND ----------

# MAGIC %md
# MAGIC Delta has some restrictions on what characters can be used in column names.  Most notabaly, this includes spaces.  We will look through
# MAGIC all of the Azure tag columns and change any invalid characters to underscores.

# COMMAND ----------

tagnames = df.select("log_entry.properties.responseBody.tags.*").columns
bad_chars = " ,;{}()\n\t="

for tagname in tagnames:
    for c in bad_chars:
        if c in tagname:
            new_tagname = tagname.replace(c, "_")
            print(f"Change tag \"{tagname}\" to \"{new_tagname}")

            if new_tagname in tagnames:
                df = df.withColumn("log_entry",
                    F.col("log_entry").withField("properties.responseBody.tags." + new_tagname,
                                                    F.coalesce(
                                                        F.col("log_entry.properties.responseBody.tags." + new_tagname),
                                                        F.col("log_entry.properties.responseBody.tags." + tagname)
                                                    )
                                                )
                ) 
            else:
                df = df.withColumn("log_entry",
                    F.col("log_entry").withField(
                        "properties.responseBody.tags." + new_tagname,
                        F.col("log_entry.properties.responseBody.tags." + tagname)
                    )
                )

            df = df.withColumn("log_entry", F.col("log_entry").dropFields("properties.responseBody.tags.`" + tagname + "`"))

# COMMAND ----------

df.write.format("delta").mode("overwrite").save(delta_location + "/bronze/activity_logs")

# COMMAND ----------

# MAGIC %md
# MAGIC #### VM SKU Information
# MAGIC We also need to convert our VM SKU data from JSON to Delta!  This is much simpler, though, because the data is not complex.

# COMMAND ----------

df = spark.read \
        .format("json") \
        .load(delta_location + "/raw/vm_skus")

df.write.format("delta").mode("overwrite").save(delta_location + "/bronze/vm_skus")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer
# MAGIC
# MAGIC Now that we have our data in the easy-to-use Delta format, we can begin transforming it into a format that is more useful for analysis.
# MAGIC The raw data contains a log of all of the VM operations that Azure performed in the Databricks-managed resource groups.  However many
# MAGIC of those operations aren't useful for our analysis.  We only want to know when a new VM was created or an existing VM was deleted.
# MAGIC (Though we won't use it for this analysis, we'll also record when spot instances are evicted because that could be interesting to
# MAGIC study.)

# COMMAND ----------

import pyspark.sql.functions as F

df = spark.read.format("delta").load(delta_location + "/bronze/activity_logs")

# Get rows for the creation, eviction, or deletion of a VM
activities = df \
                .filter("""
                    (log_entry.properties.responseBody IS NOT NULL AND log_entry.resource_type.value != 'Microsoft.Compute/virtualMachines/extensions')
                    OR lower(log_entry.operation_name.value) LIKE '%spot%'
                    OR (log_entry.operation_name.value == 'Microsoft.Compute/virtualMachines/delete' AND log_entry.event_name.value == 'EndRequest')
                """) \
                .selectExpr("vm_id", "event_timestamp", "log_entry.properties.statusCode", "log_entry.operation_name.value AS operation", "subscriptionId",
                            "workspaceName", "log_entry.properties.responseBody.properties.hardwareProfile.vmSize", "log_entry.properties.responseBody.properties.priority",
                            "log_entry.properties.responseBody.location", "log_entry.properties.responseBody.tags") \
                .orderBy("vm_id", "event_timestamp")

# COMMAND ----------

# MAGIC %md
# MAGIC For our analysis, we need the Azure tags associated with the VM.  This tells us which job, cluster, or pool the VM was part of.  However, we only
# MAGIC get the Azure tag data when a VM is created. If a VM was started before the period of time we are studying, we won't have that tag data in our logs.
# MAGIC Therefore, we will just throw out any data for VM's that are missing this critical data.

# COMMAND ----------

# Remove all information about VMs if we don't have any tags for it
activities.createOrReplaceTempView("activities_table")

no_starts = spark.sql("SELECT vm_id, COUNT(*), MIN(event_timestamp), MAX(event_timestamp) FROM activities_table WHERE vm_id NOT IN (SELECT vm_id FROM activities_table WHERE tags IS NOT NULL) GROUP BY vm_id ORDER BY 3 DESC")
print(f"\nRemoving data for {no_starts.count():,} VMs because there is no creation data in the logs.\n")
no_starts.show(truncate=False)

activities = spark.sql("SELECT * FROM activities_table WHERE vm_id IN (SELECT vm_id FROM activities_table WHERE tags IS NOT NULL)")

# COMMAND ----------

# MAGIC %md
# MAGIC Since not all data points (like Azure tag data) is present in every log entry, we will order the logs by VM ID and timestamp.  We will carry forward
# MAGIC these data points to all future rows that are missing the data.
# MAGIC
# MAGIC That is to say, if we have sparse data like this...
# MAGIC
# MAGIC | VM ID | Timestamp | VM Size  |
# MAGIC |-------|-----------|----------|
# MAGIC | 1234  | 13:38:34  | E8ds_v4  |
# MAGIC | 1234  | 13:42:18  | *null*   |
# MAGIC | 1234  | 13:46:12  | *null*   |
# MAGIC | 1234  | 13:47:09  | *null*   |
# MAGIC | 1234  | 13:49:58  | E16ds_v4 |
# MAGIC | 1234  | 13:50:02  | *null*   |
# MAGIC | 5678  | 12:17:24  | DS3_v3   |
# MAGIC | 5678  | 12:22:32  | *null*   |
# MAGIC | 5678  | 12:34:02  | *null*   |
# MAGIC
# MAGIC ... we will carry forward the values in the sparse column so it looks like this:
# MAGIC
# MAGIC | VM ID | Timestamp | VM Size  |
# MAGIC |-------|-----------|----------|
# MAGIC | 1234  | 13:38:34  | E8ds_v4  |
# MAGIC | 1234  | 13:42:18  | E8ds_v4  |
# MAGIC | 1234  | 13:46:12  | E8ds_v4  |
# MAGIC | 1234  | 13:47:09  | E8ds_v4  |
# MAGIC | 1234  | 13:49:58  | E16ds_v4 |
# MAGIC | 1234  | 13:50:02  | E16ds_v4 |
# MAGIC | 5678  | 12:17:24  | DS3_v3   |
# MAGIC | 5678  | 12:22:32  | DS3_v3   |
# MAGIC | 5678  | 12:34:02  | DS3_v3   |

# COMMAND ----------

from pyspark.sql.window import Window

window_spec = Window.partitionBy("vm_id").orderBy("event_timestamp")

activities = activities \
                .withColumn("activity_number", F.row_number().over(window_spec)) \
                .withColumn("event_end", F.lead("event_timestamp").over(window_spec) - F.expr('INTERVAL 1 MICROSECOND')) \
                .withColumn("vmSize", F.last("vmSize", True).over(window_spec)) \
                .withColumn("priority", F.last("priority", True).over(window_spec)) \
                .withColumn("location", F.last("location", True).over(window_spec)) \
                .withColumn("tags", F.last("tags", True).over(window_spec))

print(f"Rows: {activities.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC Silver processing complete!  Write out this version of the data to its own Delta table.

# COMMAND ----------

activities.write.format("delta").mode("overwrite").save(delta_location + "/silver/activities")

# COMMAND ----------

# MAGIC %md
# MAGIC #### VM SKU Information
# MAGIC This will be the final step in processing the VM SKU data.  We will interpret the SKU name to get additional information about the VM series.
# MAGIC
# MAGIC For information on how the VM SKU's are named, see this document: [Azure virtual machine sizes naming conventions](https://learn.microsoft.com/en-us/azure/virtual-machines/vm-naming-conventions)

# COMMAND ----------

df = spark.read.format("delta").load(delta_location + "/bronze/vm_skus")

df = df \
        .drop("subscription") \
        .withColumn("memory_in_gb", F.expr("memory_in_mb / 1024")) \
        .withColumn("name_without_version", F.expr("REGEXP_REPLACE(name, '\_v[0-9]+', '')")) \
        .withColumn("name_parts", F.split("name", "_")) \
        .withColumn("is_promo", F.expr("name LIKE '%_Promo'")) \
        .withColumn("family_group", F.expr("name_parts[0]")) \
        .withColumn("family", F.expr("REGEXP_EXTRACT(name_parts[1], '^[A-z]+', 0)")) \
        .withColumn("name_number", F.expr("REGEXP_EXTRACT(name_parts[1], '^([A-z]+)([0-9]+)', 2)")) \
        .withColumn("constrained_vcpu", F.expr("REGEXP_EXTRACT(name_parts[1], '^([A-z]+)([0-9]+)-([0-9]+)', 3)")) \
        .withColumn("additive_features", F.expr("REGEXP_EXTRACT(name_parts[1], '^([A-z]+)([0-9]+)((-[0-9]+)?)(.*)', 5)")) \
        .withColumn("accelerator_type", F.expr("CASE WHEN name_parts[2] != 'Promo' AND name_parts[2] NOT RLIKE 'v[0-9]+' THEN name_parts[2] ELSE NULL END")) \
        .withColumn("version_number", F.expr("CASE WHEN name_parts[2] RLIKE 'v[0-9]+' THEN name_parts[2] ELSE CASE WHEN name_parts[3] RLIKE 'v[0-9]+' THEN name_parts[3] ELSE 'v1' END END")) \
        .drop("name_parts")

df = df \
        .withColumn("is_amd", F.expr("additive_features RLIKE 'a'")) \
        .withColumn("is_block_storage_performance", F.expr("additive_features RLIKE 'b'")) \
        .withColumn("has_local_temp_disk", F.expr("additive_features RLIKE 'd'")) \
        .withColumn("is_isolated", F.expr("additive_features RLIKE 'i'")) \
        .withColumn("is_low_memory", F.expr("additive_features RLIKE 'l'")) \
        .withColumn("is_memory_intensive", F.expr("additive_features RLIKE 'm'")) \
        .withColumn("is_arm", F.expr("additive_features RLIKE 'p'")) \
        .withColumn("is_tiny_memory", F.expr("additive_features RLIKE 't'")) \
        .withColumn("has_premium_storage", F.expr("additive_features RLIKE 's'")) \
        .withColumn("is_confidential", F.expr("additive_features RLIKE 'C'")) \
        .withColumn("is_node_packing", F.expr("additive_features RLIKE 'NP'"))

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC The Azure API does not provide information about which series a VM type belongs to.  So we will copy some data from the docs and use that instead.
# MAGIC This may have to be manually updated from time to time.

# COMMAND ----------

# Copied from:  https://learn.microsoft.com/en-us/azure/virtual-machines/sizes

series_data = [
    ["General purpose",
     "B, Dsv3, Dv3, Dasv4, Dav4, DSv2, Dv2, Av2, DC, DCv2, Dpdsv5, Dpldsv5, Dpsv5, Dplsv5, Dv4, Dsv4, Ddv4, Ddsv4, Dv5, Dsv5, Ddv5, Ddsv5, Dasv5, Dadsv5",
     "Balanced CPU-to-memory ratio. Ideal for testing and development, small to medium databases, and low to medium traffic web servers."
    ],
    ["Compute optimized",
     "F, Fs, Fsv2, FX",
     "High CPU-to-memory ratio. Good for medium traffic web servers, network appliances, batch processes, and application servers."
    ],
    ["Memory optimized",
     "Esv3, Ev3, Easv4, Eav4, Epdsv5, Epsv5, Ev4, Esv4, Edv4, Edsv4, Ev5, Esv5, Edv5, Edsv5, Easv5, Eadsv5, Mv2, M, DSv2, Dv2",
      "High memory-to-CPU ratio. Great for relational database servers, medium to large caches, and in-memory analytics."
    ],
    ["Storage optimized",
     "Lsv2, Lsv3, Lasv3",
      "High disk throughput and IO ideal for Big Data, SQL, NoSQL databases, data warehousing and large transactional databases."
    ],
    ["GPU",
     "NC, NCv2, NCv3, NCasT4_v3, ND, NDv2, NV, NVv3, NVv4, NDasrA100_v4, NDm_A100_v4",
     "Specialized virtual machines targeted for heavy graphic rendering and video editing, as well as model training and inferencing (ND) with deep learning. Available with single or multiple GPUs."
    ],
    ["High performance compute",
     "HB, HBv2, HBv3, HBv4, HC, HX",
     "Our fastest and most powerful CPU virtual machines with optional high-throughput network interfaces (RDMA)."
    ]
]

series_data = [{'series': i[0], 'type': i[1].split(','), 'description': i[2]} for i in series_data]
series_data = spark.createDataFrame(series_data)

series_data = series_data \
                .withColumn("type", F.explode("type")) \
                .withColumn("type", F.expr("TRIM(type)")) \
                .withColumn("family", F.expr("REGEXP_EXTRACT(type, '^[A-Z]+', 0)")) \
                .withColumn("version_number", F.expr("REGEXP_EXTRACT(type, 'v[0-9]+$', 0)")) \
                .withColumn("version_number", F.expr("CASE WHEN version_number == '' THEN 'v1' ELSE version_number END"))

series_data = series_data \
                .groupBy("family", "version_number") \
                .agg(F.min("series").alias("series")) \
                .join(series_data.groupBy("series").agg(F.max("description").alias("description")), on="series", how="inner")

df = df.join(series_data, on=["family", "version_number"], how="left_outer")

df.write.format("delta").mode("overwrite").save(delta_location + "/silver/vm_skus")

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Layer
# MAGIC
# MAGIC For this last step in the data preparation, we want to explode the dataset rows so that there is an individual record for every minute that a VM was running.

# COMMAND ----------

activities = spark.read.format("delta").load(delta_location + "/silver/activities")

# COMMAND ----------

from datetime import datetime, timedelta
import pyspark.sql.functions as F

dates = activities \
            .agg(F.min("event_timestamp").alias("min"), F.max("event_timestamp").alias("max")) \
            .selectExpr("*", "(TO_UNIX_TIMESTAMP(max) - TO_UNIX_TIMESTAMP(min)) / 60 AS minutes") \
            .collect()

start_date = dates[0][0]
start_date = datetime(start_date.year, start_date.month, start_date.day, start_date.hour, start_date.minute)
minutes = dates[0][2]

observations = spark.range(minutes) \
                    .withColumn("date", F.lit(start_date) + F.expr("MAKE_INTERVAL(0, 0, 0, 0, 0, id)"))


observations.alias("o") \
    .join(activities.filter("operation NOT LIKE '%delete'").alias("a"),
          observations["date"].between(activities["event_timestamp"], activities["event_end"])) \
    .withColumn("JobName", F.expr("REGEXP_REPLACE(REGEXP_REPLACE(a.tags.RunName, '^ADF\_', ''), '\_[a-f0-9\-]{36}$', '')")) \
    .groupBy("o.date", "a.subscriptionId", "a.workspaceName", "a.vmSize", "a.priority", "a.location", "a.tags.ClusterName", "a.tags.JobId",
             "JobName", "a.tags.RunName", "a.tags.DatabricksInstancePoolId") \
    .count() \
    .orderBy("o.date") \
    .write \
    .format("delta") \
    .save(delta_location + "/gold/observations")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Checks
# MAGIC
# MAGIC Now that we've built our datasets, let's check them to make sure they conform to our expectations.

# COMMAND ----------

df = spark.read.format("delta").load(delta_location + "/bronze/activity_logs")
activities = spark.read.format("delta").load(delta_location + "/silver/activities")
observations = spark.read.format("delta").load(delta_location + "/gold/observations")

print(f"Bronze Rows: {df.count():,}")
print(f"Silver Rows: {activities.count():,}")
print(f"Gold Rows:   {observations.count():,}")

# COMMAND ----------

print(df.filter("event_timestamp IS NULL").count())
print(df.filter("vm_id IS NULL").count())
print(df.filter("lower(vm_id) NOT rlike '^[0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f]$'").count())

# COMMAND ----------

display(df.groupBy("log_entry.properties.statusCode").count())

# COMMAND ----------

display(df.groupBy("log_entry.operation_name.value", "log_entry.properties.statusCode").count().orderBy("value", "statusCode"))

# COMMAND ----------

# MAGIC %md
# MAGIC With Azure, there is some time between when a `DELETE` operation request is received and when it completes.  This will help us see the distribution
# MAGIC of minutes it takes to delete a VM.

# COMMAND ----------

import pyspark.sql.functions as F

display(df.where("log_entry.operation_name.value == 'Microsoft.Compute/virtualMachines/delete'").groupBy("vm_id", "log_entry.operation_name.value").count().orderBy(F.col("count").desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC This is a quick check to see if there are any Databricks Instance Pools in use.

# COMMAND ----------

display(
    df
        .filter("log_entry.properties.responseBody.tags.DatabricksInstancePoolId IS NOT NULL")
        .groupBy("log_entry.properties.responseBody.tags.DatabricksInstancePoolId")
        .agg(F.count("vm_id"), F.min("event_timestamp"), F.max("event_timestamp"))
    )

# COMMAND ----------

display(activities.groupBy("tags.ClusterName").count())

# COMMAND ----------

# MAGIC %md
# MAGIC Here we'll take a quick peek at the information we came here for... how many VM's of each type are in use at any given time.

# COMMAND ----------

display(observations.groupBy("vmSize", "date").agg(F.sum("count").alias("count")).orderBy("date"))

# COMMAND ----------

display(observations.groupBy("JobName").agg(F.sum("count").alias("vms")).orderBy(F.col("vms").desc()))

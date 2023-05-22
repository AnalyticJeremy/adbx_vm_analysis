# Azure Databricks VM Usage Analysis

Azure Databricks customers should be using [instance pools](https://learn.microsoft.com/en-us/azure/databricks/clusters/pool-best-practices)
for their production workloads.  These instance pools will help your jobs run faster (because you don't have to wait for
VM's to spin up) and will make your workload more resilient (because it reduces your exposure to "Cloud Provisioning" errors).

One common challenge to creating instance pools is knowing how large to make them.  Customers may have multiple production workspaces, each
with numerous jobs running at a variety of intervals.  Determining the right size for the pools can require complex analysis.

... and that's why I created this tool!  It's an accelerator that you can run in your environment to determine your VM usage patterns.
This will give you the insights you need to choose the size of each of your instance pools.

This tool has two phases:

1. **Data Acquisition** - use the Azure Activity Logs to gather information about VM creation and deletion over the past few days
1. **Data Analysis** - analyze the VM usage patterns to determine the most efficient size for your pools

## Setup
To run this solution, the code will need to connect to your Azure subscription.  The best way to do this is to create a new
service principal that has read access to the subscriptions you would like to analyze.  The easiest way to create this
service principal is with a CLI command like this:
```
$subscriptionId = "<your subscription id>"
az ad sp create-for-rbac --name "Databricks VM Usage Analyzer" --role reader --scopes /subscriptions/$subscriptionId
```

This command will return the results in JSON format.  You can copy those results right into the first notebook, and you
should be ready to go!

## Phase 1: Data Acquisition
Run the "Acquire Data" notebook.  This will query the Azure Activity Log and get VM operation information from the
Databricks-managed resource groups.  The data will be saved as raw JSON files.  The notebook will then transform
this data in various phases and create a tiny little Delta lake with bronze, silver, and gold layers.

## Phase 2: Data Analysis
The second notebook, "Analyze Data," will read the data acquired in the previous phase.  It will summarize VM usage
by job and by VM SKU.  You can use this data to better understand your VM usage patterns and determine how to optimize
your Databricks Instance Pool size.

## Frequently Asked Questions (FAQ)

**Why does your analysis only suggest one size for an instance pool?  Why don't you suggest settings for minimum idle, maximum capacity,
and idle instance auto-timeout that would allow the pool size to vary?**

For this analysis, we are primarialy concerned with creating pools for resiliency purposes.  The idea is to create VM's and then hang
on to them as long as possible so that in the event of an incident with the Azure VM service, we will already have the VM's that we
need.  So the question we are trying to answer is:  How many VM's should I keep on-hand to be able to run my jobs in the event of
an Azure incident?
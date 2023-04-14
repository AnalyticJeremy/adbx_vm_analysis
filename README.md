# Azure Databricks VM Usage Analysis

Azure Databricks customers should be using [instance pools](https://learn.microsoft.com/en-us/azure/databricks/clusters/pool-best-practices)
for their production workloads.  These instance pools will help your jobs run faster (because you don't have to wait for
VM's to spin up) and will make your workload more resilient (because you won't get "Cloud Provisioning" errors).

One common challenge to creating instance pools is knowing how large to make them.  Customers may have multiple production workspaces, each
with numerous jobs running at a variety of intervals.  Determining the right size for the pools can require complex analysis.

... and that's why I created this tool!  It's an accelerator that you can run in your environment to determine your VM usage patterns.
This will give you the insights you need to choose the size of each of your instance pools.

This tool has two phases:

1. **Data Acquisition** - use the Azure Activity Logs to gather information about VM creation and deletion over the past few days
1. **Data Analysis** - analyze the VM usage patterns to determine the most efficient size for your pools

## Setup
TODO:  How to set up this accelerator

## Phase 1: Data Acquisition
TODO:  Running the first notebook

## Phase 2: Data Analysis
TODO:  How to run the second notebook

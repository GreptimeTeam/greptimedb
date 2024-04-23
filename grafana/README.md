Grafana dashboard for GreptimeDB
--------------------------------

GreptimeDB's official Grafana dashboard.

Status notify: we are still working on this config. It's expected to change frequently in the recent days. Please feel free to submit your feedback and/or contribution to this dashboard 🤗

# How to use

## `greptimedb.json`

Open Grafana Dashboard page, choose `New` -> `Import`. And upload `greptimedb.json` file.

## `greptimedb-cluster.json`

This cluster dashboard provides a comprehensive view of incoming requests, response statuses, and internal activities such as flush and compaction, with a layered structure from frontend to datanode. Designed with a focus on alert functionality, its primary aim is to highlight any anomalies in metrics, allowing users to quickly pinpoint the cause of errors.

We use Prometheus to scrape off metrics from nodes in GreptimeDB cluster, Grafana to visualize the diagram. Any compatible stack should work too.

__Note__: This dashboard is still in an early stage of development. Any issue or advice on improvement is welcomed.

### Configuration

Please ensure the following configuration before importing the dashboard into Grafana.

__1. Prometheus scrape config__

Assign `greptime_pod` label to each host target. We use this label to identify each node instance.

```yml
# example config
# only to indicate how to assign labels to each target
# modify yours accordingly
scrape_configs:
  - job_name: metasrv
    static_configs:
    - targets: ['<ip>:<port>']
      labels:
        greptime_pod: metasrv

  - job_name: datanode
    static_configs:
    - targets: ['<ip>:<port>']
      labels:
        greptime_pod: datanode1
    - targets: ['<ip>:<port>']
      labels:
        greptime_pod: datanode2
    - targets: ['<ip>:<port>']
      labels:
        greptime_pod: datanode3

  - job_name: frontend
    static_configs:
    - targets: ['<ip>:<port>']
      labels:
        greptime_pod: frontend
```

__2. Grafana config__

Create a Prometheus data source in Grafana before using this dashboard. We use `datasource` as a variable in Grafana dashboard so that multiple environments are supported.

### Usage

Use `datasource` or `greptime_pod` on the upper-left corner to filter data from certain node.

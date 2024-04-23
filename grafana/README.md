Grafana dashboard for GreptimeDB
--------------------------------

GreptimeDB's official Grafana dashboard.

Status notify: we are still working on this config. It's expected to change frequently in the recent days. Please feel free to submit your feedback and/or contribution to this dashboard 🤗

# How to use

## `greptimedb.json`

Open Grafana Dashboard page, choose `New` -> `Import`. And upload `greptimedb.json` file.

## `greptimedb-cluster.json`

This cluster dashboard provides a comprehensive view of incoming requests, response statuses, and internal activities such as flush and compaction, with a layered structure from frontend to datanode. Designed with a focus on alert functionality, its primary aim is to highlight any anomalies in metrics, allowing users to quickly pinpoint the cause of errors.

### Configuration

__Prometheus scrape config__

```yml
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
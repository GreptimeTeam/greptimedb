Grafana dashboard for GreptimeDB
--------------------------------

GreptimeDB's official Grafana dashboard.

Status notify: we are still working on this config. It's expected to change frequently in the recent days. Please feel free to submit your feedback and/or contribution to this dashboard 🤗

If you use Helm [chart](https://github.com/GreptimeTeam/helm-charts) to deploy GreptimeDB cluster, you can enable self-monitoring by setting the following values in your Helm chart:

- `monitoring.enabled=true`: Deploys a standalone GreptimeDB instance dedicated to monitoring the cluster;
- `grafana.enabled=true`: Deploys Grafana and automatically imports the monitoring dashboard;

The standalone GreptimeDB instance will collect metrics from your cluster and the dashboard will be available in the Grafana UI. For detailed deployment instructions, please refer to our [Kubernetes deployment guide](https://docs.greptime.com/nightly/user-guide/deployments/deploy-on-kubernetes/getting-started).

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

Configure Prometheus to scrape the cluster.

```yml
# example config
# only to indicate how to assign labels to each target
# modify yours accordingly
scrape_configs:
  - job_name: metasrv
    static_configs:
    - targets: ['<metasrv-ip>:<port>']

  - job_name: datanode
    static_configs:
    - targets: ['<datanode0-ip>:<port>', '<datanode1-ip>:<port>', '<datanode2-ip>:<port>']

  - job_name: frontend
    static_configs:
    - targets: ['<frontend-ip>:<port>']
```

__2. Grafana config__

Create a Prometheus data source in Grafana before using this dashboard. We use `datasource` as a variable in Grafana dashboard so that multiple environments are supported.

### Usage

Use `datasource` or `instance` on the upper-left corner to filter data from certain node.

# Grafana dashboards for GreptimeDB

## Overview

This repository maintains the Grafana dashboards for GreptimeDB. It has two types of dashboards:

- `cluster/`: The dashboard for the GreptimeDB cluster.
- `standalone/`: The dashboard for the standalone GreptimeDB instance.

Actually, the `standalone` dashboard is generated from the `cluster` dashboard by removing the `instance` labels by executing the `make gen-standalone-dashboard` command in the root directory.

As the rapid development of GreptimeDB, the metrics may be changed, please feel free to submit your feedback and/or contribution to this dashboard 🤗

To simplify operations, the dashboards are currently divided by component granularity:

- `overview.json`: This dashboard summarizes the basic metrics of the current GreptimeDB, serving as an excellent entry point for understanding the overall operational status of the GreptimeDB;
- `frontend.json`: This dashboard displays various metrics of the frontend component in the GreptimeDB, including request count, response time, error rate, etc.;
- `datanode.json`: This dashboard displays various metrics of the datanode component in the GreptimeDB, including metrics from Mito Engine and OpenDAL;
- `flownode.json`: This dashboard displays various metrics of the flownode component in the GreptimeDB;

## Datasources

There are two datasources for the dashboards to fetch the metrics:

- **Prometheus**: Expose the metrics of GreptimeDB.
- **Information Schema**: It is the MySQL port of the current monitored instance. The `overview` dashboard will use this datasource to show the information schema of the current instance.

## Instance Filters

In order to deploy the dashboards for multiple scenarios (K8s, bare metal, etc.), we prefer to use the `instance` label when filtering instances.

Additionally, we recommend including the `pod` label in the legend to make it easier to identify each instance, even though this field will be empty in bare metal scenarios.

For example, the following query is recommended:

```promql
sum(process_resident_memory_bytes{instance=~"$datanode"}) by (instance, pod)
```

And the legend will be like: `[{{instance}}]-[{{ pod }}]`.

## Deployment

### Helm

If you use Helm [chart](https://github.com/GreptimeTeam/helm-charts) to deploy GreptimeDB cluster, you can enable self-monitoring by setting the following values in your Helm chart:

- `monitoring.enabled=true`: Deploys a standalone GreptimeDB instance dedicated to monitoring the cluster;
- `grafana.enabled=true`: Deploys Grafana and automatically imports the monitoring dashboard;

The standalone GreptimeDB instance will collect metrics from your cluster and the dashboard will be available in the Grafana UI. For detailed deployment instructions, please refer to our [Kubernetes deployment guide](https://docs.greptime.com/nightly/user-guide/deployments/deploy-on-kubernetes/getting-started).

### Self-host Prometheus and import dashboards manually

1. **Configure Prometheus to scrape the cluster**

   The following is an example configuration(**Please modify it according to your actual situation**):

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

2. **Configure the datasources in Grafana**

   You need to add two datasources in Grafana:

   - Prometheus: It is the Prometheus instance that scrapes the GreptimeDB metrics.
   - Information Schema: It is the MySQL port of the current monitored instance. The `overview` dashboard will use this datasource to show the information schema of the current instance.

3. **Import the dashboards based on your deployment scenario**

   - Cluster deployment: Import the `cluster/` dashboard.
   - Standalone deployment: Import the `standalone/` dashboard.

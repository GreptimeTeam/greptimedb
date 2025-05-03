# Grafana dashboards for GreptimeDB

## Overview

This repository contains Grafana dashboards for visualizing metrics and logs of GreptimeDB instances running in either cluster or standalone mode. **The Grafana version should be greater than 9.0**.

We highly recommend using the self-monitoring feature provided by [GreptimeDB Operator](https://github.com/GrepTimeTeam/greptimedb-operator) to automatically collect metrics and logs from your GreptimeDB instances and store them in a dedicated GreptimeDB instance.

- **Metrics Dashboards**

  - `dashboards/metrics/cluster/dashboard.json`: The Grafana dashboard for the GreptimeDB cluster. Read the [dashboard.md](./dashboards/metrics/cluster/dashboard.md) for more details.
  
  - `dashboards/metrics/standalone/dashboard.json`: The Grafana dashboard for the standalone GreptimeDB instance. **It's generated from the `cluster/dashboard.json` by removing the instance filter through the `make dashboards` command**. Read the [dashboard.md](./dashboards/metrics/standalone/dashboard.md) for more details.

- **Logs Dashboard**

  The `dashboards/logs/dashboard.json` provides a comprehensive Grafana dashboard for visualizing GreptimeDB logs. To utilize this dashboard effectively, you need to collect logs in JSON format from your GreptimeDB instances and store them in a dedicated GreptimeDB instance.

  For proper integration, the logs table must adhere to the following schema design with the table name `_gt_logs`:

  ```sql
  CREATE TABLE IF NOT EXISTS `_gt_logs` (
    `pod_ip` STRING NULL,
    `namespace` STRING NULL,
    `cluster` STRING NULL,
    `file` STRING NULL,
    `module_path` STRING NULL,
    `level` STRING NULL,
    `target` STRING NULL,
    `role` STRING NULL,
    `pod` STRING NULL SKIPPING INDEX WITH(granularity = '10240', type = 'BLOOM'),
    `message` STRING NULL FULLTEXT INDEX WITH(analyzer = 'English', backend = 'bloom', case_sensitive = 'false'),
    `err` STRING NULL FULLTEXT INDEX WITH(analyzer = 'English', backend = 'bloom', case_sensitive = 'false'),
    `timestamp` TIMESTAMP(9) NOT NULL,
    TIME INDEX (`timestamp`),
    PRIMARY KEY (`level`, `target`, `role`)
  )
    ENGINE=mito
  WITH (
    append_mode = 'true'
  )
  ```

## Development

As GreptimeDB evolves rapidly, metrics may change over time. We welcome your feedback and contributions to improve these dashboards 🤗

To modify the metrics dashboards, simply edit the `dashboards/metrics/cluster/dashboard.json` file and run the `make dashboards` command. This will automatically generate the updated `dashboards/metrics/standalone/dashboard.json` and other related files.

For easier dashboard maintenance, we utilize the [`dac`](https://github.com/zyy17/dac) tool to generate human-readable intermediate dashboards and documentation:

- `dashboards/metrics/cluster/dashboard.yaml`: The intermediate dashboard file for the GreptimeDB cluster.
- `dashboards/metrics/standalone/dashboard.yaml`: The intermediate dashboard file for standalone GreptimeDB instances.

## Data Sources

The following data sources are used to fetch metrics and logs:

- **`${metrics}`**: Prometheus data source for providing the GreptimeDB metrics.
- **`${logs}`**: MySQL data source for providing the GreptimeDB logs.
- **`${information_schema}`**: MySQL data source for providing the information schema of the current instance and used for the `overview` panel. It is the MySQL port of the current monitored instance.

## Instance Filters

To deploy the dashboards for multiple scenarios (K8s, bare metal, etc.), we prefer to use the `instance` label when filtering instances.

Additionally, we recommend including the `pod` label in the legend to make it easier to identify each instance, even though this field will be empty in bare metal scenarios.

For example, the following query is recommended:

```promql
sum(process_resident_memory_bytes{instance=~"$datanode"}) by (instance, pod)
```

And the legend will be like: `[{{instance}}]-[{{ pod }}]`.

## Deployment

### (Recommended) Helm Chart

If you use the [Helm Chart](https://github.com/GreptimeTeam/helm-charts) to deploy a GreptimeDB cluster, you can enable self-monitoring by setting the following values in your Helm chart:

- `monitoring.enabled=true`: Deploys a standalone GreptimeDB instance dedicated to monitoring the cluster;
- `grafana.enabled=true`: Deploys Grafana and automatically imports the monitoring dashboard;

The standalone GreptimeDB instance will collect metrics from your cluster, and the dashboard will be available in the Grafana UI. For detailed deployment instructions, please refer to our [Kubernetes deployment guide](https://docs.greptime.com/nightly/user-guide/deployments/deploy-on-kubernetes/getting-started).

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

2. **Configure the data sources in Grafana**

   You need to add two data sources in Grafana:

   - Prometheus: It is the Prometheus instance that scrapes the GreptimeDB metrics.
   - Information Schema: It is the MySQL port of the current monitored instance. The dashboard will use this datasource to show the information schema of the current instance.

3. **Import the dashboards based on your deployment scenario**

   - **Cluster**: Import the `dashboards/metrics/cluster/dashboard.json` dashboard.
   - **Standalone**: Import the `dashboards/metrics/standalone/dashboard.json` dashboard.

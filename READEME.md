<div id="top"></div>
<!--
*** Thanks for checking out the Best-README-Template. If you have a suggestion
*** that would make this better, please fork the repo and create a pull request
*** or simply open an issue with the tag "enhancement".
*** Don't forget to give the project a star!
*** Thanks again! Now go create something AMAZING! :D
-->

<!-- PROJECT SHIELDS -->
<!--
*** I'm using markdown "reference style" links for readability.
*** Reference links are enclosed in brackets [ ] instead of parentheses ( ).
*** See the bottom of this document for the declaration of the reference variables
*** for contributors-url, forks-url, etc. This is an optional, concise syntax you may use.
*** https://www.markdownguide.org/basic-syntax/#reference-style-links
-->

<!-- PROJECT LOGO -->
<br />
<div align="center">

  <h3 align="center">All Purpose Bypass</h3>

  <p align="center">
    Maybe save time but definitley save MONEY
    <br />
    <br />
  </p>
</div>

<!-- ABOUT THE PROJECT -->

## About The Project

![Alt Text](https://i.imgur.com/C63Fd5g.png)

In Databricks, running code on an All Purpose Cluster is over 3x the cost of running on a Job Cluster. All Purpose Bypass provides a convenient method to quickly convert your Notebook into a job saving you time and money.

It is perfect for when you know you will have a long running command block or plan on leaving a notebook running overnight.

<p align="right">(<a href="#top">back to top</a>)</p>

### Built With

- [python](https://www.python.org/)
- [spark](https://spark.apache.org/)
- [databricks](https://databricks.com/)

<p align="right">(<a href="#top">back to top</a>)</p>

### Prerequisites

Databricks

- This tool is meant to be used in databricks workspaces

<p align="right">(<a href="#top">back to top</a>)</p>

### Installation

pip install in your Databricks Notebook

```python
%pip install all_purpose_bypass
```

<p align="right">(<a href="#top">back to top</a>)</p>

#### Quickstart

```python
from all_purpose_bypass import Bypass

# Databricks API Token (Found in User Settings)
api_token = '###############################'

bypass = Bypass(api_token)
job_id = bypass.create_job()
bypass.run_job(job_id)

>>> Job located at: https://my-workspace.cloud.databricks.com/?#job/571474934623337
>>> Job Running: run_id is 1535015
```

#### Default Behavior

By default `Bypass` will create a Job named after the current notebook and assign the owner to the current user. The Job cluster that is created is a clone of the attached active all purpose cluster. To make the job more discoverable a tag of `all-purpose-bypass` is assigned to every job. If the job already exists, the parameters/options are updated.

![Alt Text](https://i.imgur.com/4mREMmr.png)
![Alt Text](https://i.imgur.com/C0Qj58t.png)

#### Advanced Usage

> Note: You can create cluster compatibility issues. Please check with the databricks create cluster page to make sure the options are compatible with each other.

There are a number of arguments you pass to `Bypass` to modify the default behavior.

Parameters:

- new_cluster: pass in your own json like dictionary with cluster configurations
  - https://docs.databricks.com/dev-tools/api/latest/clusters.html#examples
    ```python
    {
        "cluster_name": "autoscaling-cluster",
        "spark_version": "7.3.x-scala2.12",
        "node_type_id": "i3.xlarge",
        "autoscale" : {
            "min_workers": 2,
            "max_workers": 50},
        "aws_attributes": {
            "availability": "SPOT",
            "zone_id": "us-west-2a"}
    }
    ```
- spark_version: modify the spark_version of the default current active all purpose cluster
  - https://docs.databricks.com/dev-tools/api/latest/clusters.html#runtime-versions
- node_type_id: modify the node_type_id of the default current active all purpose cluster
  - https://docs.databricks.com/dev-tools/api/latest/clusters.html#list-node-types
- aws_attributes: modify the aws_attributes of the default current active all purpose cluster
- autoscale: modify the autoscale of the default current active all purpose cluster
  - if this parameter is set do not use `num_workers`
- num_workers: modify the num_workers of the default current active all purpose cluster
  - if this parameter is set do not use `autoscale`
- libraries: modify the libraries of the default current active all purpose cluster
- clusterId: change the default current active all purpose cluster to anothe existing all purpose cluster

Example:

```python
from all_purpose_bypass import Bypass

# Databricks API Token (Found in User Settings)
api_token = '###############################'

bypass = Bypass(api_token, node_type_id="i3.4xlarge", clusterId="1095-225741-yhdswzetj")
job_id = bypass.create_job()
bypass.run_job(job_id)

>>> Job located at: https://my-workspace.cloud.databricks.com/?#job/571474934623337
>>> Job Running: run_id is 1535015
```

<p align="right">(<a href="#top">back to top</a>)</p>

## License

Distributed under the MIT License. See `LICENSE.txt` for more information.

<p align="right">(<a href="#top">back to top</a>)</p>

import requests
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.dbutils import DBUtils

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)
dbutils = DBUtils(spark)


class Bypass:
    def __init__(
        self,
        api_token,
        new_cluster=None,
        spark_version=None,
        node_type_id=None,
        aws_attributes=None,
        autoscale=None,
        num_workers=None,
        libraries=None,
        clusterId=None,
    ):

        if clusterId is None:
            clusterId = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")

        workspaceUrl = spark.conf.get("spark.databricks.workspaceUrl")

        notebook_path = (
            dbutils.notebook.entry_point.getDbutils()
            .notebook()
            .getContext()
            .notebookPath()
            .get()
        )

        try:
            current_user = (
                dbutils.notebook.entry_point.getDbutils()
                .notebook()
                .getContext()
                .userName()
                .get()
            )
        except:
            current_user = None

        # if user does not provide a new cluster then default to the class template
        if new_cluster is None:

            # pull in the clusterId provided detail to use as a base for the new_cluster
            response = requests.get(
                f"https://{workspaceUrl}/api/2.0/clusters/get",
                headers={"Authorization": f"Bearer {api_token}"},
                json={"cluster_id": clusterId},
            )

            # if cluster attributes arguments were not provided,
            # use the cluster attributes from the api call above
            if node_type_id is None:
                node_type_id = response.json()["node_type_id"]
            if aws_attributes is None:
                aws_attributes = response.json()["aws_attributes"]
            if spark_version is None:
                spark_version = response.json()["spark_version"]

            if autoscale is None and num_workers is None:
                try:
                    autoscale = response.json()["autoscale"]
                    num_workers = None
                except:
                    num_workers = response.json()["num_workers"]
                    autoscale = None

            new_cluster = {
                "spark_version": spark_version,
                "node_type_id": node_type_id,
                "aws_attributes": aws_attributes,
                "autoscale": autoscale,
                "num_workers": num_workers,
            }

            # remove a num_workers or autoscale attribute as you can't have both
            if autoscale:
                new_cluster.pop("num_workers", None)

            if num_workers:
                new_cluster.pop("autoscale", None)

        # if no library argument was provided use the api to provide the clusterId libraries
        if libraries is None:
            libraries = []
            response = requests.get(
                f"https://{workspaceUrl}/api/2.0/libraries/all-cluster-statuses",
                headers={"Authorization": f"Bearer {api_token}"},
                json={"cluster_id": clusterId},
            )

            for cluster in response.json()["statuses"]:
                if cluster["cluster_id"] == clusterId:
                    statuses = cluster.get("library_statuses", None)
                    if statuses:
                        for status in statuses:
                            libraries.append(status["library"])

        # the request body template for creating the task
        self.job_schema = {
            "name": notebook_path,
            "tags": {"type": "all-purpose-bypass"},
            "tasks": [
                {
                    "task_key": notebook_path.split("/")[-1],
                    "job_cluster_key": "all-purpose-bypass",
                    "notebook_task": {
                        "notebook_path": notebook_path,
                        "source": "WORKSPACE",
                        "base_parameters": {"type": "all-purpose-bypass"},
                    },
                    "libraries": libraries,
                    "max_retries": 1,
                }
            ],
            "job_clusters": [
                {"job_cluster_key": "all-purpose-bypass", "new_cluster": new_cluster}
            ],
            "email_notifications": {
                "on_start": [current_user],
                "on_success": [current_user],
                "on_failure": [current_user],
            },
            "max_concurrent_runs": 10,
            "format": "MULTI_TASK",
        }

        # create instance variable to reference in below methods
        self.api_token = api_token
        self.clusterId = clusterId
        self.workspaceUrl = workspaceUrl
        self.notebook_path = notebook_path

        # prevent a loop in the job
        try:
            dbutils.widgets.get("type")
            self.job_running = True
        except:
            if spark.conf.get("spark.databricks.clusterSource") == "JOB":
                self.job_running = True
            else:
                self.job_running = False

    def create_job(self):

        if not self.job_running:

            existing_job = {}
            response = requests.get(
                f"https://{self.workspaceUrl}/api/2.1/jobs/list",
                headers={"Authorization": f"Bearer {self.api_token}"},
            )

            for job in response.json()["jobs"]:
                if (
                    self.notebook_path == job["settings"]["name"]
                    and job["settings"]["tags"]["type"] == "all-purpose-bypass"
                ):
                    existing_job.update(job)

            if existing_job:

                reset_schema = {
                    "job_id": existing_job["job_id"],
                    "new_settings": self.job_schema,
                }

                response = requests.post(
                    f"https://{self.workspaceUrl}/api/2.1/jobs/reset",
                    headers={"Authorization": f"Bearer {self.api_token}"},
                    json=reset_schema,
                )
                if response.status_code == 200:
                    job_id = existing_job["job_id"]
                    print(f"Job located at: https://{self.workspaceUrl}/?#job/{job_id}")
                    return job_id
                else:
                    print(response.json())
                    return None
            else:
                response = requests.post(
                    f"https://{self.workspaceUrl}/api/2.1/jobs/create",
                    headers={"Authorization": f"Bearer {self.api_token}"},
                    json=self.job_schema,
                )

                if response.status_code == 200:
                    job_id = response.json()["job_id"]
                    print(f"Job located at: https://{self.workspaceUrl}/?#job/{job_id}")
                    return job_id
                else:
                    print(response.json())
                    return None
        else:
            return None

    def run_job(self, job_id):

        if job_id:
            run_schema = {
                "job_id": job_id,
            }

            response = requests.post(
                f"https://{self.workspaceUrl}/api/2.1/jobs/run-now",
                headers={"Authorization": f"Bearer {self.api_token}"},
                json=run_schema,
            )
            if response.status_code == 200:
                print(f"Job Running: run_id is {response.json()['run_id']}")
            else:
                print(response.json())
        else:
            print("jobId is empty, hopefully this is a job :)")


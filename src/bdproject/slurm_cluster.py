from pathlib import Path
from urllib.parse import urlparse

from dask.utils import parse_bytes
from dask_jobqueue import SLURMCluster
from distributed import Client, Security

RUNNING_INSIDE_CLUSTER = Path("/d").exists()


def slurm_cluster_start(memory_per_worker: str, processes_per_job: int, jobs: int = 1):
    memory_bytes = parse_bytes(memory_per_worker)

    try:
        client = Client.current()
    except ValueError:
        pass
    else:
        client.shutdown()

    spill_dir = (Path.home() / "dask_spill").absolute()
    spill_dir.mkdir(exist_ok=True)
    log_dir = (Path.home() / "dask_log").absolute()
    log_dir.mkdir(exist_ok=True)
    shared_temp_dir = (Path.home() / "dask_temp").absolute()
    shared_temp_dir.mkdir(exist_ok=True)

    security = Security.temporary()

    cluster = SLURMCluster(
        cores=1,
        memory=memory_bytes * processes_per_job,
        processes=processes_per_job,
        asynchronous=False,
        security=security,
        local_directory=spill_dir,
        log_directory=log_dir,
        shared_temp_directory=shared_temp_dir,
        scheduler_options={
            "preload": ["bdproject.worker_preload"],
        },
    )
    cluster.scale(jobs=jobs)

    client = Client(
        address=cluster,
        security=security,
        asynchronous=False,
        serializers=["dask", "pickle"],
        deserializers=["dask", "pickle", "msgpack"],
        timeout="120s",
    )

    print(f"Dashboard at {urlparse(client.dashboard_link).netloc}")

    return client

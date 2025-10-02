from pathlib import Path
from time import sleep

from dask.utils import parse_bytes
from distributed import Client, LocalCluster, Security
from psutil import virtual_memory


def get_cluster_config(
    memory_per_worker: str,
    max_memory: str | None = None,
) -> tuple[int, int]:
    if max_memory is None:
        max_memory = "+0"
    max_memory_bytes = parse_bytes(max_memory)
    max_memory_bytes = (
        virtual_memory().total + max_memory_bytes
        if max_memory.startswith(("-", "+"))
        else max_memory_bytes
    )
    memory_per_worker_bytes = parse_bytes(memory_per_worker)
    n_workers = max(1, max_memory_bytes // memory_per_worker_bytes)
    return n_workers, memory_per_worker_bytes


def local_cluster_rescale(
    memory_per_worker: str,
    max_memory: str | None = None,
    cluster: LocalCluster | None = None,
):
    if cluster is None:
        cluster = Client.current().cluster
    n_workers, memory_limit = get_cluster_config(memory_per_worker, max_memory)
    cluster.new_spec["options"]["memory_limit"] = memory_limit
    cluster.scale(0)
    while len(cluster.workers) > 0:
        sleep(0.1)
    cluster.scale(n_workers)


def local_cluster_start(
    memory_per_worker: str | None = None,
    max_memory: str | None = None,
) -> Client:
    try:
        client = Client.current()
    except ValueError:
        pass
    else:
        client.shutdown()

    spill_dir = (Path.home() / "dask_spill").absolute()
    spill_dir.mkdir(exist_ok=True)

    if memory_per_worker is None:
        n_workers = 0
        memory_limit = 0
    else:
        n_workers, memory_limit = get_cluster_config(memory_per_worker, max_memory)

    security = Security.temporary()

    cluster = LocalCluster(
        n_workers=n_workers,
        threads_per_worker=1,
        memory_limit=memory_limit,
        asynchronous=False,
        security=security,
        local_directory=spill_dir,
        preload=["bdproject.worker_preload"],
    )

    client = Client(
        address=cluster,
        security=security,
        asynchronous=False,
        serializers=["dask", "pickle"],
        deserializers=["dask", "pickle", "msgpack"],
        timeout="120s",
    )

    return client

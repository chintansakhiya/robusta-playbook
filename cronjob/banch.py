import json
import logging

from hikaru.model.rel_1_26 import (
    Container,
    ObjectMeta,
    PersistentVolumeClaim,
    PersistentVolumeClaimSpec,
    PersistentVolumeClaimVolumeSource,
    PodSpec,
    ResourceRequirements,
    Volume,
    VolumeMount,
)

from typing import List

from robusta.api import (
    IMAGE_REGISTRY,
    INSTALLATION_NAMESPACE,
    ExecutionBaseEvent,
    Finding,
    FindingType,
    MarkdownBlock,
    PodRunningParams,
    RobustaJob,
    action,
    BaseBlock,
    TableBlock,
)


class DiskBenchmarkParams(PodRunningParams):
    """
    :var pvc_name: Name of the pvc created for the benchmark.
    :var test_seconds: The benchmark duration.
    :var namespace: Namespace used for the benchmark.
    :var disk_size: The size of pvc used for the benchmark.
    :var storage_class_name: Pvc storage class, From the available cluster storage classes. standard/fast/etc.
    """

    pvc_name: str = "robusta-disk-benchmark"
    test_seconds: int = 20
    namespace: str = INSTALLATION_NAMESPACE
    disk_size: str = "10Gi"
    storage_class_name: str


def format_float_per2(f_param):
    return "{:.2f}".format(f_param)+" KB"


@action
def custom_disk_benchmark(event: ExecutionBaseEvent, action_params: DiskBenchmarkParams):
    """
    Run disk benchmark in your cluster.
    The benchmark creates a PVC, using the configured storage class, and runs the benchmark using fio.
    For more details: https://fio.readthedocs.io/en/latest/
    """
    pvc = PersistentVolumeClaim(
        metadata=ObjectMeta(name=action_params.pvc_name, namespace=action_params.namespace),
        spec=PersistentVolumeClaimSpec(
            accessModes=["ReadWriteOnce"],
            storageClassName=action_params.storage_class_name,
            resources=ResourceRequirements(requests={"storage": action_params.disk_size}),
        ),
    )
    try:
        pvc.createNamespacedPersistentVolumeClaim(action_params.namespace)
        pv_name = "robusta-benchmark-pv"
        image = f"{IMAGE_REGISTRY}/robusta-fio-benchmark"
        name = "robusta-fio-disk-benchmark"
        mount_path = "/robusta/data"
        spec = PodSpec(
            volumes=[
                Volume(
                    name=pv_name,
                    persistentVolumeClaim=PersistentVolumeClaimVolumeSource(claimName=action_params.pvc_name),
                )
            ],
            containers=[
                Container(
                    name=name,
                    image=image,
                    imagePullPolicy="Always",
                    volumeMounts=[VolumeMount(mountPath=mount_path, name=pv_name)],
                    args=[
                        "--directory",
                        mount_path,
                        "--output-format",
                        "json",
                        "--group_reporting",
                        "--runtime",
                        f"{action_params.test_seconds}",
                        "/jobs/rand-rw.fio",
                    ],
                )
            ],
            restartPolicy="Never",
        )
        print("8888888888888888888888888888")
        print(spec)
        print("******************************")
        print(action_params)
        json_output = json.loads(
            RobustaJob.run_simple_job_spec(
                spec, name, 120 + action_params.test_seconds, custom_annotations=action_params.custom_annotations
            ).replace("'", '"'),
        )
        job = json_output["jobs"][0]
        cluster= event._context
        benchmark_results = (
            f"\nfio benchmark:\n"
            f"Total Time: {action_params.test_seconds} Sec\n"
            f"Read Band Width: {format_float_per2(job['read']['bw'])} KB \n"
            f"Read IO Ops/Sec: {format_float_per2(job['read']['iops'])}\n"
            f"Write Band Width: {format_float_per2(job['write']['bw'])} KB \n"
            f"Write Ops/Sec: {format_float_per2(job['write']['iops'])}\n"
            f"cluster: {cluster.cluster_name}\n"
            f"account_id: {cluster.account_id}\n "
        )

        logging.info(benchmark_results)

        block_list: List[BaseBlock] = []
        
        effected_pods_rows = [ListBanckMark(job,action_params,cluster)]
        block_list.append(
            TableBlock(effected_pods_rows, ["account_id","cluster_name","total-time", "read-band-width", "read-IO-ops/sec","write-band-width","write-ops/sec"], table_name=f"banchmark results")
        )
        event.add_enrichment(block_list)
    finally:
        pvc.deleteNamespacedPersistentVolumeClaim(name=action_params.pvc_name, namespace=action_params.namespace)

def ListBanckMark(job,action_params,cluster)->List[str]:
    return [
        cluster.account_id,cluster.cluster_name,action_params.test_seconds,format_float_per2(job['read']['bw']),format_float_per2(job['read']['iops']),format_float_per2(job['write']['bw']),format_float_per2(job['write']['iops'])
    ]

from robusta.api import (
    BaseBlock,
    NodeEvent,
    JobEvent,
    TableBlock,
    action,
    ExecutionBaseEvent,
)

from typing import List
from hikaru.model.rel_1_26 import Pod, PodList ,CronJobList,PodList
 
 

def CronJobListLoop(i: CronJobList) -> List[str]:
    return [
        i.metadata.name,i.metadata.namespace,i.spec.schedule,i.metadata.creationTimestamp,i.status.lastScheduleTime
    ]

@action
def get_cronjob(event: JobEvent):
    """
    Enrich the finding with pods running on this node, along with the 'Ready' status of each pod.
    """
    # job=event
    details=get_all_cronjob_details()
    block_list: List[BaseBlock] = []
    effected_pods_rows = [CronJobListLoop(pod) for pod in details.items]
    block_list.append(
        TableBlock(effected_pods_rows, ["name", "namespace", "schedule","creationTimestamp","lastScheduleTime"], table_name=f"cronjob running on the node")
    )
    print("block_list",block_list)
    event.add_enrichment(block_list)

@action
def get_cronjob_sec(event: ExecutionBaseEvent):
    """
    Enrich the finding with pods running on this node, along with the 'Ready' status of each pod.
    """
    details=get_all_cronjob_details()
    block_list: List[BaseBlock] = []
    effected_pods_rows = [CronJobListLoop(pod) for pod in details.items]
    block_list.append(
        # todo: add command ,job name, last three job status,status
        TableBlock(effected_pods_rows, ["name", "namespace", "schedule","creationTimestamp","lastScheduleTime"], table_name=f"cronjob running on the node")
    )

    print("block_list",block_list)
    event.add_enrichment(block_list)

def get_all_pod_details():
    try:
        pod_list_model = PodList.listPodForAllNamespaces().obj
        # print(pod_list_model.items)
        # Return the pod list model
        return pod_list_model

    except Exception as e:
        print(f"Exception when calling get_all_pod_details: {e}")
        return None

def get_all_cronjob_details():
    try:
        cron_jobs_details = CronJobList.listCronJobForAllNamespaces().obj
        return cron_jobs_details
    except Exception as e:
        print("Exception when calling CoreV1->listCronJobForAllNamespaces: %s\n" % e)

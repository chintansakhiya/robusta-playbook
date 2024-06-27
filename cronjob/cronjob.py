from robusta.api import (
    BaseBlock,
    JobEvent,
    TableBlock,
    action,
    ExecutionBaseEvent,
)

from typing import List
from hikaru.model.rel_1_26 import  CronJobList
 
 
def CronJobListLoop(i: CronJobList,account_id,cluster_name) -> List[str]:
    return [
        account_id,cluster_name,i.metadata.name,i.metadata.namespace,i.spec.schedule,i.status.lastScheduleTime,i.status.lastSuccessfulTime,i.spec.jobTemplate.spec.template.spec.containers[0].command,i.spec.jobTemplate.spec.template.spec.containers[0].args
    ]

@action
def list_cronjobs(event: JobEvent):
    """
    Enrich the finding with cronjob running on this node.
    """
    # job=event
    cluster=event._context
    details=get_all_cronjobs_details()
    block_list: List[BaseBlock] = []
    if len(details.items)!=0:
        effected_pods_rows = [CronJobListLoop(pod,cluster.account_id,cluster.cluster_name) for pod in details.items]
        block_list.append(
            TableBlock(effected_pods_rows, ["account_id","cluster_name","name", "namespace", "schedule","lastScheduleTime","lastSuccessfulTime","command","args"], table_name=f"cronjob running on the node")
        )
    event.add_enrichment(block_list)

@action
def list_cronjobs_schedule(event: ExecutionBaseEvent):
    """
    Enrich the finding with cronjob running on this node.
    """
    cluster=event._context
    details=get_all_cronjobs_details()
    block_list: List[BaseBlock] = []
    if len(details.items)!=0:
        effected_pods_rows = [CronJobListLoop(pod,cluster.account_id,cluster.cluster_name) for pod in details.items]
        block_list.append(
            TableBlock(effected_pods_rows, ["account_id","cluster_name","name", "namespace", "schedule","lastScheduleTime","lastSuccessfulTime","command","args"], table_name=f"cronjob running on the node")
        )
    event.add_enrichment(block_list)

def get_all_cronjobs_details():
    try:
        cron_jobs_details = CronJobList.listCronJobForAllNamespaces().obj
        return cron_jobs_details
    except Exception as e:
        print("Exception when calling CoreV1->listCronJobForAllNamespaces: %s\n" % e)
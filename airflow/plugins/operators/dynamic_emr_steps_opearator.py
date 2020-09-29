from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.utils.decorators import apply_defaults


class DynamicEMRStepsOperator(EmrAddStepsOperator):
    @apply_defaults
    def __init__(
            self,
            job_flow_id=None,
            steps="",
            params="{}",
            *args, **kwargs):
        super().__init__(
            job_flow_id=job_flow_id,
            steps=steps,
            params=params,
            *args, **kwargs)

    def execute(self, context):
        try:
            self.log.info('exec_date:{}'.format(context['ds_nodash']))
            self.params['exec_date'] = context['ds_nodash']
            self.log.info('params:{}'.format(self.params))
            return super().execute(context)
        except Exception as e:
            self.log.error('Error:{}'.format(e))
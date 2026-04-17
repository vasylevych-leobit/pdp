"""
Task 5: Email notification helpers.
The Airflow EmailOperator is configured directly in the DAG,
but this module provides the callback used for failure alerts
on any task (on_failure_callback) and the SLA miss callback.
"""

import logging
from datetime import datetime

log = logging.getLogger(__name__)


def build_success_html(execution_date: datetime, dq_results):
    dq_section = ""
    if dq_results:
        rows = "".join(
            f"<tr><td style='padding:4px 12px'>{k}</td>"
            f"<td style='padding:4px 12px'><b>{v}</b></td></tr>"
            for k, v in dq_results.items()
        )
        dq_section = f"""
        <h3>Data quality results</h3>
        <table border='1' cellspacing='0' cellpadding='0' style='border-collapse:collapse'>
          <tr><th style='padding:4px 12px'>Check</th><th style='padding:4px 12px'>Value</th></tr>
          {rows}
        </table>
        """

    return f"""
    <html><body>
      <h2 style='color:green'>Pipeline SUCCESS</h2>
      <p><b>Execution date:</b> {execution_date.strftime('%Y-%m-%d %H:%M UTC')}</p>
      <p>All tasks completed without errors.</p>
      {dq_section}
    </body></html>
    """


def build_failure_html(context):
    """Returns (subject, html_content) for a failure email."""
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    execution_date = context["execution_date"]
    exception = context.get("exception", "unknown error")
    log_url = context["task_instance"].log_url

    subject = f"[AIRFLOW FAILURE] {dag_id} / {task_id} @ {execution_date}"
    html = f"""
    <html><body>
      <h2 style='color:red'>Pipeline FAILURE</h2>
      <table>
        <tr><td><b>DAG</b></td><td>{dag_id}</td></tr>
        <tr><td><b>Task</b></td><td>{task_id}</td></tr>
        <tr><td><b>Execution date</b></td><td>{execution_date}</td></tr>
        <tr><td><b>Error</b></td><td><pre>{exception}</pre></td></tr>
        <tr><td><b>Logs</b></td><td><a href='{log_url}'>{log_url}</a></td></tr>
      </table>
    </body></html>
    """
    return subject, html


def on_failure_callback(context):
    """
    Attached to every task via default_args.
    Sends a failure e-mail using Airflow's built-in send_email utility.
    """
    from airflow.utils.email import send_email

    subject, html = build_failure_html(context)
    recipients = context["dag"].default_args.get("email", [])

    if not recipients:
        log.warning("on_failure_callback: no email recipients configured, skipping.")
        return

    log.info("Sending failure email to %s", recipients)
    send_email(to=recipients, subject=subject, html_content=html)


def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:
    """
    Called by the Airflow Scheduler when a task exceeds its SLA window.
    Signature is fixed by Airflow — do not change parameter names.
    """
    from airflow.utils.email import send_email

    recipients = dag.default_args.get("email", [])
    if not recipients:
        log.warning("sla_miss_callback: no email recipients configured, skipping.")
        return

    missed = ", ".join(str(t) for t in task_list)
    subject = f"[AIRFLOW SLA MISS] {dag.dag_id} — tasks: {missed}"
    html = f"""
    <html><body>
      <h2 style='color:orange'>SLA Miss Detected</h2>
      <p><b>DAG:</b> {dag.dag_id}</p>
      <p><b>Tasks that missed SLA:</b> {missed}</p>
      <p><b>Blocking tasks:</b> {', '.join(str(t) for t in blocking_task_list)}</p>
      <p>These tasks did not complete within their configured SLA window.
         The tasks are still running and have NOT been stopped.</p>
    </body></html>
    """
    log.warning("SLA miss detected for tasks: %s", missed)
    send_email(to=recipients, subject=subject, html_content=html)

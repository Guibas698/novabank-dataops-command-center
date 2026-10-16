"""DAG do Apache Airflow: Orquestração dos 12 Pipelines Corporativos (ERP -> DRE / Fluxo de Caixa) com Self-Healing."""

from __future__ import annotations

from datetime import datetime, timedelta

from dataops_command_center.chaos_and_rca import DataOpsCommandCenter

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
except ImportError:
    DAG = None  # type: ignore[assignment]
    PythonOperator = None  # type: ignore[assignment]


def run_corporate_observability_check() -> dict:
    cmd = DataOpsCommandCenter()
    return cmd.sre_kpis()


if DAG is not None:
    with DAG(
        dag_id="novabank_corporate_dre_and_observability",
        description="Orquestra os pipelines corporativos (ERP, Folha, Tesouraria, DRE) e valida Data Contracts ODCS",
        schedule_interval="*/30 * * * *",
        start_date=datetime(2026, 1, 1),
        catchup=False,
        default_args={"owner": "dataops-sre-novabank", "retries": 2, "retry_delay": timedelta(minutes=3)},
        tags=["dataops", "dre", "observability", "contracts", "self-healing"],
    ) as dag:
        PythonOperator(
            task_id="validate_contracts_and_lineage_health",
            python_callable=run_corporate_observability_check,
        )

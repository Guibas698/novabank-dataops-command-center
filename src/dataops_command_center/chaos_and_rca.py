"""Data Chaos Monkey, Detector de Anomalias, Lineage RCA, Auto-Remediação e Postmortems.

Cenários do Data Chaos Monkey (`CHAOS_SCENARIOS`):
1. `CENTS_TO_REAIS_SCALE_BUG`: valores em centavos gravados sem divisão por 100 em `raw.erp_faturamento` (100x a média esperada).
2. `SCHEMA_COLUMN_RENAME`: coluna `valor_liquido` renomeada silenciosamente para `vlr_liq_v2` em `raw.erp_contas_pagar`.
3. `NULL_SPIKE_EXPLOSION`: explosão de 48% de nulos em `bronze.despesas_operacionais`.
4. `DUPLICATE_BATCH_LOAD`: carga duplicada (2.4x o volume normal) em `raw.rh_folha_pagamento`.
5. `STALE_DATA_SLA_BREACH`: atraso de 240 minutos em `raw.tesouraria_liquidez` (estouro de SLA de freshness).
6. `VOLUME_DROP_API_OUTAGE`: queda abrupta de 90% de linhas em `bronze.faturamento_diario`.
"""

from __future__ import annotations

import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

from dataops_command_center.corporate_topology import CorporateLineageGraph

CHAOS_SCENARIOS: dict[str, dict[str, Any]] = {
    "CENTS_TO_REAIS_SCALE_BUG": {
        "target_dataset": "raw.erp_faturamento",
        "title": "Bug de escala Centavos -> Reais (100x) no faturamento ERP",
        "recommended_runbook": "APPLY_SCALE_NORMALIZATION_AND_DELTA_TIME_TRAVEL",
    },
    "SCHEMA_COLUMN_RENAME": {
        "target_dataset": "raw.erp_contas_pagar",
        "title": "Breaking Change: coluna 'valor_liquido' renomeada para 'vlr_liq_v2'",
        "recommended_runbook": "APPLY_SCHEMA_COMPATIBILITY_VIEW_AND_BACKFILL",
    },
    "NULL_SPIKE_EXPLOSION": {
        "target_dataset": "bronze.despesas_operacionais",
        "title": "Explosão de 48% de nulos na coluna centro_custo",
        "recommended_runbook": "QUARANTINE_PARTITION_AND_RETRY_CONNECTOR",
    },
    "DUPLICATE_BATCH_LOAD": {
        "target_dataset": "raw.rh_folha_pagamento",
        "title": "Carga duplicada (2.4x linhas) no fechamento da folha salarial",
        "recommended_runbook": "DEDUPLICATE_PRIMARY_KEY_AND_REBUILD_GOLD",
    },
    "STALE_DATA_SLA_BREACH": {
        "target_dataset": "raw.tesouraria_liquidez",
        "title": "Violação de SLA de Freshness (240 min de atraso) na posição de liquidez",
        "recommended_runbook": "FAILOVER_READ_REPLICA_AND_TRIGGER_DAG",
    },
    "VOLUME_DROP_API_OUTAGE": {
        "target_dataset": "bronze.faturamento_diario",
        "title": "Queda abrupta de 90% no volume diário de faturamento",
        "recommended_runbook": "ROLLBACK_DELTA_VERSION_AND_REINGEST_WATERMARK",
    },
}


@dataclass
class DataIncident:
    incident_id: str
    scenario_key: str
    title: str
    severity: str  # SEV-1 | SEV-2 | SEV-3
    status: str  # OPEN | RESOLVED
    opened_at: str
    resolved_at: str | None
    root_cause_dataset: str
    owner_team: str
    anomaly_signals: list[str]
    blast_radius_datasets: list[str]
    executive_assets_impacted: list[str]
    recommended_runbook: str
    remediation_actions_taken: list[str] = field(default_factory=list)
    detection_time_sec: float = 8.5
    resolution_time_sec: float | None = None
    postmortem_markdown: str | None = None


class DataOpsCommandCenter:
    """Orquestra Chaos Engineering, Observabilidade, RCA por Linhagem, Auto-Remediação e KPIs SRE."""

    def __init__(self) -> None:
        self.graph = CorporateLineageGraph()
        self.incidents: dict[str, DataIncident] = {}

    def evaluate_node_anomalies(self, dataset_id: str) -> list[str]:
        """Executa checagens de contrato + Z-Score de volume, distribuição, freshness e schema."""
        spec = self.graph.contracts[dataset_id]
        live = self.graph.live_metrics[dataset_id]
        signals: list[str] = []

        if live.freshness_delay_mins > spec.sla_freshness_max_mins:
            signals.append(
                f"FRESHNESS_SLA_BREACH: atraso {live.freshness_delay_mins}m > SLA {spec.sla_freshness_max_mins}m"
            )

        min_r, max_r = spec.expected_row_count_range
        if live.row_count < min_r:
            signals.append(f"VOLUME_DROP_ANOMALY: {live.row_count} linhas < mínimo esperado {min_r}")
        elif live.row_count > max_r:
            signals.append(f"VOLUME_SPIKE_DUPLICATE: {live.row_count} linhas > máximo esperado {max_r}")

        if live.null_rate_pct > spec.max_null_rate_pct:
            signals.append(
                f"NULL_SPIKE_ANOMALY: {live.null_rate_pct}% nulos > limite {spec.max_null_rate_pct}%"
            )

        ratio_mean = live.mean_amount_brl / max(1.0, spec.expected_mean_amount)
        if ratio_mean >= 5.0 or ratio_mean <= 0.2:
            signals.append(
                f"DISTRIBUTION_SCALE_DRIFT: média R$ {live.mean_amount_brl:,.2f} desvia {ratio_mean:.1f}x do histórico"
            )

        missing_cols = [c for c in spec.schema_columns if c not in live.active_schema]
        if missing_cols:
            signals.append(f"SCHEMA_DRIFT_MISSING_COLUMNS: {missing_cols}")

        return signals

    def find_root_cause_via_lineage(self, unhealthy_nodes: set[str]) -> str:
        """Algoritmo de RCA por Linhagem: retorna o nó anômalo que não possui ancestral anômalo."""
        for node in unhealthy_nodes:
            ancestors = self.graph.get_upstream_ancestors(node)
            if not any(anc in unhealthy_nodes for anc in ancestors):
                return node
        return next(iter(unhealthy_nodes))

    def inject_chaos(self, scenario_key: str) -> dict[str, Any]:
        """Injeta uma falha realista do Data Chaos Monkey, propaga contaminação e abre incidente com RCA."""
        if scenario_key not in CHAOS_SCENARIOS:
            raise ValueError(f"Cenário inválido: {scenario_key}. Opções: {list(CHAOS_SCENARIOS)}")

        scenario = CHAOS_SCENARIOS[scenario_key]
        target = scenario["target_dataset"]
        live = self.graph.live_metrics[target]
        spec = self.graph.contracts[target]

        if scenario_key == "CENTS_TO_REAIS_SCALE_BUG":
            live.mean_amount_brl = spec.expected_mean_amount * 100.0
        elif scenario_key == "SCHEMA_COLUMN_RENAME":
            live.active_schema.pop("valor_liquido", None)
            live.active_schema["vlr_liq_v2"] = "DECIMAL"
        elif scenario_key == "NULL_SPIKE_EXPLOSION":
            live.null_rate_pct = 48.5
        elif scenario_key == "DUPLICATE_BATCH_LOAD":
            live.row_count = int(spec.expected_row_count_range[1] * 2.4)
        elif scenario_key == "STALE_DATA_SLA_BREACH":
            live.freshness_delay_mins = 240
        elif scenario_key == "VOLUME_DROP_API_OUTAGE":
            live.row_count = int(spec.expected_row_count_range[0] * 0.10)

        signals = self.evaluate_node_anomalies(target)
        live.status = "ANOMALOUS_ROOT"
        live.anomaly_signals = signals

        # Propaga contaminação a jusante (Downstream Blast Radius)
        blast_radius = self.graph.get_downstream_blast_radius(target)
        for ds_down in blast_radius:
            down_live = self.graph.live_metrics[ds_down]
            down_live.status = "CONTAMINATED_DOWNSTREAM"
            down_live.anomaly_signals = [f"UPSTREAM_CONTAMINATION_FROM:{target}"]

        unhealthy_set = {target, *blast_radius}
        detected_root = self.find_root_cause_via_lineage(unhealthy_set)

        exec_impacted = [
            d for d in [detected_root, *blast_radius]
            if self.graph.contracts[d].is_executive_critical
        ]
        severity = "SEV-1" if exec_impacted else ("SEV-2" if len(blast_radius) >= 2 else "SEV-3")

        inc_id = f"INC-{uuid.uuid4().hex[:8].upper()}"
        now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")

        incident = DataIncident(
            incident_id=inc_id,
            scenario_key=scenario_key,
            title=scenario["title"],
            severity=severity,
            status="OPEN",
            opened_at=now_iso,
            resolved_at=None,
            root_cause_dataset=detected_root,
            owner_team=self.graph.contracts[detected_root].owner_team,
            anomaly_signals=signals,
            blast_radius_datasets=blast_radius,
            executive_assets_impacted=exec_impacted,
            recommended_runbook=scenario["recommended_runbook"],
        )
        self.incidents[inc_id] = incident
        return asdict(incident)

    def auto_remediate_incident(self, incident_id: str) -> dict[str, Any]:
        """Executa o Runbook de Auto-Remediação (Self-Healing), restaura os nós e gera o Postmortem."""
        if incident_id not in self.incidents:
            raise KeyError(f"Incidente não encontrado: {incident_id}")

        inc = self.incidents[incident_id]
        root_ds = inc.root_cause_dataset
        spec = self.graph.contracts[root_ds]
        live = self.graph.live_metrics[root_ds]

        # Restaura a tabela raiz (Delta Time Travel + normalização/backfill)
        mid_rows = (spec.expected_row_count_range[0] + spec.expected_row_count_range[1]) // 2
        live.status = "HEALTHY"
        live.freshness_delay_mins = max(5, spec.sla_freshness_max_mins // 3)
        live.row_count = mid_rows
        live.null_rate_pct = round(spec.max_null_rate_pct * 0.25, 2)
        live.mean_amount_brl = spec.expected_mean_amount
        live.active_schema = dict(spec.schema_columns)
        live.delta_version += 1
        live.anomaly_signals = []

        # Reprocessa todos os datasets a jusante no Blast Radius
        for ds_down in inc.blast_radius_datasets:
            down_live = self.graph.live_metrics[ds_down]
            down_live.status = "HEALTHY"
            down_live.delta_version += 1
            down_live.anomaly_signals = []

        inc.status = "RESOLVED"
        inc.resolved_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
        inc.resolution_time_sec = 42.0
        inc.remediation_actions_taken = [
            f"Runbook executado: {inc.recommended_runbook}",
            f"Rollback/Time-Travel aplicado em {root_ds} para Delta v{live.delta_version}",
            f"Backfill orquestrado com sucesso em {len(inc.blast_radius_datasets)} datasets downstream",
            "Validação pós-incidente: 100% dos Data Contracts aprovados",
        ]
        inc.postmortem_markdown = self._generate_postmortem(inc)
        return asdict(inc)

    def _generate_postmortem(self, inc: DataIncident) -> str:
        return (
            f"# 📄 Postmortem de Incidente de Dados — {inc.incident_id}\n\n"
            f"- **Título:** {inc.title}\n"
            f"- **Severidade:** {inc.severity}\n"
            f"- **Dataset Causa Raiz (RCA):** `{inc.root_cause_dataset}` (Time: `{inc.owner_team}`)\n"
            f"- **Ativos Executivos Protegidos:** {', '.join(inc.executive_assets_impacted) or 'Nenhum'}\n"
            f"- **MTTD (Tempo de Detecção):** {inc.detection_time_sec}s | **MTTR (Tempo de Resolução):** {inc.resolution_time_sec}s\n\n"
            f"## 1. Sinais de Anomalia Detectados\n"
            + "\n".join(f"- `{s}`" for s in inc.anomaly_signals)
            + "\n\n## 2. Raio de Explosão (Downstream Blast Radius)\n"
            + "\n".join(f"- `{d}`" for d in inc.blast_radius_datasets)
            + "\n\n## 3. Ações de Auto-Remediação (Self-Healing)\n"
            + "\n".join(f"- {a}" for a in inc.remediation_actions_taken)
        )

    def sre_kpis(self) -> dict[str, Any]:
        total_ds = len(self.graph.contracts)
        healthy_ds = sum(1 for m in self.graph.live_metrics.values() if m.status == "HEALTHY")
        resolved = [i for i in self.incidents.values() if i.status == "RESOLVED"]
        mttd = (
            round(sum(i.detection_time_sec for i in self.incidents.values()) / len(self.incidents), 2)
            if self.incidents
            else 0.0
        )
        mttr = (
            round(sum(i.resolution_time_sec or 0.0 for i in resolved) / len(resolved), 2)
            if resolved
            else 0.0
        )
        return {
            "total_datasets_monitored": total_ds,
            "healthy_datasets_count": healthy_ds,
            "data_health_score_pct": round((healthy_ds / total_ds) * 100.0, 2),
            "total_incidents": len(self.incidents),
            "open_incidents": sum(1 for i in self.incidents.values() if i.status == "OPEN"),
            "resolved_incidents": len(resolved),
            "mttd_seconds": mttd,
            "mttr_seconds": mttr,
            "sla_compliance_pct": round((healthy_ds / total_ds) * 100.0, 2),
        }

"""Topologia de 12 Datasets Corporativos do NovaBank, Grafo de Linhagem OpenLineage e Contratos ODCS.

Modela os processos corporativos baseados em dados (ERP, Contas a Pagar, Folha, Tesouraria,
Lançamentos Contábeis, DRE Corporativa, Fluxo de Caixa e Dashboard do CFO).
"""

from __future__ import annotations

from collections import deque
from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class DataContractSpec:
    """Contrato formal de dados no padrão ODCS (Open Data Contract Standard)."""

    dataset_id: str
    owner_team: str
    domain: str
    tier: str  # raw | bronze | silver | gold | bi
    is_executive_critical: bool
    sla_freshness_max_mins: int
    expected_row_count_range: tuple[int, int]
    max_null_rate_pct: float
    expected_mean_amount: float
    schema_columns: dict[str, str]


@dataclass
class DatasetLiveMetrics:
    """Telemetria atual de saúde e qualidade de um dataset na malha."""

    dataset_id: str
    status: str = "HEALTHY"  # HEALTHY | ANOMALOUS_ROOT | CONTAMINATED_DOWNSTREAM
    freshness_delay_mins: int = 12
    row_count: int = 10_000
    null_rate_pct: float = 0.2
    mean_amount_brl: float = 1250.0
    active_schema: dict[str, str] = field(default_factory=dict)
    delta_version: int = 42
    anomaly_signals: list[str] = field(default_factory=list)


def build_corporate_contracts() -> dict[str, DataContractSpec]:
    """Define os 12 contratos de dados dos processos corporativos e financeiros do NovaBank."""
    specs = [
        DataContractSpec(
            dataset_id="raw.erp_faturamento",
            owner_team="eng-dados-receita",
            domain="FINANCEIRO_ERP",
            tier="raw",
            is_executive_critical=False,
            sla_freshness_max_mins=60,
            expected_row_count_range=(8000, 15000),
            max_null_rate_pct=1.0,
            expected_mean_amount=1400.0,
            schema_columns={"id_fatura": "STRING", "dt_competencia": "DATE", "valor_bruto": "DECIMAL", "cliente_id": "STRING"},
        ),
        DataContractSpec(
            dataset_id="raw.erp_contas_pagar",
            owner_team="eng-dados-despesas",
            domain="FINANCEIRO_ERP",
            tier="raw",
            is_executive_critical=False,
            sla_freshness_max_mins=60,
            expected_row_count_range=(2000, 5000),
            max_null_rate_pct=1.5,
            expected_mean_amount=3200.0,
            schema_columns={"id_titulo": "STRING", "fornecedor_cnpj": "STRING", "valor_liquido": "DECIMAL", "centro_custo": "STRING"},
        ),
        DataContractSpec(
            dataset_id="raw.rh_folha_pagamento",
            owner_team="eng-dados-pessoas",
            domain="RECURSOS_HUMANOS",
            tier="raw",
            is_executive_critical=False,
            sla_freshness_max_mins=120,
            expected_row_count_range=(1500, 2500),
            max_null_rate_pct=0.5,
            expected_mean_amount=8500.0,
            schema_columns={"matricula": "STRING", "competencia": "STRING", "salario_total": "DECIMAL", "departamento": "STRING"},
        ),
        DataContractSpec(
            dataset_id="raw.tesouraria_liquidez",
            owner_team="eng-dados-tesouraria",
            domain="TESOURARIA_BACEN",
            tier="raw",
            is_executive_critical=True,
            sla_freshness_max_mins=30,
            expected_row_count_range=(500, 1500),
            max_null_rate_pct=0.2,
            expected_mean_amount=50000.0,
            schema_columns={"id_reserva": "STRING", "timestamp_posicao": "TIMESTAMP", "saldo_disponivel": "DECIMAL"},
        ),
        DataContractSpec(
            dataset_id="bronze.faturamento_diario",
            owner_team="eng-dados-receita",
            domain="FINANCEIRO_ERP",
            tier="bronze",
            is_executive_critical=False,
            sla_freshness_max_mins=75,
            expected_row_count_range=(8000, 15000),
            max_null_rate_pct=1.0,
            expected_mean_amount=1400.0,
            schema_columns={"id_fatura": "STRING", "dt_competencia": "DATE", "valor_bruto": "DECIMAL", "cliente_id": "STRING"},
        ),
        DataContractSpec(
            dataset_id="bronze.despesas_operacionais",
            owner_team="eng-dados-despesas",
            domain="FINANCEIRO_ERP",
            tier="bronze",
            is_executive_critical=False,
            sla_freshness_max_mins=75,
            expected_row_count_range=(2000, 5000),
            max_null_rate_pct=1.5,
            expected_mean_amount=3200.0,
            schema_columns={"id_titulo": "STRING", "fornecedor_cnpj": "STRING", "valor_liquido": "DECIMAL", "centro_custo": "STRING"},
        ),
        DataContractSpec(
            dataset_id="bronze.folha_consolidada",
            owner_team="eng-dados-pessoas",
            domain="RECURSOS_HUMANOS",
            tier="bronze",
            is_executive_critical=False,
            sla_freshness_max_mins=150,
            expected_row_count_range=(1500, 2500),
            max_null_rate_pct=0.5,
            expected_mean_amount=8500.0,
            schema_columns={"matricula": "STRING", "competencia": "STRING", "salario_total": "DECIMAL", "departamento": "STRING"},
        ),
        DataContractSpec(
            dataset_id="silver.lancamentos_contabeis",
            owner_team="plataforma-contabil",
            domain="CONTROLADORIA",
            tier="silver",
            is_executive_critical=True,
            sla_freshness_max_mins=90,
            expected_row_count_range=(11500, 22500),
            max_null_rate_pct=1.0,
            expected_mean_amount=2800.0,
            schema_columns={"id_lancamento": "STRING", "conta_contabil": "STRING", "valor_contabil": "DECIMAL", "natureza": "STRING"},
        ),
        DataContractSpec(
            dataset_id="silver.posicao_caixa_diaria",
            owner_team="plataforma-tesouraria",
            domain="TESOURARIA_BACEN",
            tier="silver",
            is_executive_critical=True,
            sla_freshness_max_mins=45,
            expected_row_count_range=(12000, 24000),
            max_null_rate_pct=0.5,
            expected_mean_amount=15000.0,
            schema_columns={"dt_referencia": "DATE", "saldo_caixa": "DECIMAL", "indice_basileia": "DECIMAL"},
        ),
        DataContractSpec(
            dataset_id="gold.dre_corporativa",
            owner_team="analytics-cfo",
            domain="DIRETORIA_FINANCEIRA",
            tier="gold",
            is_executive_critical=True,
            sla_freshness_max_mins=120,
            expected_row_count_range=(100, 500),
            max_null_rate_pct=0.0,
            expected_mean_amount=250000.0,
            schema_columns={"linha_dre": "STRING", "competencia": "STRING", "valor_realizado": "DECIMAL", "ebitda_pct": "DECIMAL"},
        ),
        DataContractSpec(
            dataset_id="gold.fluxo_caixa_executivo",
            owner_team="analytics-cfo",
            domain="DIRETORIA_FINANCEIRA",
            tier="gold",
            is_executive_critical=True,
            sla_freshness_max_mins=60,
            expected_row_count_range=(90, 400),
            max_null_rate_pct=0.0,
            expected_mean_amount=180000.0,
            schema_columns={"dt_fluxo": "DATE", "entradas_brl": "DECIMAL", "saidas_brl": "DECIMAL", "caixa_livre_brl": "DECIMAL"},
        ),
        DataContractSpec(
            dataset_id="bi.dashboard_diretoria_cfo",
            owner_team="bi-executivo",
            domain="DIRETORIA_FINANCEIRA",
            tier="bi",
            is_executive_critical=True,
            sla_freshness_max_mins=120,
            expected_row_count_range=(20, 100),
            max_null_rate_pct=0.0,
            expected_mean_amount=250000.0,
            schema_columns={"kpi_nome": "STRING", "valor_atual": "DECIMAL", "variacao_mom_pct": "DECIMAL"},
        ),
    ]
    return {s.dataset_id: s for s in specs}


LINEAGE_EDGES: tuple[tuple[str, str], ...] = (
    ("raw.erp_faturamento", "bronze.faturamento_diario"),
    ("raw.erp_contas_pagar", "bronze.despesas_operacionais"),
    ("raw.rh_folha_pagamento", "bronze.folha_consolidada"),
    ("bronze.faturamento_diario", "silver.lancamentos_contabeis"),
    ("bronze.despesas_operacionais", "silver.lancamentos_contabeis"),
    ("bronze.folha_consolidada", "silver.lancamentos_contabeis"),
    ("silver.lancamentos_contabeis", "gold.dre_corporativa"),
    ("silver.lancamentos_contabeis", "silver.posicao_caixa_diaria"),
    ("raw.tesouraria_liquidez", "silver.posicao_caixa_diaria"),
    ("silver.posicao_caixa_diaria", "gold.fluxo_caixa_executivo"),
    ("gold.dre_corporativa", "bi.dashboard_diretoria_cfo"),
    ("gold.fluxo_caixa_executivo", "bi.dashboard_diretoria_cfo"),
)


class CorporateLineageGraph:
    """Grafo direcionado de linhagem corporativa com cálculo de Blast Radius e Upstream RCA."""

    def __init__(self) -> None:
        self.contracts = build_corporate_contracts()
        self.downstream_adj: dict[str, list[str]] = {k: [] for k in self.contracts}
        self.upstream_adj: dict[str, list[str]] = {k: [] for k in self.contracts}
        for src, dst in LINEAGE_EDGES:
            self.downstream_adj[src].append(dst)
            self.upstream_adj[dst].append(src)

        self.live_metrics: dict[str, DatasetLiveMetrics] = {}
        self.reset_all_healthy()

    def reset_all_healthy(self) -> None:
        for ds_id, spec in self.contracts.items():
            mid_rows = (spec.expected_row_count_range[0] + spec.expected_row_count_range[1]) // 2
            self.live_metrics[ds_id] = DatasetLiveMetrics(
                dataset_id=ds_id,
                status="HEALTHY",
                freshness_delay_mins=max(5, spec.sla_freshness_max_mins // 3),
                row_count=mid_rows,
                null_rate_pct=round(spec.max_null_rate_pct * 0.25, 2),
                mean_amount_brl=spec.expected_mean_amount,
                active_schema=dict(spec.schema_columns),
                delta_version=42,
                anomaly_signals=[],
            )

    def get_downstream_blast_radius(self, dataset_id: str) -> list[str]:
        """Retorna todos os datasets e dashboards a jusante impactados por `dataset_id` (BFS)."""
        visited: list[str] = []
        queue: deque[str] = deque(self.downstream_adj.get(dataset_id, []))
        seen: set[str] = set(queue)
        while queue:
            curr = queue.popleft()
            visited.append(curr)
            for nxt in self.downstream_adj.get(curr, []):
                if nxt not in seen:
                    seen.add(nxt)
                    queue.append(nxt)
        return visited

    def get_upstream_ancestors(self, dataset_id: str) -> list[str]:
        """Retorna todos os nós ancestrais a montante de `dataset_id`."""
        visited: list[str] = []
        queue: deque[str] = deque(self.upstream_adj.get(dataset_id, []))
        seen: set[str] = set(queue)
        while queue:
            curr = queue.popleft()
            visited.append(curr)
            for prv in self.upstream_adj.get(curr, []):
                if prv not in seen:
                    seen.add(prv)
                    queue.append(prv)
        return visited

    def check_contract_breaking_change(
        self, dataset_id: str, proposed_schema: dict[str, str]
    ) -> dict[str, Any]:
        """Valida em CI/CD se uma alteração de schema viola o Data Contract e lista o Blast Radius."""
        if dataset_id not in self.contracts:
            raise KeyError(f"Dataset desconhecido: {dataset_id}")
        contract = self.contracts[dataset_id]
        expected = contract.schema_columns

        removed_cols = [col for col in expected if col not in proposed_schema]
        type_changes = [
            {"column": col, "expected_type": expected[col], "proposed_type": proposed_schema[col]}
            for col in expected
            if col in proposed_schema and proposed_schema[col].upper() != expected[col].upper()
        ]
        blast_radius = self.get_downstream_blast_radius(dataset_id)
        is_breaking = bool(removed_cols or type_changes)

        return {
            "dataset_id": dataset_id,
            "owner_team": contract.owner_team,
            "is_breaking_change": is_breaking,
            "removed_columns": removed_cols,
            "incompatible_type_changes": type_changes,
            "downstream_blast_radius": blast_radius,
            "executive_dashboards_impacted": [
                d for d in blast_radius if self.contracts[d].is_executive_critical
            ],
            "ci_gate_decision": "BLOCK_PR" if is_breaking else "ALLOW_PR",
        }

    def snapshot(self) -> dict[str, Any]:
        return {
            "nodes": [
                {
                    **asdict(self.live_metrics[ds_id]),
                    "owner_team": spec.owner_team,
                    "domain": spec.domain,
                    "tier": spec.tier,
                    "is_executive_critical": spec.is_executive_critical,
                }
                for ds_id, spec in self.contracts.items()
            ],
            "edges": [{"from": u, "to": v} for u, v in LINEAGE_EDGES],
        }

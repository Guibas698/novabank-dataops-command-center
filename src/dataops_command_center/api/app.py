"""FastAPI DataOps Command Center API & Live Lineage Dashboard."""

from __future__ import annotations

from dataclasses import asdict
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from dataops_command_center.chaos_and_rca import CHAOS_SCENARIOS, DataOpsCommandCenter


class ChaosInjectRequest(BaseModel):
    scenario_key: str = Field(
        default="CENTS_TO_REAIS_SCALE_BUG",
        description=f"Cenário de falha: {list(CHAOS_SCENARIOS.keys())}",
    )


class ContractCheckRequest(BaseModel):
    dataset_id: str = Field(default="raw.erp_contas_pagar")
    proposed_schema: dict[str, str] = Field(
        default={"id_titulo": "STRING", "fornecedor_cnpj": "STRING", "vlr_liq_v2": "DECIMAL"},
        description="Schema proposto no Pull Request",
    )


def create_app(center: DataOpsCommandCenter | None = None) -> FastAPI:
    cmd = center or DataOpsCommandCenter()

    app = FastAPI(
        title="NovaBank DataOps Command Center API",
        description=(
            "Plataforma de Observabilidade de Dados, Data Contracts (ODCS), Data Chaos Monkey, "
            "Root Cause Analysis (RCA) por Linhagem e Auto-Remediação (Self-Healing)."
        ),
        version="1.0.0",
    )

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "healthy", "service": "novabank-dataops-command-center"}

    @app.get("/api/v1/topology")
    def get_topology() -> dict[str, Any]:
        return cmd.graph.snapshot()

    @app.get("/api/v1/sre-kpis")
    def get_sre_kpis() -> dict[str, Any]:
        return cmd.sre_kpis()

    @app.get("/api/v1/chaos/scenarios")
    def list_chaos_scenarios() -> dict[str, Any]:
        return CHAOS_SCENARIOS

    @app.post("/api/v1/chaos/inject")
    def inject_chaos_scenario(req: ChaosInjectRequest) -> dict[str, Any]:
        try:
            return cmd.inject_chaos(req.scenario_key)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/incidents")
    def list_incidents() -> list[dict[str, Any]]:
        return [asdict(i) for i in cmd.incidents.values()]

    @app.post("/api/v1/incidents/{incident_id}/remediate")
    def remediate_incident(incident_id: str) -> dict[str, Any]:
        try:
            return cmd.auto_remediate_incident(incident_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.post("/api/v1/contracts/check-pr")
    def check_contract_pr(req: ContractCheckRequest) -> dict[str, Any]:
        try:
            return cmd.graph.check_contract_breaking_change(req.dataset_id, req.proposed_schema)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get("/dashboard", response_class=HTMLResponse)
    def dashboard() -> str:
        k = cmd.sre_kpis()
        return f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="utf-8"/>
<title>NovaBank DataOps Command Center</title>
<style>
  body {{ background:#090d16; color:#e2e8f0; font-family:Inter,system-ui,sans-serif; margin:0; padding:32px; }}
  h1 {{ margin:0 0 8px; font-size:28px; color:#10b981; }}
  .sub {{ color:#94a3b8; margin-bottom:24px; }}
  .grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(220px,1fr)); gap:16px; margin-bottom:28px; }}
  .card {{ background:#1e293b; border:1px solid #334155; border-radius:12px; padding:18px; }}
  .val {{ font-size:26px; font-weight:700; color:#f8fafc; }}
  .lbl {{ font-size:13px; color:#94a3b8; margin-top:4px; }}
</style>
</head>
<body>
  <h1>🐒 NovaBank DataOps Command Center</h1>
  <div class="sub">12 Pipelines Corporativos (DRE / ERP / Tesouraria) • Data Contracts ODCS • Chaos Monkey • Lineage RCA • Self-Healing</div>
  <div class="grid">
    <div class="card"><div class="val">{k['healthy_datasets_count']}/{k['total_datasets_monitored']}</div><div class="lbl">Datasets Saudáveis na Malha</div></div>
    <div class="card"><div class="val">{k['data_health_score_pct']}%</div><div class="lbl">Data Health Score</div></div>
    <div class="card"><div class="val">{k['open_incidents']}</div><div class="lbl">Incidentes Abertos</div></div>
    <div class="card"><div class="val">{k['resolved_incidents']}</div><div class="lbl">Incidentes Auto-Remediados</div></div>
    <div class="card"><div class="val">{k['mttd_seconds']}s</div><div class="lbl">MTTD (Tempo Médio de Detecção)</div></div>
    <div class="card"><div class="val">{k['mttr_seconds']}s</div><div class="lbl">MTTR (Tempo Médio de Resolução)</div></div>
  </div>
  <p>Injete falhas do Chaos Monkey e execute Self-Healing em <a style="color:#10b981" href="/docs">/docs</a>.</p>
</body>
</html>"""

    return app


app = create_app()

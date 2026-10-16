"""Testes ponta a ponta do DataOps Command Center.

Valida:
1. Topologia de 12 datasets corporativos (ERP, Folha, Tesouraria, DRE e Dashboard CFO)
2. Gate de CI/CD de Data Contracts (bloqueio de PR com Breaking Change e cálculo de Blast Radius)
3. Injeção dos 6 cenários do Data Chaos Monkey
4. Algoritmo de Root Cause Analysis (RCA) por travessia topológica na linhagem
5. Auto-Remediação (Self-Healing), geração de Postmortem em Markdown e restauração de 100% da saúde
6. Endpoints da API FastAPI
"""

from __future__ import annotations

from fastapi.testclient import TestClient

from dataops_command_center.api.app import create_app
from dataops_command_center.chaos_and_rca import CHAOS_SCENARIOS, DataOpsCommandCenter


def test_data_contract_breaking_change_gate() -> None:
    cmd = DataOpsCommandCenter()
    res = cmd.graph.check_contract_breaking_change(
        "raw.erp_faturamento",
        {"id_fatura": "STRING", "dt_competencia": "DATE", "cliente_id": "STRING"},  # removeu valor_bruto
    )
    assert res["is_breaking_change"] is True
    assert "valor_bruto" in res["removed_columns"]
    assert res["ci_gate_decision"] == "BLOCK_PR"
    assert "gold.dre_corporativa" in res["downstream_blast_radius"]
    assert "bi.dashboard_diretoria_cfo" in res["executive_dashboards_impacted"]


def test_all_chaos_scenarios_rca_and_self_healing() -> None:
    cmd = DataOpsCommandCenter()

    for scenario_key, meta in CHAOS_SCENARIOS.items():
        inc = cmd.inject_chaos(scenario_key)
        assert inc["status"] == "OPEN"
        # O algoritmo de RCA por linhagem deve apontar exatamente o dataset origem da falha!
        assert inc["root_cause_dataset"] == meta["target_dataset"]
        assert len(inc["blast_radius_datasets"]) >= 1
        assert inc["severity"] == "SEV-1"
        assert "bi.dashboard_diretoria_cfo" in inc["executive_assets_impacted"]

        # Executa Auto-Remediação (Self-Healing)
        resolved = cmd.auto_remediate_incident(inc["incident_id"])
        assert resolved["status"] == "RESOLVED"
        assert resolved["postmortem_markdown"] is not None
        assert inc["incident_id"] in resolved["postmortem_markdown"]

        # Garante que 100% dos 12 datasets voltaram ao estado HEALTHY
        kpis = cmd.sre_kpis()
        assert kpis["healthy_datasets_count"] == 12
        assert kpis["data_health_score_pct"] == 100.0


def test_command_center_fastapi_endpoints() -> None:
    app = create_app()
    client = TestClient(app)

    assert client.get("/health").status_code == 200
    assert len(client.get("/api/v1/topology").json()["nodes"]) == 12
    assert client.get("/dashboard").status_code == 200

    # Injeta falha de escala Centavos -> Reais via API
    inj_resp = client.post(
        "/api/v1/chaos/inject",
        json={"scenario_key": "CENTS_TO_REAIS_SCALE_BUG"},
    )
    assert inj_resp.status_code == 200
    inc_data = inj_resp.json()
    inc_id = inc_data["incident_id"]
    assert inc_data["root_cause_dataset"] == "raw.erp_faturamento"

    # Verifica queda na saúde da malha durante o incidente
    kpis_during = client.get("/api/v1/sre-kpis").json()
    assert kpis_during["open_incidents"] == 1
    assert kpis_during["healthy_datasets_count"] < 12

    # Executa Self-Healing via API
    rem_resp = client.post(f"/api/v1/incidents/{inc_id}/remediate")
    assert rem_resp.status_code == 200
    assert rem_resp.json()["status"] == "RESOLVED"

    kpis_after = client.get("/api/v1/sre-kpis").json()
    assert kpis_after["open_incidents"] == 0
    assert kpis_after["healthy_datasets_count"] == 12

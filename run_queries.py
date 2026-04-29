"""
Refresca el cache de las queries de Dune disparando una ejecucion fresca de
cada una. Usado por el workflow .github/workflows/refresh_dune.yml.

NOTA: Cada corrida CONSUME CREDITOS de Dune (una ejecucion por query).
Solo correrlo cuando se necesiten datos actualizados.
"""

import os
import sys
import time

import requests


QUERY_IDS = [
    6993368,
    6993341,
    6993356,
    6963145,
    7370113,
    6993335,
    3591853,
]

DUNE_API_BASE = "https://api.dune.com/api/v1"
SUBMIT_DELAY_SECONDS = 1
POLL_INTERVAL_SECONDS = 10
MAX_WAIT_SECONDS = 30 * 60


def main() -> int:
    api_key = os.environ.get("DUNE_API_KEY")
    if not api_key:
        print("ERROR: DUNE_API_KEY no esta configurada", file=sys.stderr)
        return 1
    headers = {"X-Dune-API-Key": api_key}
    total = len(QUERY_IDS)

    print(f"[1/2] Disparando {total} ejecuciones en Dune...")
    pending: dict[int, str] = {}
    for qid in QUERY_IDS:
        try:
            r = requests.post(
                f"{DUNE_API_BASE}/query/{qid}/execute", headers=headers, timeout=30
            )
            r.raise_for_status()
            exec_id = r.json()["execution_id"]
            pending[qid] = exec_id
            print(f"  Query {qid}: enviada (exec {exec_id})")
        except Exception as exc:
            print(f"  Query {qid}: ERROR al enviar -> {exc}")
        time.sleep(SUBMIT_DELAY_SECONDS)

    if not pending:
        print("\nNinguna query se pudo enviar.")
        return 1

    print(f"\n[2/2] Esperando finalizacion ({len(pending)} en curso)...")
    start = time.time()
    failures = 0

    while pending and (time.time() - start) < MAX_WAIT_SECONDS:
        for qid in list(pending.keys()):
            exec_id = pending[qid]
            try:
                r = requests.get(
                    f"{DUNE_API_BASE}/execution/{exec_id}/status",
                    headers=headers,
                    timeout=30,
                )
                r.raise_for_status()
                state = str(r.json().get("state", "")).upper()
                elapsed = time.time() - start
                if "COMPLETED" in state:
                    print(f"  [OK]   Query {qid}  ({elapsed:6.1f}s)")
                    del pending[qid]
                elif any(x in state for x in ("FAILED", "CANCELLED", "EXPIRED")):
                    print(f"  [FAIL] Query {qid}  ({elapsed:6.1f}s)  -> {state}")
                    del pending[qid]
                    failures += 1
            except Exception as exc:
                print(f"  Query {qid}: error consultando status (reintento) -> {exc}")
            time.sleep(0.5)

        if pending:
            time.sleep(POLL_INTERVAL_SECONDS)

    for qid in pending:
        print(f"  [TIMEOUT] Query {qid}")
        failures += 1

    elapsed_total = time.time() - start
    ok = total - failures - (total - len(QUERY_IDS))
    print(f"\nListo en {elapsed_total:.1f}s  ({total - failures}/{total} exitosas)")
    return 0 if failures == 0 else 2


if __name__ == "__main__":
    sys.exit(main())

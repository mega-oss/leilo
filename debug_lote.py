#!/usr/bin/env python3
"""
debug_json.py — Testa payloads na busca-elastic e salva o JSON bruto.

Uso:
    python debug_json.py              # testa payload sem filtro (pega o que vier)
    python debug_json.py --raw        # salva raw.json com os primeiros 3 itens completos
    python debug_json.py --tipos      # lista todos os valores únicos de "tipo" nos resultados
    python debug_json.py --campos     # lista todas as keys de veiculo.* nos resultados
"""

import asyncio
import httpx
import json
import argparse
from playwright.async_api import async_playwright

API_URL  = "https://api.leilo.com.br/v1/lote/busca-elastic"
BASE_URL = "https://leilo.com.br/leilao/carros"

HEADERS = {
    "accept":          "application/json, text/plain, */*",
    "accept-language": "pt-BR,pt;q=0.9",
    "content-type":    "application/json",
    "origin":          "https://leilo.com.br",
    "referer":         "https://leilo.com.br/",
    "user-agent": (
        "Mozilla/5.0 (iPhone; CPU iPhone OS 18_5 like Mac OS X) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) "
        "Version/18.5 Mobile/15E148 Safari/604.1"
    ),
}

# Payloads a testar em sequência até achar carros
PAYLOADS_TESTE = [
    {
        "nome": "Sem filtro (size=5)",
        "payload": {
            "from": 0, "size": 5,
            "listaOrdenacao": [{"campo": "dataFim", "tipoCampo": "long", "tipoOrdenacao": "asc"}]
        }
    },
    {
        "nome": "Filtro tipo=Veículos (campo raiz)",
        "payload": {
            "from": 0, "size": 5,
            "requisicoesBusca": [
                {"tipo": "exata", "campo": "tipo", "label": "Tipo", "valor": "Veículos"}
            ],
            "listaOrdenacao": [{"campo": "dataFim", "tipoCampo": "long", "tipoOrdenacao": "asc"}]
        }
    },
    {
        "nome": "Filtro tipo=Veiculo (sem acento)",
        "payload": {
            "from": 0, "size": 5,
            "requisicoesBusca": [
                {"tipo": "exata", "campo": "tipo", "label": "Tipo", "valor": "Veiculo"}
            ],
            "listaOrdenacao": [{"campo": "dataFim", "tipoCampo": "long", "tipoOrdenacao": "asc"}]
        }
    },
    {
        "nome": "Filtro tipo=VEICULO (maiúsculo)",
        "payload": {
            "from": 0, "size": 5,
            "requisicoesBusca": [
                {"tipo": "exata", "campo": "tipo", "label": "Tipo", "valor": "VEICULO"}
            ],
            "listaOrdenacao": [{"campo": "dataFim", "tipoCampo": "long", "tipoOrdenacao": "asc"}]
        }
    },
    {
        "nome": "Filtro veiculo.categoria=CARRO (original)",
        "payload": {
            "from": 0, "size": 5,
            "requisicoesBusca": [
                {"tipo": "exata", "campo": "veiculo.categoria", "label": "Categoria", "valor": "CARRO"}
            ],
            "listaOrdenacao": [{"campo": "dataFim", "tipoCampo": "long", "tipoOrdenacao": "asc"}]
        }
    },
    {
        "nome": "Filtro anoModelo intervalo 2018-2026 (sem categoria)",
        "payload": {
            "from": 0, "size": 5,
            "requisicoesBusca": [
                {"tipo": "intervalo", "campo": "veiculo.anoModelo", "label": "Ano",
                 "valorMinimo": "2018", "valorMaximo": "2026"},
            ],
            "listaOrdenacao": [{"campo": "dataFim", "tipoCampo": "long", "tipoOrdenacao": "asc"}]
        }
    },
    {
        "nome": "Filtro km intervalo 0-200000 (sem categoria)",
        "payload": {
            "from": 0, "size": 5,
            "requisicoesBusca": [
                {"tipo": "intervalo", "campo": "veiculo.km", "label": "KM",
                 "valorMinimo": "0", "valorMaximo": "200000"},
            ],
            "listaOrdenacao": [{"campo": "dataFim", "tipoCampo": "long", "tipoOrdenacao": "asc"}]
        }
    },
]


async def get_cookies(show: bool) -> dict:
    print("  Abrindo browser pra pegar cookies...")
    cookies = {}
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=not show,
            args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
        )
        ctx = await browser.new_context(
            user_agent=HEADERS["user-agent"],
            viewport={"width": 390, "height": 844},
            locale="pt-BR",
            bypass_csp=True,
        )
        await ctx.add_init_script(
            "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"
        )
        page = await ctx.new_page()
        try:
            await page.goto(BASE_URL, wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(3)
        except Exception as e:
            print(f"  browser erro: {e}")
        raw = await ctx.cookies()
        cookies = {c["name"]: c["value"] for c in raw}
        await browser.close()
    print(f"  ✓ {len(cookies)} cookies: {list(cookies.keys())}\n")
    return cookies


async def bater(client: httpx.AsyncClient, nome: str, payload: dict) -> list:
    print(f"  ▶ {nome}")
    try:
        resp = await client.post(API_URL, json=payload, timeout=20)
        print(f"    status={resp.status_code}  bytes={len(resp.content)}")
        if resp.status_code not in (200, 201):
            print(f"    ❌ HTTP {resp.status_code}: {resp.text[:200]}")
            return []
        data = resp.json()
        items = data if isinstance(data, list) else []
        if isinstance(data, dict):
            for k in ["lotes", "items", "results", "data", "content", "hits"]:
                if isinstance(data.get(k), list):
                    items = data[k]
                    break
            if not items:
                for k, v in data.items():
                    if isinstance(v, list):
                        items = v
                        break
        print(f"    itens={len(items)}")
        if items:
            tipos = {}
            for it in items:
                t = it.get("tipo", "?")
                tipos[t] = tipos.get(t, 0) + 1
            print(f"    tipos: {tipos}")
        return items
    except Exception as e:
        print(f"    ❌ erro: {e}")
        return []


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--show",   action="store_true")
    parser.add_argument("--raw",    action="store_true", help="Salva raw.json com 3 itens do melhor payload")
    parser.add_argument("--tipos",  action="store_true", help="Lista valores únicos de 'tipo'")
    parser.add_argument("--campos", action="store_true", help="Lista keys de veiculo.* e valor.*")
    args = parser.parse_args()

    cookies = await get_cookies(args.show)

    print("=" * 60)
    print("TESTANDO PAYLOADS")
    print("=" * 60)

    best_items = []
    best_nome  = ""

    async with httpx.AsyncClient(headers=HEADERS, cookies=cookies, follow_redirects=True) as client:
        for t in PAYLOADS_TESTE:
            items = await bater(client, t["nome"], t["payload"])
            if len(items) > len(best_items):
                best_items = items
                best_nome  = t["nome"]
            print()

    print("=" * 60)
    print(f"MELHOR PAYLOAD: {best_nome}  ({len(best_items)} itens)")
    print("=" * 60)

    if not best_items:
        print("❌ Nenhum payload retornou itens!")
        return

    if args.tipos:
        print("\nVALORES ÚNICOS DE 'tipo':")
        tipos = {}
        for it in best_items:
            t = it.get("tipo", "?")
            tipos[t] = tipos.get(t, 0) + 1
        for k, v in sorted(tipos.items(), key=lambda x: -x[1]):
            print(f"  '{k}': {v}x")

    if args.campos:
        print("\nKEYS DE veiculo.*:")
        vkeys = set()
        for it in best_items:
            vkeys.update((it.get("veiculo") or {}).keys())
        for k in sorted(vkeys):
            vals = [str((it.get("veiculo") or {}).get(k, ""))[:30] for it in best_items[:3]]
            print(f"  {k}: {vals}")

        print("\nKEYS DE valor.*:")
        valkeys = set()
        for it in best_items:
            valkeys.update((it.get("valor") or {}).keys())
        for k in sorted(valkeys):
            vals = [str((it.get("valor") or {}).get(k, ""))[:30] for it in best_items[:3]]
            print(f"  {k}: {vals}")

    if args.raw:
        with open("raw.json", "w", encoding="utf-8") as f:
            json.dump(best_items[:3], f, ensure_ascii=False, indent=2)
        print("\n✓ Salvo em raw.json (primeiros 3 itens)")

    # Resumo do primeiro item
    print("\nPRIMEIRO ITEM:")
    it = best_items[0]
    print(f"  nome:  {it.get('nome')}")
    print(f"  tipo:  {it.get('tipo')}")
    print(f"  id:    {it.get('id')}")
    print(f"  lelId: {it.get('lelId')}")
    vei = it.get("veiculo") or {}
    print(f"  veiculo.categoria:    {vei.get('categoria', '—')}")
    print(f"  veiculo.valorMercado: {vei.get('valorMercado')}")
    print(f"  veiculo.anoModelo:    {vei.get('anoModelo')}")
    print(f"  veiculo.infocarMarca: {vei.get('infocarMarca')}")
    val = it.get("valor") or {}
    lance = val.get("lance") or {}
    print(f"  valor.lance.valor:    {lance.get('valor')}")
    print(f"  fotosUrls:            {len(it.get('fotosUrls') or [])} fotos")


if __name__ == "__main__":
    asyncio.run(main())
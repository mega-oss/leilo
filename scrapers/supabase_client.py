#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SUPABASE CLIENT — auctions.veiculos
✅ Schema: auctions / tabela: veiculos
✅ Conflict key: link (URL única do lote)
✅ Normaliza chaves antes de enviar (fix PGRST102)
✅ Remove duplicatas DENTRO do batch (fix PGRST21000)
✅ Heartbeat com logs estruturados
"""

import os
import time
import requests
from datetime import datetime
from typing import List, Dict, Optional


class SupabaseClient:
    """Cliente Supabase — schema auctions, tabela veiculos"""

    def __init__(self, service_name: str = None, service_type: str = 'scraper'):
        self.url = os.getenv('SUPABASE_URL')
        self.key = os.getenv('SUPABASE_SERVICE_ROLE_KEY')

        if not self.url or not self.key:
            raise ValueError("⚠️  Configure SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY")

        self.url = self.url.rstrip('/')

        # Headers padrão — schema auctions
        self.headers = {
            'apikey': self.key,
            'Authorization': f'Bearer {self.key}',
            'Content-Type': 'application/json',
            'Content-Profile': 'auctions',
            'Accept-Profile': 'auctions',
        }

        self.session = requests.Session()
        self.session.headers.update(self.headers)

        # Heartbeat
        self.service_name = service_name
        self.service_type = service_type
        self.heartbeat_id = None
        self.heartbeat_enabled = bool(service_name)
        self.start_time = time.time()
        self.items_processed = 0

        self.heartbeat_metrics = {
            'items_processed': 0,
            'items_inserted': 0,
            'items_updated': 0,
            'errors': 0,
            'warnings': 0,
        }

    # =========================================================================
    # HEARTBEAT
    # =========================================================================

    def heartbeat_start(self, metadata: Optional[Dict] = None) -> bool:
        if not self.heartbeat_enabled:
            return False
        try:
            url = f"{self.url}/rest/v1/infra_actions?on_conflict=service_name"
            logs = {
                'event': 'start',
                'message': 'Scraper iniciado',
                'timestamp': datetime.now().isoformat(),
                'metrics': self.heartbeat_metrics.copy(),
                'elapsed_seconds': 0,
            }
            if metadata:
                logs.update(metadata)

            payload = {
                'service_name': self.service_name,
                'service_type': self.service_type,
                'status': 'active',
                'last_activity': datetime.now().isoformat(),
                'logs': logs,
                'metadata': metadata or {},
            }

            hb_headers = {
                **self.headers,
                'Content-Profile': 'public',
                'Accept-Profile': 'public',
                'Prefer': 'resolution=merge-duplicates,return=representation',
            }

            r = self.session.post(url, json=[payload], headers=hb_headers, timeout=30)
            if r.status_code in (200, 201):
                data = r.json()
                if data:
                    self.heartbeat_id = data[0].get('id')
                    print(f"  💓 Heartbeat iniciado: {self.heartbeat_id}")
                    return True
                print(f"  ⚠️  Heartbeat: resposta vazia (status {r.status_code})")
            else:
                print(f"  ⚠️  Heartbeat: HTTP {r.status_code} — {r.text[:200]}")
        except Exception as e:
            print(f"  ⚠️  Erro ao iniciar heartbeat: {e}")
        return False

    def heartbeat_update(self, status: str = 'active',
                         custom_logs: Optional[Dict] = None,
                         error_message: Optional[str] = None) -> bool:
        if not self.heartbeat_enabled or not self.heartbeat_id:
            return False
        try:
            url = f"{self.url}/rest/v1/infra_actions?id=eq.{self.heartbeat_id}"
            elapsed = time.time() - self.start_time
            logs = {
                'event': 'progress',
                'message': f"Processados {self.heartbeat_metrics['items_processed']} itens",
                'timestamp': datetime.now().isoformat(),
                'metrics': self.heartbeat_metrics.copy(),
                'elapsed_seconds': round(elapsed, 2),
            }
            if custom_logs:
                logs.update(custom_logs)

            payload = {
                'status': status,
                'last_activity': datetime.now().isoformat(),
                'logs': logs,
            }
            if error_message:
                payload['error_message'] = error_message

            hb_headers = {
                **self.headers,
                'Content-Profile': 'public',
                'Accept-Profile': 'public',
                'Prefer': 'resolution=merge-duplicates',
            }
            r = self.session.patch(url, json=payload, headers=hb_headers, timeout=30)
            return r.status_code == 204
        except Exception:
            return False

    def heartbeat_progress(self, items_processed: int = 0,
                           custom_logs: Optional[Dict] = None) -> bool:
        self.items_processed += items_processed
        self.heartbeat_metrics['items_processed'] += items_processed
        return self.heartbeat_update(status='active', custom_logs=custom_logs)

    def heartbeat_success(self, final_stats: Optional[Dict] = None) -> bool:
        if not self.heartbeat_enabled or not self.heartbeat_id:
            return False
        elapsed = time.time() - self.start_time
        custom_logs = {
            'event': 'completed',
            'message': 'Scraper concluído com sucesso',
            'timestamp': datetime.now().isoformat(),
            'final_stats': final_stats or {},
            'total_elapsed_seconds': round(elapsed, 2),
        }
        success = self.heartbeat_update(status='active', custom_logs=custom_logs)
        if success:
            print(f"  💓 Heartbeat finalizado: {self.heartbeat_metrics['items_processed']} itens")
        return success

    def heartbeat_error(self, error_message: str) -> bool:
        return self.heartbeat_update(status='error', error_message=error_message)

    def heartbeat_finish(self, status: str = 'inactive',
                         final_stats: Optional[Dict] = None) -> bool:
        if not self.heartbeat_enabled or not self.heartbeat_id:
            return False
        custom_logs = {
            'finished_at': datetime.now().isoformat(),
            'total_items_processed': self.items_processed,
            'total_elapsed_seconds': round(time.time() - self.start_time, 2),
        }
        if final_stats:
            custom_logs['final_stats'] = final_stats
        return self.heartbeat_update(status=status, custom_logs=custom_logs)

    # =========================================================================
    # DEDUPLICAÇÃO E NORMALIZAÇÃO
    # =========================================================================

    def _deduplicate_batch(self, items: List[Dict]) -> tuple:
        """
        Remove duplicatas DENTRO do batch baseado em `link` (URL única do lote).
        Resolve PGRST21000: "cannot affect row a second time"
        """
        if not items:
            return items, 0

        seen: dict = {}
        unique: list = []
        dupes = 0

        for item in items:
            key = item.get('link')
            if not key:
                continue
            if key not in seen:
                seen[key] = True
                unique.append(item)
            else:
                dupes += 1

        return unique, dupes

    def _normalize_batch_keys(self, items: List[Dict]) -> List[Dict]:
        """
        Garante que todos os items do batch tenham as mesmas chaves.
        Resolve PGRST102: "All object keys must match"
        """
        if not items:
            return items

        all_keys: set = set()
        for item in items:
            all_keys.update(item.keys())

        return [{k: item.get(k) for k in all_keys} for item in items]

    # =========================================================================
    # UPSERT → auctions.veiculos
    # =========================================================================

    def upsert_veiculos(self, items: List[Dict]) -> Dict:
        """
        Upsert em auctions.veiculos com conflito em `link`.

        Campos esperados (mínimo obrigatório):
            titulo, tipo, ano_fabricacao, ano_modelo,
            modalidade, valor_inicial, data_encerramento, link
        """
        return self.upsert('veiculos', items)

    def upsert(self, tabela: str, items: List[Dict]) -> Dict:
        """
        Upsert genérico com deduplicação e normalização de chaves.
        Conflict key: `link`
        Timestamps gerenciados: atualizado_em (auto via trigger no DB).
        """
        if not items:
            return {'inserted': 0, 'updated': 0, 'errors': 0,
                    'total': 0, 'duplicates_removed': 0}

        # Remove campos que o DB gerencia automaticamente
        _auto_fields = {'id', 'criado_em', 'atualizado_em'}
        for item in items:
            for f in _auto_fields:
                item.pop(f, None)

        stats = {
            'inserted': 0,
            'updated': 0,
            'errors': 0,
            'total': len(items),
            'duplicates_removed': 0,
        }

        batch_size = 500
        total_batches = (len(items) + batch_size - 1) // batch_size

        # Conflict em `link` — URL do lote é a chave única
        url = f"{self.url}/rest/v1/{tabela}?on_conflict=link"

        upsert_headers = {
            **self.headers,
            'Prefer': 'resolution=merge-duplicates,return=representation',
        }

        for i in range(0, len(items), batch_size):
            batch = items[i:i + batch_size]
            batch_num = (i // batch_size) + 1

            try:
                batch_unique, batch_dupes = self._deduplicate_batch(batch)

                if batch_dupes > 0:
                    stats['duplicates_removed'] += batch_dupes
                    print(f"  🔄 Batch {batch_num}/{total_batches}: "
                          f"{batch_dupes} duplicata(s) removida(s)")

                if not batch_unique:
                    print(f"  ⚠️  Batch {batch_num}/{total_batches}: "
                          f"vazio após deduplicação")
                    continue

                normalized = self._normalize_batch_keys(batch_unique)

                r = self.session.post(
                    url,
                    json=normalized,
                    headers=upsert_headers,
                    timeout=120,
                )

                if r.status_code in (200, 201):
                    try:
                        resp_data = r.json()
                        count = len(resp_data) if isinstance(resp_data, list) \
                            else len(batch_unique)
                    except Exception:
                        count = len(batch_unique)

                    stats['inserted'] += count
                    self.heartbeat_metrics['items_inserted'] += count
                    print(f"  ✅ Batch {batch_num}/{total_batches}: "
                          f"{len(batch_unique)} itens → DB")

                    self.heartbeat_progress(
                        items_processed=len(batch_unique),
                        custom_logs={'batch': batch_num,
                                     'total_batches': total_batches},
                    )

                else:
                    err = r.text[:300] if r.text else 'Sem detalhes'
                    print(f"  ❌ Batch {batch_num}/{total_batches}: "
                          f"HTTP {r.status_code}")
                    print(f"     {err}")
                    stats['errors'] += len(batch_unique)
                    self.heartbeat_metrics['errors'] += len(batch_unique)

            except requests.exceptions.Timeout:
                print(f"  ⏱️  Batch {batch_num}/{total_batches}: Timeout (120s)")
                stats['errors'] += len(batch)

            except Exception as e:
                print(f"  ❌ Batch {batch_num}/{total_batches}: "
                      f"{type(e).__name__}: {str(e)[:200]}")
                stats['errors'] += len(batch)

            if batch_num < total_batches:
                time.sleep(0.5)

        return stats

    # =========================================================================
    # AUXILIARES
    # =========================================================================

    def test(self) -> bool:
        """Testa conexão com Supabase."""
        try:
            r = self.session.get(f"{self.url}/rest/v1/", timeout=10)
            if r.status_code == 200:
                print("✅ Conexão com Supabase OK")
                return True
            print(f"❌ HTTP {r.status_code}: {r.text[:200]}")
            return False
        except Exception as e:
            print(f"❌ Erro: {e}")
            return False

    def get_stats(self, tabela: str = 'veiculos') -> Dict:
        """Retorna estatísticas da tabela (total e ativos)."""
        try:
            url = f"{self.url}/rest/v1/{tabela}"
            r = self.session.get(
                url,
                params={'select': 'count'},
                headers={**self.headers, 'Prefer': 'count=exact'},
                timeout=30,
            )
            if r.status_code == 200:
                total = int(r.headers.get('Content-Range', '0/0').split('/')[-1])

                r_a = self.session.get(
                    url,
                    params={'select': 'count', 'ativo': 'eq.true'},
                    headers={**self.headers, 'Prefer': 'count=exact'},
                    timeout=30,
                )
                active = 0
                if r_a.status_code == 200:
                    active = int(r_a.headers.get('Content-Range', '0/0')
                                 .split('/')[-1])

                return {'total': total, 'active': active,
                        'inactive': total - active, 'table': tabela}
        except Exception as e:
            print(f"  ⚠️  Erro ao buscar stats: {e}")
        return {'total': 0, 'active': 0, 'inactive': 0, 'table': tabela}

    def __del__(self):
        if hasattr(self, 'session'):
            self.session.close()
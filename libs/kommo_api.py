import os
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import logging
import pytz
from dateutil import parser
from .file_logger import sync_file_logger

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    force=True)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

sao_paulo_tz = pytz.timezone('America/Sao_Paulo')


def parse_datetime_sp(value):
    if not value:
        return None

    if isinstance(value, (int, float)):
        dt = datetime.fromtimestamp(value, tz=sao_paulo_tz)
    elif isinstance(value, str):
        dt = parser.parse(value)
        if dt.tzinfo is None:
            dt = sao_paulo_tz.localize(dt)
        else:
            dt = dt.astimezone(sao_paulo_tz)
    elif isinstance(value, datetime):
        dt = value if value.tzinfo else sao_paulo_tz.localize(value)
        dt = dt.astimezone(sao_paulo_tz)
    else:
        return None

    return dt


class KommoAPI:

    def set_date_range(self, start_date, end_date):
        """Set date range for API queries"""
        self.start_date = start_date
        self.end_date = end_date
        if isinstance(start_date, datetime):
            self.api_config['sync_start_date'] = int(start_date.timestamp())
        if isinstance(end_date, datetime):
            self.api_config['sync_end_date'] = int(end_date.timestamp())

    def __init__(self,
                 api_url=None,
                 access_token=None,
                 api_config=None,
                 supabase_client=None):
        try:
            logger.info("Initializing KommoAPI")

            from .rate_limit_monitor import RateLimitMonitor
            self.rate_monitor = RateLimitMonitor()

            self.api_config = api_config
            self.supabase_client = supabase_client
            self.api_url = api_url or (api_config.get('api_url') if api_config
                                       else None) or os.getenv("KOMMO_API_URL")
            self.access_token = access_token or (
                api_config.get('access_token')
                if api_config else None) or os.getenv("ACCESS_TOKEN_KOMMO")
            self.start_date = None
            self.end_date = None

            if not self.api_url or not self.access_token:
                raise ValueError("API URL and access token must be provided")

            if self.api_url.endswith('/'):
                self.api_url = self.api_url[:-1]

            logger.info("KommoAPI initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing KommoAPI: {str(e)}")
            raise

    def _make_request(self,
                      endpoint,
                      method="GET",
                      params=None,
                      data=None,
                      retry_count=3):
        """
        Make a request to the Kommo API with retry logic and rate limiting
        """
        url = f"{self.api_url}/{endpoint}"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }

        for attempt in range(retry_count):
            try:
                # Aplica rate limiting de 7 req/s conforme documentação Kommo
                self.rate_monitor.enforce_rate_limit()

                logger.info(f"Making API request to: {url}")
                response = requests.request(method=method,
                                            url=url,
                                            headers=headers,
                                            params=params,
                                            json=data)

                if response.status_code == 204:
                    logger.info(
                        f"Received 204 No Content from {url} - no more data available."
                    )
                    return None  # Explicitly return None for 204 responses

                response.raise_for_status()

                # Check if response content is empty
                if not response.text.strip():
                    logger.warning("Empty response received from API")
                    return {}

                return response.json()

            except requests.exceptions.RequestException as e:
                status_code = e.response.status_code if hasattr(
                    e, 'response') else 0

                # Usa o novo handler de erros específicos da Kommo
                if status_code in (429, 403, 504):
                    company_id = getattr(self, 'api_config',
                                         {}).get('company_id', 'unknown')
                    sync_file_logger.log_api_error(company_id, endpoint,
                                                   status_code, attempt)

                    if not self.rate_monitor.handle_kommo_error(
                            status_code, endpoint, attempt):
                        logger.error(
                            f"Stopping retries for {endpoint} due to {status_code}"
                        )
                        raise
                    self.rate_monitor.wait_before_retry(endpoint, attempt)
                else:
                    logger.warning(
                        f"API request failed (attempt {attempt+1}/{retry_count}): {str(e)}"
                    )
                    if attempt >= retry_count - 1:
                        raise
                    time.sleep(2)  # Default delay for non-rate-limit errors

    def get_users(self, active_only=True):
        """
        Retrieve all users (brokers) from Kommo CRM

        Args:
            active_only (bool): If True, only return active users
        """
        try:
            logger.info("Retrieving ALL users from Kommo CRM")

            # Get safe pagination limits
            limits = self._get_safe_pagination_limits()

            users_data = []
            page = 1
            max_user_pages = 20  # Users são geralmente poucos, limite menor

            while page <= max_user_pages:
                try:
                    response = self._make_request("users",
                                                  params={
                                                      "page": page,
                                                      "limit":
                                                      limits['page_size']
                                                  })

                    # If response is None (204 No Content), no more data available
                    if response is None:
                        logger.info(
                            f"Página {page}: API retornou 204 - não há mais usuários - finalizando busca"
                        )
                        break

                    if response is not None:
                        users = response["_embedded"]["users"]
                        users_data.extend(users)

                        logger.info(
                            f"Página {page}: {len(users)} usuários encontrados (total: {len(users_data)})"
                        )

                    page += 1
                    time.sleep(limits['delay_between_pages'])

                except Exception as e:
                    logger.error(
                        f"Erro ao buscar usuários página {page}: {str(e)}")
                    break

            logger.info(f"Total de usuários encontrados: {len(users_data)}")

            # Process users data into a more usable format
            processed_users = []
            for user in users_data:
                # Skip inactive users if active_only is True
                is_active = user.get("rights", {}).get("is_active", False)
                if active_only and not is_active:
                    continue

                processed_users.append({
                    "id":
                    user.get("id"),
                    "nome":
                    f"{user.get('name', '')} {user.get('lastname', '')}".strip(
                    ),
                    "email":
                    user.get("email"),
                    "foto_url":
                    user.get("_links", {}).get("avatar", {}).get("href"),
                    "criado_em": (parse_datetime_sp(user.get("created_at"))
                                  if user.get("created_at") else None),
                    "cargo":
                    user.get("rights", {}).get("is_admin") and "Administrador"
                    or "Corretor"
                })

            logger.info(
                f"Usuários processados (ativos): {len(processed_users)}")
            return pd.DataFrame(processed_users)

        except Exception as e:
            logger.error(f"Failed to retrieve users: {str(e)}")
            raise

    def _get_safe_pagination_limits(self):
        """Define limites seguros para paginação respeitando 7 req/s da API Kommo"""
        return {
            'max_pages_per_request':
            500,  # Aumentado para sincronização completa
            'max_total_records':
            50000,  # Aumentado para capturar todos os dados
            'page_size': 250,  # Mantido para estabilidade
            'delay_between_pages':
            0.0  # Removido - rate limiting é feito no _make_request
        }

    def _save_last_sync_record(self, company_id, last_activity_created_at):
        """Save the timestamp of the last synchronized activity"""
        try:
            if hasattr(
                    self,
                    'supabase_client') and self.supabase_client and company_id:
                # Convert datetime to ISO string
                if isinstance(last_activity_created_at, datetime):
                    last_sync_str = last_activity_created_at.isoformat()
                else:
                    last_sync_str = str(last_activity_created_at)

                # Update the last_sync field in kommo_config
                result = self.supabase_client.client.schema("cf_kommo").table(
                    "kommo_config").update({
                        "last_sync": last_sync_str
                    }).eq("company_id", company_id).eq("active",
                                                       True).execute()

                logger.info(
                    f"Updated last sync timestamp for company {company_id}: {last_sync_str}"
                )
                return True
        except Exception as e:
            logger.error(f"Error saving last sync record: {e}")
            return False

    def _get_last_sync_timestamp(self, company_id):
        """Get the last sync timestamp from database"""
        try:
            if hasattr(
                    self,
                    'supabase_client') and self.supabase_client and company_id:
                result = self.supabase_client.client.schema("cf_kommo").table(
                    "kommo_config").select("last_sync").eq(
                        "company_id", company_id).eq("active", True).execute()

                if result.data and result.data[0].get('last_sync'):
                    last_sync_str = result.data[0]['last_sync']
                    try:
                        # Handle both ISO format and timezone-aware strings
                        if 'T' in last_sync_str:
                            if last_sync_str.endswith('Z'):
                                last_sync_date = datetime.fromisoformat(
                                    last_sync_str.replace('Z', '+00:00'))
                            elif '+' in last_sync_str or last_sync_str.endswith(
                                    'UTC'):
                                last_sync_date = datetime.fromisoformat(
                                    last_sync_str.replace('UTC', '+00:00'))
                            else:
                                # Assume UTC if no timezone info
                                last_sync_date = datetime.fromisoformat(
                                    last_sync_str).replace(tzinfo=timezone.utc)
                        else:
                            # Handle timestamp format
                            last_sync_date = datetime.fromtimestamp(
                                float(last_sync_str), tz=timezone.utc)

                        # Ensure the date is timezone-aware and not in the future
                        if last_sync_date.tzinfo is None:
                            last_sync_date = last_sync_date.replace(
                                tzinfo=timezone.utc)

                        now = datetime.now(timezone.utc)
                        if last_sync_date > now:
                            logger.warning(
                                f"Last sync date {last_sync_date} is in the future, using current time"
                            )
                            last_sync_date = now

                        # Add safety margin of 60 seconds to avoid boundary misses
                        from_dt = last_sync_date - timedelta(seconds=60)
                        logger.info(
                            f"Retrieved last sync date from database: {last_sync_date}, using: {from_dt}"
                        )
                        return int(from_dt.timestamp())
                    except Exception as parse_error:
                        logger.error(
                            f"Error parsing last_sync date '{last_sync_str}': {parse_error}"
                        )
                        # Fall through to fallback logic
                else:
                    logger.info("No last_sync found in database")

            # If no last sync or error parsing, get events from last 7 days to avoid overload
            fallback_date = datetime.now(timezone.utc) - timedelta(days=7)
            logger.info(f"Using fallback date: {fallback_date}")
            return int(fallback_date.timestamp())
        except Exception as e:
            logger.error(f"Error getting last sync timestamp: {e}")
            # Fallback to last 24 hours
            fallback_date = datetime.now(timezone.utc) - timedelta(hours=24)
            logger.info(
                f"Error fallback: syncing last 24 hours from: {fallback_date}")
            return int(fallback_date.timestamp())

    def get_stages_list(self, company_id=None, active_only=True):
        """
        Retrieve stages from specific pipelines in Kommo CRM
        Args:
            company_id (str): Optional company ID to filter stages
            active_only (bool): If True, only return active users (default: True)
        """
        try:
            logger.info("Retrieving ALL stages from Kommo CRM")

            # Get safe pagination limits
            limits = self._get_safe_pagination_limits()

            users_data = []
            page = 1
            max_stages_pages = 3  # Users são geralmente poucos, limite menor

            while page <= max_stages_pages:
                try:
                    response = self._make_request("leads/pipelines",
                                                  params={
                                                      "page": page,
                                                      "limit":
                                                      limits['page_size']
                                                  })

                    if response is not None:
                        users = response["_embedded"]["users"]
                        users_data.extend(users)

                        logger.info(
                            f"Página {page}: {len(users)} usuários encontrados (total: {len(users_data)})"
                        )

                    page += 1
                    time.sleep(limits['delay_between_pages'])

                except Exception as e:
                    logger.error(
                        f"Erro ao buscar usuários página {page}: {str(e)}")
                    break

            logger.info(f"Total de usuários encontrados: {len(users_data)}")

            # Process users data into a more usable format
            processed_users = []
            for user in users_data:
                # Skip inactive users if active_only is True
                is_active = user.get("rights", {}).get("is_active", False)
                if active_only and not is_active:
                    continue

                processed_users.append({
                    "id":
                    user.get("id"),
                    "nome":
                    f"{user.get('name', '')} {user.get('lastname', '')}".strip(
                    ),
                    "email":
                    user.get("email"),
                    "foto_url":
                    user.get("_links", {}).get("avatar", {}).get("href"),
                    "criado_em": (parse_datetime_sp(user.get("created_at"))
                                  if user.get("created_at") else None),
                    "cargo":
                    user.get("rights", {}).get("is_admin") and "Administrador"
                    or "Corretor"
                })

            logger.info(
                f"Usuários processados (ativos): {len(processed_users)}")
            return pd.DataFrame(processed_users)
        except Exception as e:
            logger.error(f"Failed to retrieve users: {str(e)}")

    def get_leads(self, company_id=None):
        """
        Retrieve leads from specific pipelines in Kommo CRM
        Args:
            company_id (str): Optional company ID to filter leads
        """
        try:
            # Get company_id from config
            company_id = company_id or self.api_config.get('company_id')

            # Get target pipeline IDs from database for this specific company
            target_pipeline_ids = []
            if hasattr(self, 'supabase_client') and self.supabase_client:
                try:
                    # Get pipeline_id from kommo_config table for this company
                    result = self.supabase_client.client.schema(
                        "cf_kommo").table("kommo_config").select(
                            "pipeline_id").eq("company_id",
                                              company_id).eq("active",
                                                             True).execute()

                    if result.data and result.data[0].get('pipeline_id'):
                        pipeline_data = result.data[0]['pipeline_id']
                        if isinstance(pipeline_data, list):
                            target_pipeline_ids = pipeline_data
                        elif isinstance(pipeline_data, str):
                            # Try to parse as JSON array
                            import json
                            try:
                                target_pipeline_ids = json.loads(pipeline_data)
                            except:
                                logger.error(
                                    f"Failed to parse pipeline_id JSON for company {company_id}"
                                )
                                return pd.DataFrame()

                        logger.info(
                            f"Company {company_id} - Pipeline IDs from config: {target_pipeline_ids}"
                        )
                    else:
                        logger.error(
                            f"No pipeline_id found in kommo_config for company {company_id}"
                        )
                        return pd.DataFrame()

                except Exception as e:
                    logger.error(
                        f"Error getting pipeline IDs from database for company {company_id}: {e}"
                    )
                    return pd.DataFrame()
            else:
                logger.error("Supabase client not available")
                return pd.DataFrame()

            if not target_pipeline_ids:
                logger.error(
                    f"No pipeline IDs configured for company {company_id}")
                return pd.DataFrame()

            # Get safe pagination limits
            limits = self._get_safe_pagination_limits()

            logger.info(
                f"Buscando etapas dos pipelines específicos: {target_pipeline_ids}"
            )
            pipeline_response = self._make_request("leads/pipelines")

            if not pipeline_response:
                logger.error("Failed to retrieve pipelines from Kommo API")
                return pd.DataFrame(), pd.DataFrame()

            pipelines = pipeline_response.get("_embedded",
                                              {}).get("pipelines", [])

            # Create maps for stage information lookup
            status_to_stage_map = {}  # Maps status_id to stage info
            pipeline_name_map = {}  # Maps pipeline_id to pipeline name

            # Collect all stages from configured pipelines
            all_stages = []

            for pipeline in pipelines:
                pipeline_id = pipeline.get("id")
                pipeline_name = pipeline.get("name", "")

                # Store pipeline name mapping
                pipeline_name_map[pipeline_id] = pipeline_name

                # Only process stages for configured pipelines
                if pipeline_id in target_pipeline_ids:
                    for status in pipeline.get("_embedded",
                                               {}).get("statuses", []):
                        status_id = status.get("id")
                        status_name = status.get("name")

                        # Store stage information for lookup
                        status_to_stage_map[status_id] = {
                            'stage_id': status_id,
                            'stage_name': status_name,
                            'pipeline_id': pipeline_id,
                            'pipeline_name': pipeline_name
                        }

                        # Add stage to collection for stages_list table
                        all_stages.append({
                            "stage_id": status_id,
                            "stage_name": status_name,
                            "pipeline_id": pipeline_id,
                            "pipeline_name": pipeline_name,
                            "position": status.get("sort", 0)
                        })

            # Create DataFrame with all collected stages
            df_stages = pd.DataFrame(all_stages)

            logger.info(
                f"Etapas dos pipelines {target_pipeline_ids} carregadas com sucesso"
            )
            logger.info(
                f"Retrieving ALL leads from ALL configured pipelines {target_pipeline_ids} in Kommo CRM"
            )

            filtered_leads = []

            # Fetch leads from ALL configured pipelines
            for pipeline_id in target_pipeline_ids:
                page = 1
                per_page = limits['page_size']

                logger.info(f"Fetching ALL leads from pipeline {pipeline_id}")

                while page <= limits['max_pages_per_request'] and len(
                        filtered_leads) < limits['max_total_records']:
                    params = {
                        "page": page,
                        "limit": per_page,
                        "with":
                        "contacts,pipeline_id,loss_reason,catalog_elements,company",
                        "filter[pipeline_id]": pipeline_id
                    }

                    try:
                        response = self._make_request("leads", params=params)

                        # If response is None (204 No Content), no more data available
                        if response is None:
                            logger.info(
                                f"Pipeline {pipeline_id}, Página {page}: API retornou 204 - não há mais dados - finalizando pipeline"
                            )
                            break

                        if response is not None:
                            leads = response.get("_embedded",
                                                 {}).get("leads", [])

                            if leads:
                                filtered_leads.extend(leads)
                                logger.info(
                                    f"Pipeline {pipeline_id}, Página {page}: {len(leads)} leads encontrados"
                                )
                            else:
                                logger.info(
                                    f"Pipeline {pipeline_id}, Página {page}: nenhum lead encontrado - finalizando pipeline"
                                )
                                break  # No more leads in this pipeline

                        # Rate limiting delay between requests
                        time.sleep(limits['delay_between_pages'])
                        page += 1

                    except Exception as e:
                        logger.error(
                            f"Erro no pipeline {pipeline_id}, página {page}: {str(e)}"
                        )
                        break

                logger.info(f"Finalizou busca no pipeline {pipeline_id}")

            # Check if we hit limits
            if len(filtered_leads) >= limits['max_total_records']:
                logger.warning(
                    f"Limite de registros atingido: {limits['max_total_records']}"
                )

            logger.info(
                f"Total de leads encontrados em TODOS os pipelines: {len(filtered_leads)}"
            )

            # Buscar todos os usuários para mapear nomes para IDs
            users_df = self.get_users(active_only=False)  # Incluir usuários inativos também
            user_name_to_id = {}
            if not users_df.empty:
                for _, user in users_df.iterrows():
                    user_name_to_id[user['nome']] = user['id']
                logger.info(f"Mapeamento de usuários criado: {len(user_name_to_id)} usuários")

            processed_leads = []
            for lead in filtered_leads:
                # Nova lógica para extrair responsavel_id do custom_fields_values
                responsavel_id = None
                corretor_responsavel_name = None
                
                # Processar custom_fields_values para buscar "Corretor responsável"
                custom_fields = lead.get("custom_fields_values", [])
                if custom_fields:
                    for field in custom_fields:
                        if isinstance(field, dict):
                            field_name = field.get("field_name")
                            if field_name == "Corretor responsável":
                                values = field.get("values", [])
                                if values and len(values) > 0 and isinstance(values[0], dict):
                                    corretor_responsavel_name = values[0].get("value")
                                    if corretor_responsavel_name:
                                        # Buscar ID do usuário pelo nome
                                        responsavel_id = user_name_to_id.get(corretor_responsavel_name)
                                        if responsavel_id:
                                            logger.debug(f"Lead {lead.get('id')}: Corretor '{corretor_responsavel_name}' mapeado para ID {responsavel_id}")
                                        else:
                                            logger.warning(f"Lead {lead.get('id')}: Corretor '{corretor_responsavel_name}' não encontrado nos usuários")
                                break
                
                # Se não encontrou corretor responsável nos custom_fields, usar o responsible_user_id original
                if responsavel_id is None:
                    responsavel_id = lead.get("responsible_user_id")

                contato_nome = ""
                if lead.get("_embedded", {}).get("contacts"):
                    contato_nome = lead["_embedded"]["contacts"][0].get(
                        "name", "")

                # Nova lógica para status_id baseado no campo "Perdidos"
                status_id = lead.get("status_id")
                perdido_status = False
                
                # Processar custom_fields_values para buscar campo "Perdidos"
                if custom_fields:
                    for field in custom_fields:
                        if isinstance(field, dict):
                            field_name = field.get("field_name")
                            if field_name == "Perdidos":
                                values = field.get("values", [])
                                if values and len(values) > 0 and isinstance(values[0], dict):
                                    perdido_value = values[0].get("value")
                                    if perdido_value is True or str(perdido_value).lower() == "true":
                                        perdido_status = True
                                        logger.debug(f"Lead {lead.get('id')}: Campo 'Perdidos' definido como true")
                                break
                
                # Setar status_id = 143 apenas se campo "Perdidos" estiver true
                if perdido_status:
                    status_id = 143
                
                pipeline_id = lead.get("pipeline_id")

                # Get stage information from our mapping
                stage_info = status_to_stage_map.get(status_id)
                if stage_info:
                    # Format: stage_name (pipeline_name)
                    etapa = f"{stage_info['stage_name']} ({stage_info['pipeline_name']})"
                else:
                    # Fallback if stage not found in configured pipelines
                    pipeline_name = pipeline_name_map.get(
                        pipeline_id, "Pipeline Desconhecido")
                    etapa = f"Etapa Desconhecida ({pipeline_name})"

                processed_leads.append({
                    "id":
                    lead.get("id"),
                    "nome":
                    lead.get("name"),
                    "responsavel_id":
                    responsavel_id,
                    "contato_nome":
                    contato_nome,
                    "valor":
                    lead.get("price"),
                    "status_id":
                    status_id,  # This is the stage_id
                    "pipeline_id":
                    pipeline_id,
                    "etapa":
                    etapa,  # Format: stage_name (pipeline_name)
                    "custom_fields_values":
                    lead.get("custom_fields_values", {}),
                    "criado_em": (parse_datetime_sp(lead.get("created_at"))
                                  if lead.get("created_at") else None),
                    "atualizado_em": (parse_datetime_sp(lead.get("updated_at"))
                                      if lead.get("updated_at") else None),
                    "fechado":
                    lead.get("closed_at") is not None,
                    "status":
                    ("Ganho" if status_id == 142 else
                     "Perdido" if status_id == 143 else "Em progresso")
                })

            df_leads = pd.DataFrame(processed_leads)

            logger.info(
                f"Leads processados com sucesso: {len(processed_leads)} leads")
            logger.info(f"Stages processados: {len(all_stages)} stages")

            return df_leads, df_stages

        except Exception as e:
            logger.error(f"Erro ao buscar leads: {str(e)}")
            return pd.DataFrame(), pd.DataFrame()

    def get_activities(self,
                       company_id=None,
                       last_sync_date=None,
                       event_types=None):
        """
        Retrieve activities from Kommo CRM filtering only specific event types.
        Implements incremental sync with proper deduplication and last sync tracking.
        """
        try:
            logger.info(
                "Retrieving activities with incremental sync from Kommo CRM")

            # 1) company_id
            company_id = company_id or (self.api_config.get('company_id')
                                        if self.api_config else None)
            if not company_id:
                logger.error("Company ID is required for activity sync")
                return pd.DataFrame()

            # 2) event types
            requested_event_types = event_types or [
                "lead_status_changed",
                "outgoing_chat_message",
                "entity_responsible_changed",
            ]
            logger.info(f"Syncing event types: {requested_event_types}")

            # 3) timestamp inicial
            def _to_unix_seconds(dt_or_ts):
                from datetime import datetime, timezone
                if dt_or_ts is None:
                    return None
                if isinstance(dt_or_ts, (int, float)):
                    # Validate timestamp is not in the future or too far in past
                    ts = int(dt_or_ts)
                    now_ts = int(datetime.now(timezone.utc).timestamp())
                    # Check if timestamp is more than 1 day in future or more than 1 year in past
                    if ts > now_ts + 86400:  # 1 day in future
                        logger.warning(
                            f"Timestamp {ts} is in the future, using current time"
                        )
                        return now_ts
                    elif ts < now_ts - 31536000:  # 1 year in past
                        logger.warning(
                            f"Timestamp {ts} is too old, using 7 days ago")
                        return now_ts - 604800  # 7 days ago
                    return ts
                if isinstance(dt_or_ts, str):
                    if dt_or_ts.isdigit():
                        return _to_unix_seconds(int(dt_or_ts))
                    try:
                        dt = datetime.fromisoformat(
                            dt_or_ts.replace('Z', '+00:00'))
                        return _to_unix_seconds(int(dt.timestamp()))
                    except Exception:
                        return None
                if isinstance(dt_or_ts, datetime):
                    if dt_or_ts.tzinfo is None:
                        dt_or_ts = dt_or_ts.replace(tzinfo=timezone.utc)
                    return _to_unix_seconds(int(dt_or_ts.timestamp()))
                return None

            from_store = self._get_last_sync_timestamp(company_id)
            base_from_ts = _to_unix_seconds(
                last_sync_date) or _to_unix_seconds(from_store) or 0
            from_timestamp = base_from_ts + 1 if base_from_ts else 0

            # Convert back to datetime for logging
            if from_timestamp:
                from datetime import datetime, timezone
                from_dt = datetime.fromtimestamp(from_timestamp,
                                                 tz=timezone.utc)
                logger.info(
                    f"Starting incremental sync from (unix): {from_timestamp} ({from_dt.isoformat()})"
                )
            else:
                logger.info(
                    "Starting incremental sync from beginning (no timestamp)")

            # 4) paginação
            limits = self._get_safe_pagination_limits()
            page_size = int(limits.get("page_size", 250))
            max_pages = int(limits.get("max_pages_per_request", 200))

            # 5) coletores
            events_by_id = {}
            max_created_at_seen = 0

            # 6) ids existentes
            existing_activity_ids = set()
            if getattr(self, "supabase_client", None) and company_id:
                try:
                    existing_result = self.supabase_client.client.table("activities") \
                        .select("id") \
                        .eq("company_id", company_id) \
                        .execute()
                    if existing_result.data:
                        existing_activity_ids = {
                            str(item["id"])
                            for item in existing_result.data
                        }
                    logger.info(
                        f"Found {len(existing_activity_ids)} existing activities in DB"
                    )
                except Exception as e:
                    logger.warning(
                        f"Could not load existing activity IDs: {e}")

            # 7) helper functions for processing
            def as_dict(x):
                return x if isinstance(x, dict) else {}

            def as_list(x):
                return x if isinstance(x, list) else []

            def first_dict(lst):
                for item in as_list(lst):
                    if isinstance(item, dict):
                        return item
                return None

            def extract_lead_id_from_payload(payload):
                d = as_dict(payload)
                leads = d.get("leads")
                if isinstance(leads, list) and leads:
                    ld0 = first_dict(leads)
                    if ld0:
                        return ld0.get("id")
                if isinstance(d.get("lead"), dict):
                    return d["lead"].get("id")
                if "lead_id" in d:
                    return d.get("lead_id")
                return None

            def extract_message(payload):
                d = as_dict(payload)
                text = d.get("text") or d.get("message") or d.get("body")
                src = d.get("source") or d.get("channel") or d.get("provider")
                if not text and isinstance(d.get("data"), dict):
                    dd = d["data"]
                    text = dd.get("text") or dd.get("message") or text
                    src = dd.get("source") or src
                return text, src

            def to_dt_from_unix(ts):
                from datetime import datetime, timezone
                try:
                    return datetime.fromtimestamp(int(ts), tz=timezone.utc)
                except Exception:
                    return None

            type_mapping = {
                "lead_status_changed": "mudança_status",
                "outgoing_chat_message": "mensagem_enviada",
                "entity_responsible_changed": "mudança_responsável",
            }

            # 7) loop por tipo com processamento em tempo real
            for event_type in requested_event_types:
                logger.info(
                    f"Fetching type='{event_type}' from {from_timestamp}")

                # Special debugging for outgoing_chat_message
                if event_type == "outgoing_chat_message":
                    from_dt = datetime.fromtimestamp(
                        from_timestamp,
                        tz=timezone.utc) if from_timestamp else None
                    logger.info(
                        f"DEBUG outgoing_chat_message: timestamp={from_timestamp}, date={from_dt}, company_id={company_id}"
                    )

                page = 1
                while page <= max_pages:
                    params = {
                        "page": page,
                        "limit": page_size,
                        "filter[type]": event_type,
                    }

                    # Only add entity filter for non-chat message events
                    if event_type != "outgoing_chat_message":
                        params["filter[entity]"] = "lead"

                    if from_timestamp:
                        params["filter[created_at][from]"] = from_timestamp
                    params["order[created_at]"] = "desc"

                    # Debug logging for outgoing_chat_message
                    if event_type == "outgoing_chat_message":
                        logger.info(
                            f"DEBUG outgoing_chat_message params: {params}")

                    try:
                        resp = self._make_request("events", params=params)
                    except Exception as e:
                        logger.error(
                            f"Request error for {event_type} page {page}: {e}")
                        if "400" in str(e) or "Bad Request" in str(e):
                            logger.warning(
                                f"Skipping {event_type} due to API error")
                            break
                        page += 1
                        time.sleep(limits.get("delay_between_pages", 0.3))
                        continue

                    # Handle None response (204 No Content) - but continue for first few pages
                    if resp is None:
                        logger.info(
                            f"{event_type} page {page}: received 204 No Content"
                        )
                        if page <= 2:  # Continue for first 2 pages in case of temporary 204
                            logger.info(
                                f"Continuing pagination for {event_type} despite 204 on page {page}"
                            )
                            page += 1
                            time.sleep(limits.get("delay_between_pages", 0.3))
                            continue
                        else:
                            logger.info(
                                f"{event_type}: stopping pagination due to 204"
                            )
                            break

                    if not isinstance(resp, dict) or not resp.get("_embedded"):
                        logger.info(
                            f"{event_type} page {page}: empty/invalid response -> stopping"
                        )
                        break

                    evs = resp["_embedded"].get("events") or []
                    if not evs:
                        logger.info(
                            f"{event_type} page {page}: 0 events -> stopping")
                        break

                    # Processar e salvar atividades em tempo real
                    batch_to_save = []
                    added_now = 0

                    for ev in evs:
                        if not isinstance(
                                ev, dict) or ev.get("type") != event_type:
                            continue
                        ev_id = str(ev.get("id"))
                        if not ev_id or ev_id == "None":
                            continue

                        try:
                            created_at_int = int(ev.get("created_at") or 0)
                        except Exception:
                            created_at_int = 0

                        if created_at_int > max_created_at_seen:
                            max_created_at_seen = created_at_int

                        # Verificar se já existe
                        if ev_id in events_by_id or ev_id in existing_activity_ids:
                            continue

                        events_by_id[ev_id] = ev
                        added_now += 1

                        # Processar atividade imediatamente
                        activity_type = ev.get("type") or "unknown"
                        entity_type = ev.get("entity_type", "")
                        entity_id = ev.get("entity_id")

                        va_raw = ev.get("value_after")
                        vb_raw = ev.get("value_before")
                        va = as_dict(va_raw)
                        vb = as_dict(vb_raw)

                        lead_id = None
                        if entity_type == "lead":
                            lead_id = entity_id
                        elif entity_type == "contact":
                            lead_id = extract_lead_id_from_payload(
                                va) or extract_lead_id_from_payload(vb)

                        try:
                            if lead_id is not None and str(lead_id).isdigit():
                                lead_id = int(lead_id)
                        except Exception:
                            pass

                        message_text = message_source = None
                        status_before = status_after = None
                        old_responsible = new_responsible = None

                        if activity_type == "outgoing_chat_message":
                            message_text, message_source = extract_message(va)
                        if activity_type == "lead_status_changed":
                            # Extract status IDs from the JSON structure
                            status_before = None
                            status_after = None

                            # Extract from valor_anterior (vb_raw)
                            if isinstance(vb_raw, list) and len(vb_raw) > 0:
                                if isinstance(
                                        vb_raw[0],
                                        dict) and "lead_status" in vb_raw[0]:
                                    if isinstance(vb_raw[0]["lead_status"],
                                                  dict):
                                        status_before = vb_raw[0][
                                            "lead_status"].get("id")

                            # Extract from valor_novo (va_raw)
                            if isinstance(va_raw, list) and len(va_raw) > 0:
                                if isinstance(
                                        va_raw[0],
                                        dict) and "lead_status" in va_raw[0]:
                                    if isinstance(va_raw[0]["lead_status"],
                                                  dict):
                                        status_after = va_raw[0][
                                            "lead_status"].get("id")

                        if activity_type == "entity_responsible_changed":
                            # Extract responsible user IDs from the JSON structure
                            old_responsible = None
                            new_responsible = None

                            # Extract from valor_anterior (vb_raw)
                            if isinstance(vb_raw, list) and len(vb_raw) > 0:
                                if isinstance(
                                        vb_raw[0], dict
                                ) and "responsible_user" in vb_raw[0]:
                                    if isinstance(
                                            vb_raw[0]["responsible_user"],
                                            dict):
                                        old_responsible = vb_raw[0][
                                            "responsible_user"].get("id")

                            # Extract from valor_novo (va_raw)
                            if isinstance(va_raw, list) and len(va_raw) > 0:
                                if isinstance(
                                        va_raw[0], dict
                                ) and "responsible_user" in va_raw[0]:
                                    if isinstance(
                                            va_raw[0]["responsible_user"],
                                            dict):
                                        new_responsible = va_raw[0][
                                            "responsible_user"].get("id")

                        criado_em = to_dt_from_unix(ev.get("created_at"))

                        from datetime import datetime, timezone

                        # Preparar registro para salvar
                        processed_activity = {
                            "id": ev.get("id"),
                            "lead_id": lead_id,
                            "user_id": ev.get("created_by"),
                            "tipo": type_mapping.get(activity_type, "outro"),
                            "valor_anterior": vb_raw,
                            "valor_novo": va_raw,
                            "status_anterior": status_before,
                            "status_novo": status_after,
                            "texto_mensagem": message_text,
                            "fonte_mensagem": message_source,
                            "responsavel_anterior": old_responsible,
                            "responsavel_novo": new_responsible,
                            "entity_type": entity_type,
                            "entity_id": entity_id,
                            "criado_em": criado_em,
                            "company_id": company_id,
                            "updated_at":
                            datetime.now(timezone.utc).isoformat()
                        }

                        batch_to_save.append(processed_activity)

                    # Salvar batch no banco de dados imediatamente
                    if batch_to_save and getattr(self, "supabase_client",
                                                 None):
                        try:
                            # Preparar dados para inserção
                            activities_data = []
                            for activity in batch_to_save:
                                # Converter datetime para ISO format se necessário
                                if activity.get("criado_em") and hasattr(
                                        activity["criado_em"], 'isoformat'):
                                    activity["criado_em"] = activity[
                                        "criado_em"].isoformat()

                                # Validar campos obrigatórios
                                if activity.get("id"):
                                    activities_data.append(activity)

                            if activities_data:
                                # Usar upsert para inserir/atualizar
                                result = self.supabase_client.client.table(
                                    "activities").upsert(
                                        activities_data,
                                        on_conflict='id').execute()

                                if hasattr(result, "error") and result.error:
                                    logger.error(
                                        f"Error saving activities batch: {result.error}"
                                    )
                                else:
                                    logger.info(
                                        f"Saved {len(activities_data)} activities to database in real-time"
                                    )

                        except Exception as save_error:
                            logger.error(
                                f"Error saving activities batch: {save_error}")

                    logger.info(
                        f"{event_type} page {page}: got {len(evs)}; new added: {added_now}"
                    )

                    if len(evs) < page_size:
                        break

                    page += 1
                    time.sleep(limits.get("delay_between_pages", 0.3))

                logger.info(f"{event_type}: finished pagination")

            # 8) atualizar last sync
            if max_created_at_seen and getattr(self, "supabase_client", None):
                from datetime import datetime, timezone
                try:
                    latest_dt = datetime.fromtimestamp(
                        int(max_created_at_seen), tz=timezone.utc)
                    self._save_last_sync_record(company_id, latest_dt)
                    logger.info(
                        f"Updated last sync timestamp to: {latest_dt.isoformat()}"
                    )
                except Exception as e:
                    logger.error(f"Error saving last sync timestamp: {e}")

            if not events_by_id:
                logger.info("No new activities found since last sync")
                return pd.DataFrame()

            logger.info(
                f"Total NEW activities processed and saved in real-time: {len(events_by_id)}"
            )

            # Retornar DataFrame vazio já que salvamos em tempo real
            # O sync_manager não precisa mais processar as atividades
            return pd.DataFrame()

        except Exception as e:
            logger.error(f"Failed to retrieve activities: {str(e)}")
            raise

    def get_lead_notes(self, lead_id):
        """
        Retrieve notes for a specific lead
        Args:
            lead_id (int): ID of the lead
        """
        try:
            logger.info(f"Retrieving notes for lead {lead_id}")

            notes_data = []
            page = 1

            while True:
                response = self._make_request(
                    f"leads/{lead_id}/notes",
                    params={
                        "page": page,
                        "limit": min(250, 250)  # Respeitando limite
                    })

                if not response:
                    return

                notes = response["_embedded"]["notes"]
                notes_data.extend(notes)
                page += 1

            processed_notes = []
            for note in notes_data:
                # Skip system/automatic notes if possible
                if note.get("created_by") == 0:  # Sistema
                    continue

                processed_notes.append({
                    "id":
                    note.get("id"),
                    "lead_id":
                    lead_id,
                    "user_id":
                    note.get("created_by"),
                    "texto":
                    note.get("text"),
                    "criado_em":
                    datetime.fromtimestamp(note.get("created_at", 0))
                    if note.get("created_at") else None
                })

            return pd.DataFrame(processed_notes)

        except Exception as e:
            logger.error(
                f"Failed to retrieve notes for lead {lead_id}: {str(e)}")
            return pd.DataFrame()

    def get_tasks(self):
        """
        Retrieve all tasks from Kommo CRM
        """
        try:
            logger.info("Retrieving tasks from Kommo CRM")

            tasks_data = []
            page = 1
            max_pages = 3  # Limit to 3 pages (reduzido conforme limitações)

            while True:
                logger.info(f"Fetching tasks page {page}")
                response = self._make_request(
                    "tasks",
                    params={
                        "page": page,
                        "limit": min(30, 250)  # Respeitando limite
                    })

                if not response:
                    logger.info("No more tasks found")
                    return

                tasks = response["_embedded"]["tasks"]
                tasks_data.extend(tasks)
                logger.info(
                    f"Retrieved {len(tasks)} tasks (total: {len(tasks_data)})")

                page += 1

                # Break after specified number of pages to avoid rate limiting
                if page > max_pages:
                    logger.info(
                        f"Reached maximum number of pages ({max_pages})")
                    break

            # Process tasks data into a more usable format
            processed_tasks = []
            for task in tasks_data:
                # Convert timestamps to datetime
                created_at = datetime.fromtimestamp(task.get(
                    "created_at", 0)) if task.get("created_at") else None
                updated_at = datetime.fromtimestamp(task.get(
                    "updated_at", 0)) if task.get("updated_at") else None
                complete_till = datetime.fromtimestamp(
                    task.get("complete_till",
                             0)) if task.get("complete_till") else None

                processed_tasks.append({
                    "id":
                    task.get("id"),
                    "responsavel_id":
                    task.get("responsible_user_id"),
                    "lead_id":
                    task.get("entity_id")
                    if task.get("entity_type") == "lead" else None,
                    "texto":
                    task.get("text"),
                    "tipo":
                    task.get("task_type"),
                    "completada":
                    task.get("is_completed"),
                    "criado_em":
                    created_at,
                    "atualizado_em":
                    updated_at,
                    "prazo":
                    complete_till
                })

            return pd.DataFrame(processed_tasks)

        except Exception as e:
            logger.error(f"Failed to retrieve tasks: {str(e)}")
            raise

    def _make_request_with_params(self, endpoint, base_params, retry_count=3):
        """
        Make request handling multiple parameters with same name
        """
        import urllib.parse

        url = f"{self.api_url}/{endpoint}"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }

        # Build query string manually to handle multiple filter[type] parameters
        query_parts = []
        for key, value in base_params.items():
            if isinstance(value, list):
                for item in value:
                    query_parts.append(
                        f"{urllib.parse.quote(key)}={urllib.parse.quote(str(item))}"
                    )
            else:
                query_parts.append(
                    f"{urllib.parse.quote(key)}={urllib.parse.quote(str(value))}"
                )

        query_string = "&".join(query_parts)
        full_url = f"{url}?{query_string}"

        for attempt in range(retry_count):
            try:
                self.rate_monitor.enforce_rate_limit()

                logger.info(f"Making API request to: {full_url}")
                response = requests.get(full_url, headers=headers)

                logger.info(f"Response status: {response.status_code}")
                logger.debug(f"Response content: {response.text[:500]}")

                response.raise_for_status()

                if not response.text.strip():
                    logger.warning("Empty response received from API")
                    return {}

                return response.json()

            except requests.exceptions.RequestException as e:
                status_code = e.response.status_code if hasattr(
                    e, 'response') else 0

                if status_code in (429, 403, 504):
                    if not self.rate_monitor.handle_kommo_error(
                            status_code, endpoint, attempt):
                        logger.error(
                            f"Stopping retries for {endpoint} due to {status_code}"
                        )
                        raise
                    self.rate_monitor.wait_before_retry(endpoint, attempt)
                else:
                    logger.warning(
                        f"API request failed (attempt {attempt+1}/{retry_count}): {str(e)}"
                    )
                    if attempt >= retry_count - 1:
                        raise
                    time.sleep(2)

import os
from libs.kommo_api import KommoAPI
from libs.sync_manager import SyncManager
from supabase import create_client
import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
import requests
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class SupabaseClient:

    def __init__(self, url=None, key=None):
        self.url = url or os.getenv("VITE_SUPABASE_URL")
        self.key = key or os.getenv("VITE_SUPABASE_ANON_KEY")

        if not self.url or not self.key:
            raise ValueError("Supabase URL and key must be provided")

        try:
            self.client = create_client(self.url, self.key)
            logger.info("Supabase client initialized successfully")
            self.kommo_config = None
            self.rules = None
            self.last_check = datetime.now()

            # Try initial load of config and rules
            self._load_initial_config()

        except Exception as e:
            logger.error(f"Failed to initialize Supabase client: {str(e)}")
            raise

    def _load_initial_config(self):
        """Try to load initial configuration without raising errors"""
        try:
            config = self.client.table("kommo_config").select("*").eq(
                "active", True).execute()
            if config.data:
                self.kommo_config = config.data[0]

                if not self.kommo_config.get('company_id'):
                    company_id = self._get_company_id(
                        self.kommo_config['api_url'],
                        self.kommo_config['access_token'])
                    self.client.table("kommo_config").update({
                        'company_id':
                        company_id
                    }).eq('id', self.kommo_config['id']).execute()
                    self.kommo_config['company_id'] = company_id

                try:
                    self.rules = self.load_rules()
                except Exception as e:
                    logger.warning(f"Could not load rules: {e}")
                    self.rules = {}

                logger.info("Initial configuration loaded successfully")
                return

            logger.info("Waiting for Kommo configuration to be added...")
        except Exception as e:
            logger.error(f"Error loading initial configuration: {str(e)}")

    def _handle_config_update(self, updated_config):
        """Handle kommo_config updates"""
        try:
            if updated_config and updated_config != self.kommo_config:
                logger.info("Kommo configuration updated")
                self.kommo_config = updated_config

                if not updated_config.get('company_id'):
                    company_id = self._get_company_id(
                        updated_config['api_url'],
                        updated_config['access_token'])
                    self.client.table("kommo_config").update({
                        'company_id':
                        company_id
                    }).eq('id', updated_config['id']).execute()
                    updated_config['company_id'] = company_id

                self._sync_all_data(updated_config)
                logger.info("Configuration update handled successfully")
        except Exception as e:
            logger.error(f"Failed to handle config update: {str(e)}")

    def check_config_changes(self):
        """Check for configuration changes periodically"""
        try:
            current_time = datetime.now()
            if (current_time - self.last_check
                ).total_seconds() < 30:  # Check every 30 seconds
                return

            self.last_check = current_time
            result = self.client.table("kommo_config").select("*").execute()

            if not result.data:
                return

            new_config = result.data[0]

            if not self.kommo_config:
                logger.info("New Kommo configuration detected")
                self._handle_config_insert({"new": new_config})
            elif new_config != self.kommo_config:
                logger.info("Kommo configuration updated")
                self._handle_config_update(new_config)

                # Atualiza a cópia da config local
                self.kommo_config = new_config

                if not new_config.get('company_id'):
                    company_id = self._get_company_id(
                        new_config['api_url'], new_config['access_token'])
                    self.client.table("kommo_config").update({
                        'company_id':
                        company_id
                    }).eq('id', new_config['id']).execute()
                    new_config['company_id'] = company_id

                self._sync_all_data(new_config)
                logger.info("Configuration update handled successfully")

        except Exception as e:
            logger.error(f"Failed to check or handle config changes: {str(e)}")

    def _sync_company_data(self, config, company_id):
        """Separate thread function to handle company data synchronization"""
        try:
            logger.info(f"Starting sync thread for company {company_id}")
            kommo_api = KommoAPI(api_url=config['api_url'],
                                 access_token=config['access_token'],
                                 supabase_client=self)
            sync_manager = SyncManager(kommo_api, self, config)

            brokers = kommo_api.get_users()
            leads = kommo_api.get_leads()
            activities = kommo_api.get_activities()

            # Add company_id to all DataFrames
            if not brokers.empty:
                brokers['company_id'] = company_id
            if not leads.empty:
                leads['company_id'] = company_id
            if not activities.empty:
                activities['company_id'] = company_id

            # Sync all data with company_id
            sync_manager.sync_data(brokers=brokers,
                                   leads=leads,
                                   activities=activities,
                                   company_id=company_id)

            # Initialize broker points for this company
            self.initialize_broker_points(company_id)

            # Update broker points after sync
            self.update_broker_points(brokers=brokers,
                                      leads=leads,
                                      activities=activities,
                                      company_id=company_id)
        except Exception as e:
            logger.error(
                f"Error in sync thread for company {company_id}: {str(e)}")

    def _handle_config_insert(self, event):
        """Handle new kommo_config insertion"""
        try:
            new_config = event.get("new", {})
            if new_config:
                logger.info("New Kommo configuration detected")
                self.kommo_config = new_config

                # Get company_id and update config
                company_id = self._get_company_id(new_config['api_url'],
                                                  new_config['access_token'])
                self.client.table("kommo_config").update({
                    'company_id': company_id,
                    'active': True
                }).eq('id', new_config['id']).execute()

                # Setup default rules for new company
                self.setup_company_rules(company_id)

                # Trigger sync through FastAPI endpoint
                try:
                    response = requests.post("http://0.0.0.0:5002/start")
                    if response.status_code == 200:
                        logger.info("Sync started for all companies")

                        while True:
                            try:
                                status_response = requests.get(
                                    "http://0.0.0.0:5002/status")
                                if status_response.status_code == 200:
                                    all_status = status_response.json()
                                    company_status = all_status.get(
                                        str(company_id))

                                    if not company_status:
                                        logger.error(
                                            f"No status found for company {company_id}"
                                        )
                                        break

                                    status = company_status.get('status')

                                    if status in ('initializing', 'running'):
                                        logger.info(
                                            f"Company {company_id} sync in progress: {status}"
                                        )
                                        time.sleep(
                                            30)  # Check every 30 seconds
                                        continue
                                    else:
                                        logger.info(
                                            f"Sync completed for company {company_id} with status: {status}"
                                        )
                                        break
                                else:
                                    logger.error(
                                        f"Failed to get sync status. HTTP {status_response.status_code}"
                                    )
                                    break
                            except Exception as e:
                                logger.error(
                                    f"Exception while checking sync status: {e}"
                                )
                                break
                    else:
                        logger.error(
                            f"Failed to start sync for company {company_id}")
                except Exception as e:
                    logger.error(f"Error in sync process: {str(e)}")
        except Exception as e:
            logger.error(f"Failed to initialize Supabase client: {str(e)}")
            raise

    def insert_log(self, type: str, message: str):
        """Insere um log na tabela sync_logs"""
        try:
            self.client.table("sync_logs").insert({
                "timestamp":
                datetime.now().isoformat(),
                "type":
                type,
                "message":
                message,
                "company_id":
                self.kommo_config.get('company_id')
            }).execute()
        except Exception as e:
            logger.error(f"Failed to insert log: {str(e)}")

    def load_kommo_config(self, company_id=None):
        """Load Kommo API configuration from Supabase"""
        try:
            query = self.client.table("kommo_config").select("*").eq(
                "active", True)

            if company_id:
                query = query.eq("company_id", company_id)

            result = query.execute()
            if hasattr(result, "error") and result.error:
                raise Exception(f"Supabase error: {result.error}")

            if not result.data:
                # Apenas ignorar e retornar lista vazia para quem chamar essa função
                return []

            configs = result.data
            for config in configs:
                if config.get('company_id') is None:
                    company_id = self._get_company_id(config['api_url'],
                                                      config['access_token'])
                    self.client.table("kommo_config").update({
                        'company_id':
                        company_id
                    }).eq('id', config['id']).execute()
                    config['company_id'] = company_id
                    self._sync_all_data(config)

            return configs
        except Exception as e:
            logger.error(f"Failed to load Kommo config: {str(e)}")
            raise

    def _get_company_id(self, api_url, access_token):
        """Get company ID from Kommo API"""
        try:
            response = requests.get(
                f"{api_url}/api/v4/account",
                headers={"Authorization": f"Bearer {access_token}"})
            response.raise_for_status()
            return response.json().get('id')
        except Exception as e:
            logger.error(f"Failed to get company ID: {str(e)}")
            raise

    def _sync_all_data(self, config):
        """Trigger sync for all tables with company_id"""
        try:
            kommo_api = KommoAPI(api_url=config['api_url'],
                                 access_token=config['access_token'],
                                 supabase_client=self)
            brokers = kommo_api.get_users()
            leads = kommo_api.get_leads()
            activities = kommo_api.get_activities()

            # Add company_id to all DataFrames
            for df in [brokers, leads, activities]:
                if not df.empty:
                    df['company_id'] = config['company_id']

            # Sync all data
            self.upsert_brokers(brokers)
            self.upsert_leads(leads)
            self.upsert_activities(activities)

            # Initialize broker points with company_id
            self.initialize_broker_points(config['company_id'])

        except Exception as e:
            logger.error(f"Failed to sync data: {str(e)}")
            raise

    def load_rules(self, company_id=None):
        """Load gamification rules from Supabase for specific company"""
        try:
            company_id = company_id or self.kommo_config.get('company_id')
            if not company_id:
                logger.warning("No company_id provided for loading rules")
                return {}

            # First try to load company-specific rules from company_rules table
            company_rules_result = self.client.table("company_rules").select(
                """
                rules!inner(coluna_nome, pontos),
                pontos,
                active
            """).eq("company_id", company_id).eq("active", True).execute()

            rules_dict = {}

            if company_rules_result.data:
                # Use company-specific rule points
                for rule in company_rules_result.data:
                    rules_dict[rule['rules']['coluna_nome']] = rule['pontos']
                logger.info(f"Loaded {len(rules_dict)} company-specific rules")
            else:
                # Fallback to default rules
                result = self.client.table("rules").select("*").eq(
                    "company_id", company_id).execute()
                if result.data:
                    for rule in result.data:
                        rules_dict[rule['coluna_nome']] = rule['pontos']
                    logger.info(f"Loaded {len(rules_dict)} default rules")
                else:
                    logger.warning("No rules found for company")

            # Also load custom rules
            custom_rules_result = self.client.table("custom_rules").select(
                "*").eq("company_id", company_id).eq("active", True).execute()

            if custom_rules_result.data:
                for rule in custom_rules_result.data:
                    rules_dict[rule['coluna_nome']] = rule['pontos']
                logger.info(
                    f"Added {len(custom_rules_result.data)} custom rules")

            return rules_dict
        except Exception as e:
            logger.error(f"Failed to load rules: {str(e)}")
            return {}

    def upsert_brokers(self, brokers_df):
        """
        Insert or update broker data in the Supabase database

        Args:
            brokers_df (pandas.DataFrame): DataFrame containing broker data
        """
        try:
            if brokers_df.empty:
                logger.warning("No broker data to insert")
                return

            logger.info(f"Upserting {len(brokers_df)} brokers to Supabase")

            # Filtrar apenas corretores
            brokers_df_filtered = brokers_df[brokers_df['cargo'] ==
                                             'Corretor'].copy()

            if brokers_df_filtered.empty:
                logger.warning("No brokers with 'Corretor' role found")
                return

            # Convert DataFrame to list of dicts
            brokers_data = brokers_df_filtered.to_dict(orient="records")

            # Add updated_at timestamp
            for broker in brokers_data:
                broker["updated_at"] = datetime.now().isoformat()

            # Upsert data to Supabase - inserir novos e atualizar existentes
            result = self.client.table("brokers").upsert(
                brokers_data, on_conflict='id').execute()

            if hasattr(result, "error") and result.error:
                raise Exception(f"Supabase error: {result.error}")

            logger.info(
                f"Brokers upserted successfully: {len(brokers_data)} records processed"
            )
            return result

        except Exception as e:
            logger.error(f"Failed to upsert brokers: {str(e)}")
            raise

    def upsert_leads(self, leads_df):
        """
        Insert or update lead data in the Supabase database

        Args:
            leads_df (pandas.DataFrame): DataFrame containing lead data
        """
        try:
            if leads_df.empty:
                logger.warning("No lead data to insert")
                return

            logger.info(f"Upserting {len(leads_df)} leads to Supabase")

            # Make a copy of the DataFrame to avoid modifying the original
            leads_df_clean = leads_df.copy()

            # Replace infinite values with None (null in JSON)
            numeric_cols = leads_df_clean.select_dtypes(
                include=['float', 'int']).columns
            for col in numeric_cols:
                # Replace NaN and infinite values with None
                mask = ~np.isfinite(leads_df_clean[col])
                if mask.any():
                    leads_df_clean.loc[mask, col] = None

            # Convert bigint columns from float to int to avoid "invalid input syntax for type bigint" errors
            bigint_columns = ['responsavel_id', 'visitor_lead_id', 'amocrm_id']
            for col in bigint_columns:
                if col in leads_df_clean.columns:
                    # Only convert finite values (NaN/None will be handled separately)
                    mask = np.isfinite(leads_df_clean[col])
                    if mask.any():
                        leads_df_clean.loc[mask,
                                           col] = leads_df_clean.loc[mask, col].astype(
                                               'Int64')

            # The 'id' column in leads table is of type TEXT in SQL, but Kommo API might return it as a number
            # We need to ensure it's converted to string
            if 'id' in leads_df_clean.columns:
                leads_df_clean['id'] = leads_df_clean['id'].astype(str)

            # Convert datetime columns to ISO format
            datetime_columns = [
                'criado_em', 'atualizado_em', 'data_contato', 'data_criacao_amocrm'
            ]
            for col in datetime_columns:
                if col in leads_df_clean.columns:
                    # Use errors='coerce' to turn unparseable dates into NaT
                    leads_df_clean[col] = pd.to_datetime(leads_df_clean[col],
                                                         errors='coerce')
                    # Convert NaT to None for database compatibility
                    leads_df_clean[col] = leads_df_clean[col].apply(
                        lambda x: x.isoformat() if pd.notnull(x) else None)

            # Upsert data to Supabase - inserir novos e atualizar existentes
            leads_data = leads_df_clean.to_dict(orient="records")

            result = self.client.table("leads").upsert(
                leads_data, on_conflict='id').execute()

            if hasattr(result, "error") and result.error:
                raise Exception(f"Supabase error: {result.error}")

            logger.info(f"Leads upserted successfully: {len(leads_data)} records processed")
            return result

        except Exception as e:
            logger.error(f"Failed to upsert leads: {str(e)}")
            raise

    def upsert_activities(self, activities_df):
        """
        Insert or update activity data in the Supabase database

        Args:
            activities_df (pandas.DataFrame): DataFrame containing activity data
        """
        try:
            if activities_df.empty:
                logger.warning("No activity data to insert")
                return

            logger.info(f"Processing {len(activities_df)} activities")

            # First, get a list of all lead_ids in the leads table
            try:
                # Query existing lead IDs from the database to ensure we only insert activities for existing leads
                leads_result = self.client.table("leads").select(
                    "id").execute()
                if hasattr(leads_result, "error") and leads_result.error:
                    raise Exception(
                        f"Supabase error querying leads: {leads_result.error}")

                # Create a set of existing lead IDs for faster lookup
                existing_lead_ids = set()
                for lead in leads_result.data:
                    existing_lead_ids.add(lead['id'])

                logger.info(
                    f"Found {len(existing_lead_ids)} existing leads in database"
                )
            except Exception as e:
                logger.warning(
                    f"Could not query existing leads, proceeding without validation: {str(e)}"
                )
                existing_lead_ids = None

            # Make a copy of the DataFrame to avoid modifying the original
            activities_df_clean = activities_df.copy()

            # Replace infinite values with None (null in JSON)
            numeric_cols = activities_df_clean.select_dtypes(
                include=['float', 'int']).columns
            for col in numeric_cols:
                # Replace NaN and infinite values with None
                mask = ~np.isfinite(activities_df_clean[col])
                if mask.any():
                    activities_df_clean.loc[mask, col] = None

            # Convert bigint columns from float to int to avoid "invalid input syntax for type bigint" errors
            bigint_columns = ['lead_id', 'user_id']
            for col in bigint_columns:
                if col in activities_df_clean.columns:
                    # Only convert finite values (NaN/None will be handled separately)
                    mask = np.isfinite(activities_df_clean[col])
                    if mask.any():
                        activities_df_clean.loc[mask,
                                                col] = activities_df_clean.loc[
                                                    mask, col].astype('Int64')

            # The 'id' column in activities table is of type TEXT in SQL, but Kommo API might return it as a number
            # We need to ensure it's converted to string
            if 'id' in activities_df_clean.columns:
                activities_df_clean['id'] = activities_df_clean['id'].astype(
                    str)

            # Get a list of all broker_ids in the brokers table
            try:
                # Query existing broker IDs from the database to ensure we only insert activities with valid user_ids
                brokers_result = self.client.table("brokers").select(
                    "id").execute()
                if hasattr(brokers_result, "error") and brokers_result.error:
                    raise Exception(
                        f"Supabase error querying brokers: {brokers_result.error}")

                # Create a set of existing broker IDs for faster lookup
                existing_broker_ids = set()
                for broker in brokers_result.data:
                    existing_broker_ids.add(broker['id'])

                logger.info(
                    f"Found {len(existing_broker_ids)} existing brokers in database"
                )
            except Exception as e:
                logger.warning(
                    f"Could not query existing brokers, proceeding without validation: {str(e)}"
                )
                existing_broker_ids = None

            # Filter activities to only include those with existing lead_ids and user_ids
            filter_needed = False

            # Filter by lead_id - convert both sides to string for comparison
            if existing_lead_ids is not None and 'lead_id' in activities_df_clean.columns:
                filter_needed = True
                original_count = len(activities_df_clean)

                # Convert existing_lead_ids to strings for comparison
                existing_lead_ids_str = {
                    str(lead_id)
                    for lead_id in existing_lead_ids
                }

                # Convert lead_id column to string for comparison, keeping NaN as NaN
                activities_df_clean['lead_id_str'] = activities_df_clean[
                    'lead_id'].astype(str)
                activities_df_clean.loc[activities_df_clean['lead_id'].isna(),
                                        'lead_id_str'] = None

                # Filter: keep if lead_id exists in leads table OR if lead_id is null
                activities_df_clean = activities_df_clean[
                    activities_df_clean['lead_id_str'].
                    isin(existing_lead_ids_str)
                    | activities_df_clean['lead_id'].isna()]

                # Remove the temporary column
                activities_df_clean = activities_df_clean.drop('lead_id_str',
                                                               axis=1)

                filtered_count = len(activities_df_clean)
                if filtered_count < original_count:
                    logger.warning(
                        f"Filtered out {original_count - filtered_count} activities with non-existent lead_ids"
                    )
                    # Log some examples of filtered lead_ids for debugging
                    if filtered_count > 0:
                        sample_valid_leads = activities_df_clean[
                            'lead_id'].dropna().head(5).tolist()
                        logger.info(
                            f"Sample valid lead_ids in activities: {sample_valid_leads}"
                        )
                    logger.info(
                        f"Total existing leads in database: {len(existing_lead_ids_str)}"
                    )

            # Filter by user_id - convert both sides for comparison
            if existing_broker_ids is not None and 'user_id' in activities_df_clean.columns:
                filter_needed = True
                original_count = len(activities_df_clean)

                # Convert existing_broker_ids to strings for comparison
                existing_broker_ids_str = {
                    str(broker_id)
                    for broker_id in existing_broker_ids
                }

                # Convert user_id column to string for comparison, keeping NaN as NaN
                activities_df_clean['user_id_str'] = activities_df_clean[
                    'user_id'].astype(str)
                activities_df_clean.loc[activities_df_clean['user_id'].isna(),
                                        'user_id_str'] = None

                # Filter: keep if user_id exists in brokers table OR if user_id is null
                activities_df_clean = activities_df_clean[
                    activities_df_clean['user_id_str'].
                    isin(existing_broker_ids_str)
                    | activities_df_clean['user_id'].isna()]

                # Remove the temporary column
                activities_df_clean = activities_df_clean.drop('user_id_str',
                                                               axis=1)

                filtered_count = len(activities_df_clean)
                if filtered_count < original_count:
                    logger.warning(
                        f"Filtered out {original_count - filtered_count} activities with non-existent user_ids"
                    )

            # If we have no activities after filtering, exit early
            if activities_df_clean.empty:
                logger.warning("No valid activities to insert after filtering")
                return

            logger.info(
                f"Upserting {len(activities_df_clean)} activities to Supabase")

            # Convert DataFrame to list of dicts
            activities_data = activities_df_clean.to_dict(orient="records")

            # Add updated_at timestamp and convert datetime objects
            for activity in activities_data:
                activity["updated_at"] = datetime.now().isoformat()

                # Convert datetime objects to ISO format
                if "criado_em" in activity and activity[
                        "criado_em"] is not None:
                    activity["criado_em"] = activity["criado_em"].isoformat()

                # Additional check for any remaining non-JSON compatible values and type conversions
                for key, value in list(
                        activity.items()
                ):  # Create a list to avoid "dictionary changed size during iteration"
                    # Check for NaN, Infinity, -Infinity in float values
                    if isinstance(value, float) and (np.isnan(value)
                                                     or np.isinf(value)):
                        activity[key] = None
                    # Convert float to int for bigint columns
                    elif key in bigint_columns and isinstance(
                            value, float) and value.is_integer():
                        activity[key] = int(value)

            # Upsert data to Supabase - inserir novos e atualizar existentes
            result = self.client.table("activities").upsert(
                activities_data, on_conflict='id').execute()

            if hasattr(result, "error") and result.error:
                raise Exception(f"Supabase error: {result.error}")

            logger.info(
                f"Activities upserted successfully: {len(activities_data)} records processed"
            )
            return result

        except Exception as e:
            logger.error(f"Failed to upsert activities: {str(e)}")
            raise

    def get_broker_points(self):
        """
        Retrieve broker points from the Supabase database
        """
        try:
            logger.info("Retrieving broker points from Supabase")

            result = self.client.table("broker_points").select("*").execute()

            if hasattr(result, "error") and result.error:
                raise Exception(f"Supabase error: {result.error}")

            if not result.data:
                return pd.DataFrame()

            return pd.DataFrame(result.data)

        except Exception as e:
            logger.error(f"Failed to retrieve broker points: {str(e)}")
            raise

    def upsert_broker_points(self, points_df):
        """
        Atualiza ou insere os dados na tabela broker_points no Supabase.

        Args:
            points_df (pandas.DataFrame): DataFrame contendo os dados de pontuação dos corretores.
        """
        import numpy as np
        import logging

        logger = logging.getLogger(__name__)

        try:
            if points_df.empty:
                logger.warning("Nenhum dado de pontos para inserir.")
                return

            # Garante que company_id está presente
            if 'company_id' not in points_df.columns:
                logger.error("DataFrame não contém a coluna company_id")
                return

            # Filtra registros por company_id
            unique_companies = points_df['company_id'].unique()
            all_responses = []

            for company_id in unique_companies:
                company_df = points_df[points_df['company_id'] ==
                                       company_id].copy()

                logger.info(
                    f"Upsert de {len(company_df)} registros na tabela broker_points para company_id {company_id}."
                )

                # Trata valores infinitos ou inválidos
                numeric_cols = company_df.select_dtypes(
                    include=['float', 'int']).columns
                for col in numeric_cols:
                    mask = ~np.isfinite(company_df[col])
                    if mask.any():
                        company_df.loc[mask, col] = None

                # Realiza o upsert na tabela broker_points
                records = company_df.to_dict("records")
                for record in records:
                    for key, value in record.items():
                        if isinstance(value, pd.Timestamp):
                            record[key] = value.isoformat()

                # Verifica se os registros já existem e faz update ou insert
                for record in records:
                    broker_id = record.get('id')
                    if broker_id:
                        try:
                            # Verifica se o registro já existe
                            existing = self.client.table(
                                "broker_points").select("id").eq(
                                    "id",
                                    broker_id).eq("company_id",
                                                  company_id).execute()

                            if existing.data:
                                # Update se existe - remove campos que não devem ser atualizados na condição
                                update_record = {
                                    k: v
                                    for k, v in record.items()
                                    if k not in ['id', 'company_id']
                                }
                                response = self.client.table(
                                    "broker_points").update(update_record).eq(
                                        "id",
                                        broker_id).eq("company_id",
                                                      company_id).execute()
                            else:
                                # Insert se não existe
                                response = self.client.table(
                                    "broker_points").insert(record).execute()

                            if hasattr(response, "error") and response.error:
                                logger.error(
                                    f"Error updating broker points: {response.error}"
                                )
                                raise Exception(
                                    f"Error updating broker points: {response.error}"
                                )

                            all_responses.append(response)
                        except Exception as individual_error:
                            logger.warning(
                                f"Error processing record for broker {broker_id}: {individual_error}"
                            )
                            continue

            return all_responses

        except Exception as e:
            logger.error(f"Erro ao fazer upsert em broker_points: {e}")
            raise

    def ensure_webhook_table(self):
        """
        Ensure the from_webhook table exists with proper structure
        This is a safety check - the table should be created in Supabase dashboard
        """
        try:
            # Test if table exists by trying to select from it
            result = self.client.table("from_webhook").select("*").limit(
                1).execute()
            logger.info("from_webhook table exists and is accessible")
        except Exception as e:
            logger.warning(
                f"from_webhook table may not exist or is not accessible: {str(e)}"
            )
            logger.info(
                "Please ensure the from_webhook table is created in Supabase with the following structure:"
            )
            logger.info("""
            CREATE TABLE from_webhook (
                id SERIAL PRIMARY KEY,
                webhook_type TEXT,
                payload_id TEXT,
                chat_id TEXT,
                talk_id TEXT,
                contact_id TEXT,
                text TEXT,
                created_at TEXT,
                element_type TEXT,
                entity_type TEXT,
                element_id TEXT,
                entity_id TEXT,
                message_type TEXT,
                author_id TEXT,
                author_type TEXT,
                author_name TEXT,
                author_avatar_url TEXT,
                origin TEXT,
                raw_payload JSONB,
                broker_id TEXT,
                lead_id TEXT,
                inserted_at TIMESTAMP DEFAULT NOW()
            );
            """)

    def link_webhook_message_to_broker(self, webhook_message):
        """
        Vincula uma mensagem de webhook ao broker responsável

        Args:
            webhook_message (dict): Dados da mensagem do webhook

        Returns:
            dict: Dados atualizados com broker_id e lead_id
        """
        try:
            broker_id = None
            lead_id = None

            # 1. Se a mensagem tem author_id e é do tipo "outgoing", é do broker
            if (webhook_message.get('author_id')
                    and webhook_message.get('message_type') == 'outgoing'):

                # Verificar se o author_id é um broker válido
                broker_result = self.client.table("brokers").select(
                    "id, nome").eq("id",
                                   webhook_message['author_id']).execute()

                if broker_result.data:
                    broker_id = webhook_message['author_id']
                    logger.info(
                        f"Mensagem vinculada ao broker {broker_id} (mensagem enviada)"
                    )

            # 2. Para mensagens recebidas, buscar pelo lead responsável
            elif webhook_message.get('entity_id') and webhook_message.get(
                    'entity_type') == 'lead':
                lead_result = self.client.table(
                    "leads").select("id, responsavel_id").eq(
                        "id", webhook_message['entity_id']).execute()

                if lead_result.data:
                    lead_data = lead_result.data[0]
                    lead_id = lead_data['id']
                    broker_id = lead_data['responsavel_id']
                    logger.info(
                        f"Mensagem vinculada ao broker {broker_id} via lead {lead_id}"
                    )

            # 3. Se ainda não encontrou, tentar pelo contact_id
            elif webhook_message.get('contact_id'):
                # Buscar leads que tenham esse contact como contato principal
                contact_leads = self.client.table("leads").select(
                    "id, responsavel_id, contato_nome").ilike(
                        "contato_nome",
                        f"%{webhook_message.get('author_name', '')}%").execute(
                        )

                if contact_leads.data:
                    # Pegar o lead mais recente deste contato
                    latest_lead = contact_leads.data[0]
                    lead_id = latest_lead['id']
                    broker_id = latest_lead['responsavel_id']
                    logger.info(
                        f"Mensagem vinculada ao broker {broker_id} via contact matching"
                    )

            # Atualizar o registro do webhook com os IDs encontrados
            if broker_id or lead_id:
                update_data = {}
                if broker_id:
                    update_data['broker_id'] = broker_id
                if lead_id:
                    update_data['lead_id'] = lead_id

                # Atualizar na base de dados se temos o ID do webhook
                if webhook_message.get('id'):
                    self.client.table("from_webhook").update(update_data).eq(
                        "payload_id",
                        webhook_message.get('payload_id')).execute()

                webhook_message.update(update_data)
                logger.info(
                    f"Webhook atualizado com broker_id: {broker_id}, lead_id: {lead_id}"
                )

            return webhook_message

        except Exception as e:
            logger.error(f"Erro ao vincular mensagem ao broker: {str(e)}")
            return webhook_message

    def get_broker_messages(self, broker_id, limit=50):
        """
        Busca mensagens de um broker específico

        Args:
            broker_id (str): ID do broker
            limit (int): Limite de mensagens

        Returns:
            list: Lista de mensagens do broker
        """
        try:
            result = self.client.table("from_webhook").select("*").eq(
                "broker_id",
                broker_id).order("inserted_at",
                                 desc=True).limit(limit).execute()

            return result.data if result.data else []

        except Exception as e:
            logger.error(
                f"Erro ao buscar mensagens do broker {broker_id}: {str(e)}")
            return []

    def get_lead_messages(self, lead_id, limit=50):
        """
        Busca mensagens de um lead específico

        Args:
            lead_id (str): ID do lead
            limit (int): Limite de mensagens

        Returns:
            list: Lista de mensagens do lead
        """
        try:
            result = self.client.table("from_webhook").select("*").eq(
                "lead_id", lead_id).order("inserted_at",
                                          desc=True).limit(limit).execute()

            return result.data if result.data else []

        except Exception as e:
            logger.error(
                f"Erro ao buscar mensagens do lead {lead_id}: {str(e)}")
            return []

    def initialize_broker_points(self, company_id=None):
        """
        Cria registros na tabela broker_points para todos os corretores cadastrados,
        com os campos de pontuação zerados. Evita duplicações verificando existência.
        """
        company_id = company_id or self.kommo_config.get('company_id')
        try:
            # Buscar corretores com cargo "Corretor" e company_id específico
            brokers_result = self.client.table("brokers").select(
                "id, nome").eq("cargo", "Corretor").eq("company_id",
                                                       company_id).execute()
            if hasattr(brokers_result, "error") and brokers_result.error:
                raise Exception(
                    f"Erro ao buscar corretores: {brokers_result.error}")

            brokers = brokers_result.data
            if not brokers:
                logger.warning(
                    "Nenhum corretor encontrado para inicializar broker_points."
                )
                return

            # Buscar registros existentes para evitar duplicatas
            existing_result = self.client.table("broker_points").select(
                "id").eq("company_id", company_id).execute()

            existing_ids = set()
            if existing_result.data:
                existing_ids = {
                    record['id']
                    for record in existing_result.data
                }

            # Filtrar apenas corretores que não têm registros
            brokers_to_insert = [
                b for b in brokers if b['id'] not in existing_ids
            ]

            if not brokers_to_insert:
                logger.info(
                    f"Todos os corretores já têm registros em broker_points para company_id {company_id}"
                )
                return True

            # Criar registros com pontuação zero e company_id (apenas campos do novo schema)
            now = datetime.now().isoformat()
            new_records = [{
                "id": b["id"],
                "company_id": company_id,
                "nome": b["nome"],
                "leads_visitados": 0,
                "propostas_enviadas": 0,
                "vendas_realizadas": 0,
                "leads_perdidos": 0,
                "leads_descartados": 0,
                "pontos": 0,
                "updated_at": now
            } for b in brokers_to_insert]

            # Inserir registros novos
            if new_records:
                result = self.client.table("broker_points").insert(
                    new_records).execute()

                if hasattr(result, "error") and result.error:
                    logger.error(
                        f"Erro ao inserir broker_points: {result.error}")
                    return False

                logger.info(
                    f"Broker points inicializados para {len(new_records)} corretores."
                )
            return True

        except Exception as e:
            logger.error(f"Erro ao inicializar broker_points: {str(e)}")
            # Não fazer raise para não quebrar o fluxo principal
            return False

    def update_broker_points(self,
                             brokers=[],
                             leads=[],
                             activities=[],
                             company_id=None):
        try:
            company_id = company_id or self.kommo_config.get('company_id')
            logger.info(
                f"Starting broker points calculation for company {company_id}")

            # Convert to DataFrames if needed
            if not isinstance(brokers, pd.DataFrame):
                if isinstance(brokers, list) and len(brokers) > 0:
                    brokers = pd.DataFrame(brokers)
                else:
                    logger.warning("No broker data provided")
                    return

            if not isinstance(leads, pd.DataFrame):
                if isinstance(leads, list):
                    leads = pd.DataFrame(leads) if len(
                        leads) > 0 else pd.DataFrame()
                else:
                    leads = pd.DataFrame()

            if not isinstance(activities, pd.DataFrame):
                if isinstance(activities, list):
                    activities = pd.DataFrame(activities) if len(
                        activities) > 0 else pd.DataFrame()
                else:
                    activities = pd.DataFrame()

            # Get date filter from component_filters table
            date_filter_start = None
            date_filter_end = None
            filter_type = None
            filter_data = {}

            try:
                filter_result = self.client.table("component_filters").select(
                    "*").eq("component_name",
                            "ranking_metrics").eq("company_id",
                                                  company_id).execute()

                if filter_result.data:
                    filter_data = filter_result.data[0]
                    filter_type = filter_data.get('filter_type')
                else:
                    logger.info(
                        "No component_filters found for ranking_metrics, using all data"
                    )

            except Exception as inner_e:
                logger.error(f"Erro ao buscar filtro de datas: {inner_e}")

            from datetime import datetime, timedelta
            import pytz

            sao_paulo_tz = pytz.timezone('America/Sao_Paulo')
            now = datetime.now(sao_paulo_tz)

            if filter_type == 'custom_range':
                start_date = filter_data.get('start_date')
                end_date = filter_data.get('end_date')

                if start_date and end_date:
                    date_filter_start = pd.to_datetime(start_date, utc=True)
                    date_filter_end = pd.to_datetime(end_date, utc=True)
                    logger.info(
                        f"Using custom date range filter: {date_filter_start} to {date_filter_end}"
                    )
                else:
                    logger.info(
                        "Filter type is custom_range but dates are null, using all data"
                    )

            elif filter_type == 'current_month':
                first_day_of_month = now.replace(day=1,
                                                 hour=0,
                                                 minute=0,
                                                 second=0,
                                                 microsecond=0)
                date_filter_start = pd.to_datetime(first_day_of_month,
                                                   utc=True)
                date_filter_end = pd.to_datetime(now, utc=True)
                logger.info(
                    f"Using current month filter: {date_filter_start} to {date_filter_end}"
                )

            elif filter_type == 'month':
                selected_month = int(filter_data.get('month'))
                selected_year = int(filter_data.get('year'))

                first_day = datetime(selected_year,
                                     selected_month,
                                     1,
                                     tzinfo=sao_paulo_tz)
                if selected_month == 12:
                    next_month = datetime(selected_year + 1,
                                          1,
                                          1,
                                          tzinfo=sao_paulo_tz)
                else:
                    next_month = datetime(selected_year,
                                          selected_month + 1,
                                          1,
                                          tzinfo=sao_paulo_tz)

                last_day = next_month - timedelta(seconds=1)

                date_filter_start = pd.to_datetime(first_day, utc=True)
                date_filter_end = pd.to_datetime(last_day, utc=True)

                logger.info(
                    f"Using 'month' filter: {date_filter_start} to {date_filter_end}"
                )

            elif filter_type == 'last_month':
                first_day_current_month = now.replace(day=1,
                                                      hour=0,
                                                      minute=0,
                                                      second=0,
                                                      microsecond=0)
                last_day_last_month = first_day_current_month - timedelta(
                    days=1)
                first_day_last_month = last_day_last_month.replace(
                    day=1, hour=0, minute=0, second=0, microsecond=0)

                date_filter_start = pd.to_datetime(first_day_last_month,
                                                   utc=True)
                date_filter_end = pd.to_datetime(last_day_last_month.replace(
                    hour=23, minute=59, second=59),
                                                 utc=True)
                logger.info(
                    f"Using last month filter: {date_filter_start} to {date_filter_end}"
                )

            elif filter_type == 'current_week':
                days_since_monday = now.weekday()
                monday_this_week = now - timedelta(days=days_since_monday)
                monday_this_week = monday_this_week.replace(hour=0,
                                                            minute=0,
                                                            second=0,
                                                            microsecond=0)

                date_filter_start = pd.to_datetime(monday_this_week, utc=True)
                date_filter_end = pd.to_datetime(now, utc=True)
                logger.info(
                    f"Using current week filter: {date_filter_start} to {date_filter_end}"
                )

            else:
                logger.info(f"Unknown or no filter type set, using all data")

            # Apply date filter to leads and activities if applicable
            if date_filter_start and date_filter_end:
                if not leads.empty and 'criado_em' in leads.columns:
                    leads['criado_em'] = pd.to_datetime(leads['criado_em'],
                                                        errors='coerce',
                                                        utc=True)
                    leads = leads[(leads['criado_em'] >= date_filter_start)
                                  & (leads['criado_em'] <= date_filter_end)]
                    logger.info(
                        f"Filtered leads to {len(leads)} records within date range"
                    )

                if not activities.empty and 'criado_em' in activities.columns:
                    activities['criado_em'] = pd.to_datetime(
                        activities['criado_em'], errors='coerce', utc=True)
                    activities = activities[
                        (activities['criado_em'] >= date_filter_start)
                        & (activities['criado_em'] <= date_filter_end)]
                    logger.info(
                        f"Filtered activities to {len(activities)} records within date range"
                    )

            # Load current rules for this company
            rules = self.load_rules(company_id)
            if not rules:
                logger.warning(
                    f"No rules found for point calculation for company {company_id}"
                )
                return

            existing_points = self.client.table("broker_points").select(
                "*").eq("company_id", company_id).execute()
            points_dict = {
                point['id']: point
                for point in existing_points.data
            }

            for _, broker in brokers.iterrows():
                broker_id = broker['id']
                broker_name = broker.get('nome', 'Unknown')
                total_points = 0
                rule_results = {}

                broker_leads = leads[
                    leads['responsavel_id'] ==
                    broker_id] if not leads.empty else pd.DataFrame()
                broker_activities = activities[
                    activities['user_id'] ==
                    broker_id] if not activities.empty else pd.DataFrame()

                logger.info(
                    f"Calculating points for broker {broker_name} (ID: {broker_id})"
                )
                logger.info(f"  - {len(broker_leads)} leads")
                logger.info(f"  - {len(broker_activities)} activities")

                for rule_name, rule_config in rules.items():
                    try:
                        count = self._calculate_rule_points(
                            rule_name, rule_config, broker_leads,
                            broker_activities, leads, activities, company_id, broker_id)
                        rule_results[rule_name] = count

                        points_per_occurrence = rule_config.get(
                            'pontos', 0) if isinstance(rule_config,
                                                       dict) else rule_config
                        rule_points = count * points_per_occurrence
                        total_points += rule_points

                        if count > 0:
                            logger.info(
                                f"  - {rule_name}: {count} occurrences × {points_per_occurrence} = {rule_points} points"
                            )

                    except Exception as e:
                        logger.error(
                            f"Error calculating rule {rule_name} for broker {broker_id}: {str(e)}"
                        )
                        rule_results[rule_name] = 0

                broker_points_data = {
                    'id': broker_id,
                    'company_id': company_id,
                    'pontos': total_points,
                    'nome': broker_name,
                    'updated_at': datetime.now().isoformat()
                }

                # Mapear resultados das regras para campos específicos do schema
                schema_field_mapping = {
                    'leads_visitados': 'leads_visitados',
                    'propostas_enviadas': 'propostas_enviadas',
                    'vendas_realizadas': 'vendas_realizadas',
                    'leads_perdidos': 'leads_perdidos',
                    'leads_descartados': 'leads_descartados'
                }

                for rule_name, count in rule_results.items():
                    if rule_name in schema_field_mapping:
                        field_name = schema_field_mapping[rule_name]
                        broker_points_data[field_name] = count
                        logger.info(f"  - Mapped {rule_name}: {count} → broker_points.{field_name}")

                        # Log específico para leads_perdidos para debug
                        if rule_name == 'leads_perdidos':
                            logger.info(f"  - 🔥 MAPEANDO leads_perdidos: {count} para broker {broker_name}")
                            logger.info(f"  - 📋 Valor {count} será salvo na coluna {field_name} da tabela broker_points")
                            logger.info(f"  - 🎯 broker_points_data['{field_name}'] = {broker_points_data[field_name]}")

                # Debug final: mostrar todos os dados que serão salvos
                logger.info(f"📊 broker_points_data FINAL para {broker_name}: {broker_points_data}")

                try:
                    existing_check = self.client.table("broker_points").select(
                        "*").eq("id", broker_id).eq("company_id",
                                                    company_id).execute()

                    if existing_check.data:
                        existing_data = existing_check.data[0]
                        update_data = {}

                        for key, new_value in broker_points_data.items():
                            if key in ['id', 'company_id']:
                                continue

                            existing_value = existing_data.get(key)
                            if existing_value != new_value:
                                if isinstance(existing_value,
                                              (int, float)) and isinstance(
                                                  new_value, (int, float)):
                                    if existing_value != new_value:
                                        update_data[key] = new_value
                                else:
                                    update_data[key] = new_value

                        if update_data:
                            # Log especial para leads_perdidos
                            if 'leads_perdidos' in update_data:
                                logger.info(f"🔥 ATUALIZANDO leads_perdidos para {broker_name}: {update_data['leads_perdidos']}")
                            
                            result = self.client.table("broker_points").update(
                                update_data).eq("id", broker_id).eq(
                                    "company_id", company_id).execute()

                            if hasattr(result, "error") and result.error:
                                logger.error(
                                    f"Update error for broker {broker_id}: {result.error}"
                                )
                                continue

                            logger.info(
                                f"Updated {len(update_data)} fields for {broker_name}: {total_points} total points"
                            )
                            
                            # Verificação adicional para leads_perdidos
                            if 'leads_perdidos' in update_data:
                                logger.info(f"✅ leads_perdidos atualizado com sucesso: {update_data['leads_perdidos']}")
                        else:
                            logger.info(
                                f"No changes detected for {broker_name} - skipping update"
                            )
                    else:
                        # Log especial para leads_perdidos na inserção
                        if broker_points_data.get('leads_perdidos', 0) > 0:
                            logger.info(f"🔥 INSERINDO leads_perdidos para {broker_name}: {broker_points_data['leads_perdidos']}")
                        
                        result = self.client.table("broker_points").insert(
                            broker_points_data).execute()

                        if hasattr(result, "error") and result.error:
                            logger.error(
                                f"Insert error for broker {broker_id}: {result.error}"
                            )
                            continue

                        logger.info(
                            f"Inserted new record for {broker_name}: {total_points} total points"
                        )
                        
                        # Verificação adicional para leads_perdidos na inserção
                        if broker_points_data.get('leads_perdidos', 0) > 0:
                            logger.info(f"✅ leads_perdidos inserido com sucesso: {broker_points_data['leads_perdidos']}")

                except Exception as db_error:
                    logger.error(
                        f"Database error for broker {broker_id}: {str(db_error)}"
                    )
                    continue

            logger.info("Broker points calculation completed successfully")

            # Calculate dynamic metrics after broker points
            self.calculate_dynamic_metrics(company_id)

        except Exception as e:
            logger.error(f"Error updating broker points: {str(e)}")
            return

    def generate_sla_report(self, company_id, start_date=None, end_date=None):
        """
        Gera relatório detalhado de SLA para todos os corretores.

        Args:
            company_id: ID da empresa
            start_date: Data inicial (opcional)
            end_date: Data final (opcional)

        Returns:
            dict: Relatório completo de SLA por corretor
        """
        try:
            logger.info(f"Gerando relatório SLA para empresa {company_id}")

            # Buscar todos os corretores
            brokers_result = self.client.table("brokers").select(
                "id, nome").eq("company_id",
                               company_id).eq("cargo", "Corretor").execute()

            if not brokers_result.data:
                logger.warning(
                    f"Nenhum corretor encontrado para empresa {company_id}")
                return {}

            # Buscar todas as atividades relevantes
            activities_query = self.client.table("activities").select("*").eq(
                "company_id", company_id)

            if start_date:
                activities_query = activities_query.gte(
                    "criado_em", start_date.isoformat())
            if end_date:
                activities_query = activities_query.lte(
                    "criado_em", end_date.isoformat())

            activities_result = activities_query.execute()

            if not activities_result.data:
                logger.warning("Nenhuma atividade encontrada para o período")
                return {}

            all_activities = pd.DataFrame(activities_result.data)

            # Gerar relatório por corretor
            sla_report = {}

            for broker in brokers_result.data:
                broker_id = broker['id']
                broker_name = broker['nome']

                logger.info(
                    f"Calculando SLA para {broker_name} (ID: {broker_id})")

                perdas_inatividade = self._calculate_leads_perdidos_por_inatividade(
                    broker_id, all_activities, company_id)

                sla_report[broker_id] = {
                    'nome':
                    broker_name,
                    'leads_perdidos_inatividade':
                    perdas_inatividade,
                    'sla_status':
                    'CRÍTICO' if perdas_inatividade > 5 else
                    'ATENÇÃO' if perdas_inatividade > 2 else 'OK'
                }

            logger.info(
                f"Relatório SLA gerado para {len(sla_report)} corretores")
            return sla_report

        except Exception as e:
            logger.error(f"Erro ao gerar relatório SLA: {e}")
            return {}

    def save_sla_metrics(self, company_id):
        """
        Salva métricas de SLA em uma tabela específica para monitoramento.
        """
        try:
            logger.info(f"Salvando métricas SLA para empresa {company_id}")

            sla_report = self.generate_sla_report(company_id)

            if not sla_report:
                logger.warning("Nenhuma métrica SLA para salvar")
                return

            # Preparar dados para inserção
            sla_metrics = []
            current_time = datetime.now().isoformat()

            for broker_id, metrics in sla_report.items():
                sla_metrics.append({
                    'company_id':
                    company_id,
                    'broker_id':
                    broker_id,
                    'broker_name':
                    metrics['nome'],
                    'leads_perdidos_inatividade':
                    metrics['leads_perdidos_inatividade'],
                    'sla_status':
                    metrics['sla_status'],
                    'calculated_at':
                    current_time,
                    'period_start':
                    datetime.now().replace(day=1).isoformat(), # Início do mês
                    'period_end':
                    current_time
                })

            # Salvar na tabela sla_metrics (criar se não existir)
            try:
                result = self.client.table("sla_metrics").upsert(
                    sla_metrics,
                    on_conflict='company_id,broker_id,calculated_at').execute(
                    )

                if hasattr(result, "error") and result.error:
                    logger.error(
                        f"Erro ao salvar métricas SLA: {result.error}")
                else:
                    logger.info(
                        f"Métricas SLA salvas: {len(sla_metrics)} registros")

            except Exception as table_error:
                logger.warning(
                    f"Tabela sla_metrics pode não existir: {table_error}")
                logger.info(
                    "Para criar a tabela sla_metrics, execute no Supabase:")
                logger.info("""
                CREATE TABLE sla_metrics (
                    id SERIAL PRIMARY KEY,
                    company_id TEXT NOT NULL,
                    broker_id BIGINT NOT NULL,
                    broker_name TEXT,
                    leads_perdidos_inatividade INTEGER DEFAULT 0,
                    sla_status TEXT,
                    calculated_at TIMESTAMP,
                    period_start TIMESTAMP,
                    period_end TIMESTAMP,
                    created_at TIMESTAMP DEFAULT NOW(),
                    UNIQUE(company_id, broker_id, calculated_at)
                );
                """)

            return sla_metrics
        except Exception as e:
            logger.error(f"Erro ao salvar métricas SLA: {e}")

            # Note: SLA metrics are now integrated into broker_points calculation
            logger.info("SLA metrics integrated into broker points calculation")

    def setup_company_rules(self, company_id, default_rules=None):
        """
        Setup default rules for a company if they don't exist
        """
        try:
            if not default_rules:
                default_rules = {
                    'leads_visitados': 40,
                    'propostas_enviadas': 8,
                    'vendas_realizadas': 100,
                    'leads_perdidos': -10,
                    'leads_descartados': -5
                }

            # Check if company already has rules
            existing_rules = self.client.table("rules").select("*").eq(
                "company_id", company_id).execute()

            if not existing_rules.data:
                # Create default rules for company
                rules_to_insert = []
                for rule_name, points in default_rules.items():
                    rules_to_insert.append({
                        'nome':
                        rule_name.replace('_', ' ').title(),
                        'coluna_nome':
                        rule_name,
                        'pontos':
                        points,
                        'company_id':
                        company_id,
                        'descricao':
                        f'Regra para {rule_name.replace("_", " ")}'
                    })

                result = self.client.table("rules").insert(
                    rules_to_insert).execute()
                if hasattr(result, "error") and result.error:
                    raise Exception(
                        f"Error creating default rules: {result.error}")

                logger.info(
                    f"Created {len(rules_to_insert)} default rules for company {company_id}"
                )

            return True
        except Exception as e:
            logger.error(f"Error setting up company rules: {str(e)}")
            return False

    def get_sync_status(self, company_id):
        """
        Get sync status from sync_control table
        """
        try:
            result = self.client.table("sync_control").select("*").eq(
                "company_id", company_id).execute()
            if result.data:
                return result.data[0]
            return None
        except Exception as e:
            logger.error(f"Error getting sync status: {str(e)}")
            return None

    def update_sync_status(self, company_id, status, error=None):
        """
        Update sync status in sync_control table
        """
        try:
            update_data = {
                'company_id': company_id,
                'status': status,
                'last_sync': datetime.now().isoformat()
            }

            if error:
                update_data['error'] = error

            result = self.client.table("sync_control").upsert(
                update_data, on_conflict='company_id').execute()
            if hasattr(result, "error") and result.error:
                raise Exception(f"Error updating sync status: {result.error}")

            return True
        except Exception as e:
            logger.error(f"Error updating sync status: {str(e)}")
            return False

    def calculate_dynamic_metrics(self, company_id):
        """
        Calcula as métricas dinâmicas da empresa e salva na tabela metric_results
        """
        try:
            logger.info(
                f"Starting dynamic metrics calculation for company {company_id}"
            )

            # Buscar métricas dinâmicas da empresa
            dynamic_metrics_result = self.client.table(
                "dynamic_metrics").select("*").eq("company_id",
                                                  company_id).execute()

            if not dynamic_metrics_result.data:
                logger.info(
                    f"No dynamic metrics found for company {company_id}")
                return

            dynamic_metrics = dynamic_metrics_result.data
            logger.info(
                f"Found {len(dynamic_metrics)} dynamic metrics for company {company_id}"
            )

            # Buscar todos os leads da empresa para cálculo
            leads_result = self.client.table("leads").select("*").eq(
                "company_id", company_id).execute()

            if not leads_result.data:
                logger.warning(f"No leads found for company {company_id}")
                return

            all_leads = pd.DataFrame(leads_result.data)

            # Apply date filter if configured
            date_filter_start = None
            date_filter_end = None

            try:
                filter_result = self.client.table("component_filters").select(
                    "*").eq("component_name",
                            "ranking_metrics").eq("company_id",
                                                  company_id).execute()

                if filter_result.data:
                    filter_data = filter_result.data[0]
                    filter_type = filter_data.get('filter_type')

                    from datetime import datetime, timedelta
                    import pytz

                    sao_paulo_tz = pytz.timezone('America/Sao_Paulo')
                    now = datetime.now(sao_paulo_tz)

                    if filter_type == 'custom_range':
                        start_date = filter_data.get('start_date')
                        end_date = filter_data.get('end_date')

                        if start_date and end_date:
                            date_filter_start = pd.to_datetime(start_date,
                                                               utc=True)
                            date_filter_end = pd.to_datetime(end_date,
                                                             utc=True)

                    elif filter_type == 'current_month':
                        first_day_of_month = now.replace(day=1,
                                                         hour=0,
                                                         minute=0,
                                                         second=0,
                                                         microsecond=0)
                        date_filter_start = pd.to_datetime(first_day_of_month,
                                                           utc=True)
                        date_filter_end = pd.to_datetime(now, utc=True)

                    elif filter_type == 'last_month':
                        first_day_current_month = now.replace(day=1,
                                                              hour=0,
                                                              minute=0,
                                                              second=0,
                                                              microsecond=0)
                        last_day_last_month = first_day_current_month - timedelta(
                            days=1)
                        first_day_last_month = last_day_last_month.replace(
                            day=1, hour=0, minute=0, second=0, microsecond=0)

                        date_filter_start = pd.to_datetime(
                            first_day_last_month, utc=True)
                        date_filter_end = pd.to_datetime(
                            last_day_last_month.replace(hour=23,
                                                        minute=59,
                                                        second=59),
                            utc=True)

                    elif filter_type == 'current_week':
                        days_since_monday = now.weekday()
                        monday_this_week = now - timedelta(
                            days=days_since_monday)
                        monday_this_week = monday_this_week.replace(
                            hour=0, minute=0, second=0, microsecond=0)

                        date_filter_start = pd.to_datetime(monday_this_week,
                                                           utc=True)
                        date_filter_end = pd.to_datetime(now, utc=True)

            except Exception as filter_error:
                logger.warning(f"Error applying date filter: {filter_error}")

            # Apply date filter to leads if configured
            if date_filter_start and date_filter_end:
                if 'criado_em' in all_leads.columns:
                    all_leads['criado_em'] = pd.to_datetime(
                        all_leads['criado_em'], errors='coerce', utc=True)
                    all_leads = all_leads[
                        (all_leads['criado_em'] >= date_filter_start)
                        & (all_leads['criado_em'] <= date_filter_end)]
                    logger.info(
                        f"Filtered leads to {len(all_leads)} records within date range"
                    )

            # Calcular cada métrica dinâmica
            for metric in dynamic_metrics:
                try:
                    metric_id = metric['id']
                    pipeline_stage_id = metric['pipeline_stage_id']
                    valor_minimo = metric['valor_minimo']

                    logger.info(
                        f"Calculating metric {metric_id}: stage {pipeline_stage_id}, minimum {valor_minimo}"
                    )

                    # Contar leads que atingiram a etapa especificada
                    leads_count = 0
                    if not all_leads.empty and 'status_id' in all_leads.columns:
                        leads_that_reached_stage = all_leads[
                            all_leads['status_id'] == pipeline_stage_id]
                        leads_count = len(leads_that_reached_stage)

                    # Verificar se atingiu o valor mínimo
                    atingiu_meta = leads_count >= valor_minimo

                    logger.info(
                        f"Metric {metric_id}: {leads_count} leads reached stage {pipeline_stage_id}, target: {valor_minimo}, achieved: {atingiu_meta}"
                    )

                    # Preparar dados para salvar
                    current_time = datetime.now().isoformat()

                    # Usar periodo_referencia se disponível na tabela de filtros
                    periodo_referencia = "periodo_atual"
                    if date_filter_start and date_filter_end:
                        periodo_referencia = f"{date_filter_start.strftime('%Y-%m-%d')} a {date_filter_end.strftime('%Y-%m-%d')}"

                    metric_result_data = {
                        'dynamic_metric_id': metric_id,
                        'company_id': company_id,
                        'valor_atual': leads_count,
                        'status': 'ativo',
                        'periodo_referencia': periodo_referencia,
                        'leads_count': leads_count,
                        'atingiu_meta': atingiu_meta,
                        'calculado_em': current_time,
                        'created_at': current_time,
                        'updated_at': current_time
                    }

                    # Verificar se já existe resultado para esta métrica
                    existing_result = self.client.table(
                        "metric_results").select("*").eq(
                            "dynamic_metric_id",
                            metric_id).eq("company_id", company_id).execute()

                    if existing_result.data:
                        # Atualizar resultado existente
                        existing_id = existing_result.data[0]['id']
                        update_data = {
                            'valor_atual': leads_count,
                            'leads_count': leads_count,
                            'atingiu_meta': atingiu_meta,
                            'calculado_em': current_time,
                            'updated_at': current_time
                        }

                        result = self.client.table("metric_results").update(
                            update_data).eq("id", existing_id).execute()

                        if hasattr(result, "error") and result.error:
                            logger.error(
                                f"Error updating metric result {existing_id}: {result.error}"
                            )
                        else:
                            logger.info(
                                f"Updated metric result for metric {metric_id}"
                            )
                    else:
                        # Inserir novo resultado
                        result = self.client.table("metric_results").insert(
                            metric_result_data).execute()

                        if hasattr(result, "error") and result.error:
                            logger.error(
                                f"Error inserting metric result for metric {metric_id}: {result.error}"
                            )
                        else:
                            logger.info(
                                f"Inserted new metric result for metric {metric_id}"
                            )

                except Exception as metric_error:
                    logger.error(
                        f"Error calculating metric {metric.get('id', 'unknown')}: {str(metric_error)}"
                    )
                    continue

            logger.info(
                f"Dynamic metrics calculation completed for company {company_id}"
            )

        except Exception as e:
            logger.error(
                f"Error calculating dynamic metrics for company {company_id}: {str(e)}"
            )

    def _calculate_rule_points(self, rule_name, rule_config, broker_leads,
                               broker_activities, all_leads, all_activities,
                               company_id, broker_id=None):
        """Calculate count for a specific rule - returns the number of occurrences, not points"""
        try:
            # Ensure datetime columns are properly converted with better error handling
            try:
                if not broker_activities.empty and 'criado_em' in broker_activities.columns:
                    broker_activities = broker_activities.copy()
                    broker_activities.loc[:, 'criado_em'] = pd.to_datetime(
                        broker_activities['criado_em'],
                        errors='coerce',
                        utc=True)

                if not broker_leads.empty:
                    broker_leads = broker_leads.copy()
                    if 'criado_em' in broker_leads.columns:
                        broker_leads.loc[:, 'criado_em'] = pd.to_datetime(
                            broker_leads['criado_em'],
                            errors='coerce',
                            utc=True)
                    if 'atualizado_em' in broker_leads.columns:
                        broker_leads.loc[:, 'atualizado_em'] = pd.to_datetime(
                            broker_leads['atualizado_em'],
                            errors='coerce',
                            utc=True)
            except Exception as date_error:
                logger.warning(
                    f"Error converting datetime columns in rule {rule_name}: {date_error}"
                )
                # Continue with original data if conversion fails

            if rule_name == "leads_visitados":
                # Leads visitados - usando mudanças de status específicas (já filtradas por data)
                if broker_activities.empty:
                    return 0

                # Verificar se a coluna lead_id existe nas atividades
                if 'lead_id' not in broker_activities.columns:
                    logger.warning(
                        f"Column 'lead_id' not found in broker_activities for rule {rule_name}"
                    )
                    return 0

                visits = broker_activities[
                    (broker_activities.get('tipo', '') == 'mudança_status')
                    & (broker_activities.get('status_novo',
                                             pd.Series()).notna())]
                unique_leads_visited = visits['lead_id'].nunique(
                ) if not visits.empty else 0
                return unique_leads_visited

            elif rule_name == "propostas_enviadas":
                # Propostas enviadas - usando mudanças para status específico ou notas (já filtradas por data)
                if broker_activities.empty:
                    return 0

                # Verificar se a coluna lead_id existe nas atividades
                if 'lead_id' not in broker_activities.columns:
                    logger.warning(
                        f"Column 'lead_id' not found in broker_activities for rule {rule_name}"
                    )
                    return 0

                try:
                    # Buscar por mudanças de status para "Proposta" ou notas contendo "proposta"
                    status_proposals = broker_activities[
                        (broker_activities.get('tipo', '') == 'mudança_status')
                        & (broker_activities.get('valor_novo', pd.Series()).
                           astype(str).str.contains(
                               'proposta', case=False, na=False))]

                    note_proposals = broker_activities[
                        (broker_activities.get('tipo', '') == 'nota_adicionada'
                         ) & (broker_activities.get('texto_mensagem',
                                                    pd.Series()).astype(str).
                              str.contains('proposta', case=False, na=False))]

                    proposal_activities = pd.concat(
                        [status_proposals, note_proposals],
                        ignore_index=True).drop_duplicates()
                    unique_proposals = proposal_activities['lead_id'].nunique(
                    ) if not proposal_activities.empty else 0
                    return unique_proposals
                except Exception as e:
                    logger.warning(
                        f"Error in propostas_enviadas calculation: {e}")
                    return 0

            elif rule_name == "vendas_realizadas":
                # Vendas realizadas - buscar atividades de mudança para status "Ganho" no período filtrado
                if broker_activities.empty:
                    # Se não há atividades, usar fallback dos leads
                    if not broker_leads.empty and 'status' in broker_leads.columns:
                        sales = broker_leads[broker_leads['status'] == 'Ganho']
                        return len(sales)
                    return 0

                # Verificar se a coluna lead_id existe nas atividades
                if 'lead_id' not in broker_activities.columns:
                    logger.warning(
                        f"Column 'lead_id' not found in broker_activities for rule {rule_name}"
                    )
                    # Usar fallback dos leads
                    if not broker_leads.empty and 'status' in broker_leads.columns:
                        sales = broker_leads[broker_leads['status'] == 'Ganho']
                        return len(sales)
                    return 0

                try:
                    # Buscar atividades de mudança de status para "Ganho" no período filtrado
                    sales_activities = broker_activities[
                        (broker_activities.get('tipo', '') == 'mudança_status')
                        & (broker_activities.get('valor_novo', pd.Series()).
                           astype(str).str.contains(
                               'ganho|won|vendido', case=False, na=False))]

                    # Se não encontrar por atividade, usar os leads com status Ganho que foram criados no período
                    if sales_activities.empty and not broker_leads.empty:
                        sales = broker_leads[broker_leads.get('status', '') ==
                                             'Ganho']
                        return len(sales)

                    unique_sales = sales_activities['lead_id'].nunique(
                    ) if not sales_activities.empty else 0
                    return unique_sales
                except Exception as e:
                    logger.warning(
                        f"Error in vendas_realizadas calculation: {e}")
                    # Fallback para leads com status Ganho
                    if not broker_leads.empty and 'status' in broker_leads.columns:
                        sales = broker_leads[broker_leads['status'] == 'Ganho']
                        return len(sales)
                    return 0

            elif rule_name == "leads_perdidos":
                # Nova lógica: leads perdidos por inatividade (27 minutos sem resposta)
                logger.info(
                    f"\n🔍 INICIANDO CÁLCULO LEADS_PERDIDOS para rule_name: {rule_name}"
                )
                logger.info(f"⚠️ AVISO: broker_activities e all_activities são IGNORADOS")
                logger.info(f"⚠️ A função fará suas próprias consultas isoladas no banco")
                
                # Usar sempre o broker_id do contexto (mais confiável)
                current_broker_id = broker_id
                
                # Fallback apenas se broker_id for None
                if current_broker_id is None:
                    # Tentar extrair das atividades do broker
                    if not broker_activities.empty and 'user_id' in broker_activities.columns:
                        user_ids = broker_activities['user_id'].dropna().unique()
                        if len(user_ids) > 0:
                            current_broker_id = user_ids[0]
                            logger.debug(f"Broker ID identificado das atividades (fallback): {current_broker_id}")
                    
                    # Tentar extrair dos leads
                    elif not broker_leads.empty and 'responsavel_id' in broker_leads.columns:
                        responsavel_ids = broker_leads['responsavel_id'].dropna().unique()
                        if len(responsavel_ids) > 0:
                            current_broker_id = responsavel_ids[0]
                            logger.debug(f"Broker ID identificado dos leads (fallback): {current_broker_id}")

                if current_broker_id is None:
                    logger.error("❌ Nenhum broker ID disponível - retornando 0")
                    return 0

                # Converter para int se necessário
                try:
                    current_broker_id = int(current_broker_id) if isinstance(current_broker_id, (str, float)) else current_broker_id
                except (ValueError, TypeError):
                    logger.error(f"Erro ao converter broker_id {current_broker_id} para int")
                    return 0

                logger.info(f"🎯 Usando broker_id: {current_broker_id} (consultas isoladas)")

                # FUNÇÃO COMPLETAMENTE ISOLADA - ignora parâmetros de DataFrames
                result = self._calculate_leads_perdidos_por_inatividade(
                    current_broker_id, None, company_id)  # None indica que será ignorado

                logger.info(f"🔥 LEADS_PERDIDOS calculado para broker {current_broker_id}: {result}")
                logger.info(f"🏁 RESULTADO FINAL LEADS_PERDIDOS: {result}")
                
                return result

            elif rule_name == "leads_descartados":
                # Antiga lógica de leads_perdidos - leads descartados por status
                if broker_activities.empty:
                    # Se não há atividades, usar fallback dos leads
                    if not broker_leads.empty and 'status' in broker_leads.columns:
                        discarded_leads = broker_leads[broker_leads['status']
                                                       == 'Perdido']
                        return len(discarded_leads)
                    return 0

                # Verificar se a coluna lead_id existe nas atividades
                if 'lead_id' not in broker_activities.columns:
                    logger.warning(
                        f"Column 'lead_id' not found in broker_activities for rule {rule_name}"
                    )
                    # Usar fallback dos leads
                    if not broker_leads.empty and 'status' in broker_leads.columns:
                        discarded_leads = broker_leads[broker_leads['status']
                                                       == 'Perdido']
                        return len(discarded_leads)
                    return 0

                try:
                    # Buscar atividades de mudança de status para "Perdido" no período filtrado
                    discarded_activities = broker_activities[(
                        broker_activities.get('tipo', '') == 'mudança_status'
                    ) & (broker_activities.get('valor_novo', pd.Series(
                    )).astype(str).str.contains(
                        'perdido|lost|fechado|cancelado', case=False, na=False)
                         )]

                    # Se não encontrar por atividade, usar os leads com status Perdido que foram criados no período
                    if discarded_activities.empty and not broker_leads.empty:
                        discarded_leads = broker_leads[broker_leads.get(
                            'status', '') == 'Perdido']
                        return len(discarded_leads)

                    unique_discarded = discarded_activities['lead_id'].nunique(
                    ) if not discarded_activities.empty else 0
                    return unique_discarded
                except Exception as e:
                    logger.warning(
                        f"Error in leads_descartados calculation: {e}")
                    # Fallback para leads com status Perdido
                    if not broker_leads.empty and 'status' in broker_leads.columns:
                        discarded_leads = broker_leads[broker_leads['status']
                                                       == 'Perdido']
                        return len(discarded_leads)
                    return 0

            # Remove legacy rules that don't exist in new schema
            elif rule_name in [
                    "leads_atualizados_mesmo_dia", "resposta_rapida_3h",
                    "todos_leads_respondidos", "cadastro_completo",
                    "acompanhamento_pos_venda", "leads_sem_interacao_24h",
                    "leads_ignorados_48h", "leads_respondidos_1h",
                    "feedbacks_positivos", "leads_respondidos_apos_18h",
                    "leads_tempo_resposta_acima_12h",
                    "leads_5_dias_sem_mudanca"
            ]:
                logger.info(
                    f"Skipping legacy rule {rule_name} - not in new schema")
                return 0
            else:
                logger.warning(f"Unknown rule: {rule_name}")
                return 0

        except Exception as e:
            logger.error(f"Error calculating rule {rule_name}: {str(e)}")
            return 0

    def _calculate_leads_perdidos_por_inatividade(self, broker_id,
                                                  all_activities, company_id):
        """
        Calcula leads perdidos por inatividade seguindo o fluxo correto:

        1. Lead é criado na etapa "Sem Contato" com responsavel_id = 0
        2. Responsável é definido via atividade mudança_responsavel (responsavel_novo)
        3. Verifica se esse responsável enviou mensagem em 27 minutos
        4. Se não enviou, há mudança_responsavel (responsavel_anterior = quem perdeu)
        5. Processo continua até alguém enviar mensagem

        Args:
            broker_id: ID do corretor
            all_activities: DataFrame com todas as atividades (IGNORADO - faz próprias consultas)
            company_id: ID da empresa

        Returns:
            int: Número de leads perdidos por inatividade
        """
        try:
            if broker_id is None:
                logger.debug(f"Broker ID is None")
                return 0

            # Converter broker_id para o tipo correto
            broker_id = int(broker_id) if isinstance(broker_id, (str, float)) else broker_id

            logger.info(f"\n=== CALCULANDO SLA PARA BROKER {broker_id} - CONSULTAS ISOLADAS ===")

            # 1. BUSCAR ETAPA "SEM CONTATO" - CONSULTA DIRETA E ISOLADA
            sem_contato_stage_id = None
            try:
                logger.debug("🔍 Buscando etapa 'Sem Contato' no banco...")
                logger.info(f"🔧 SQL QUERY 1 - Buscando etapa 'Sem Contato': SELECT stage_id, stage_name FROM stages_list WHERE company_id = '{company_id}' AND stage_name ILIKE '%sem contato%'")
                
                stages_query = self.client.table("stages_list").select("stage_id, stage_name") \
                    .eq("company_id", company_id) \
                    .ilike("stage_name", "%sem contato%")
                
                stages_result = stages_query.execute()
                
                if not stages_result.data:
                    logger.warning("⚠️ Etapa 'Sem Contato' não encontrada na empresa")
                    logger.info(f"🔧 RESULTADO SQL QUERY 1: Nenhum resultado encontrado")
                    return 0
                
                sem_contato_stage_id = stages_result.data[0]['stage_id']
                logger.info(f"✅ Etapa 'Sem Contato' encontrada: ID {sem_contato_stage_id}")
                logger.info(f"🔧 RESULTADO SQL QUERY 1: Encontrados {len(stages_result.data)} registros, usando stage_id = {sem_contato_stage_id}")

            except Exception as e:
                logger.error(f"❌ Erro ao buscar etapa 'Sem Contato': {e}")
                return 0

            # 2. BUSCAR TODOS OS LEADS DA EMPRESA QUE PASSARAM PELA ETAPA "SEM CONTATO" - CONSULTA DIRETA E FILTRADA
            all_leads_data = []
            try:
                logger.debug("🔍 Buscando leads que passaram pela etapa 'Sem Contato' no banco...")
                logger.info(f"🔧 SQL QUERY 2 - Buscando leads com filtro: SELECT id, responsavel_id, status_id, criado_em, atualizado_em FROM leads WHERE company_id = '{company_id}' AND status_id = {sem_contato_stage_id}")
                
                # Buscar leads que estão atualmente na etapa "Sem Contato"
                leads_query = self.client.table("leads").select("id, responsavel_id, status_id, criado_em, atualizado_em") \
                    .eq("company_id", company_id) \
                    .eq("status_id", sem_contato_stage_id)
                
                leads_result = leads_query.execute()
                
                if leads_result.data:
                    all_leads_data = leads_result.data
                    logger.info(f"✅ Encontrados {len(all_leads_data)} leads na etapa 'Sem Contato'")
                    logger.info(f"🔧 RESULTADO SQL QUERY 2: {len(all_leads_data)} leads retornados (filtro: status_id = {sem_contato_stage_id})")
                    
                    # Log de alguns leads para debug
                    if len(all_leads_data) > 0:
                        sample_leads = all_leads_data[:3]
                        logger.debug(f"🔧 AMOSTRA LEADS (Sem Contato): {sample_leads}")
                else:
                    logger.info("ℹ️ Nenhum lead encontrado atualmente na etapa 'Sem Contato'")
                    logger.info(f"🔧 RESULTADO SQL QUERY 2: Nenhum lead na etapa 'Sem Contato' (status_id = {sem_contato_stage_id})")
                    
                # BUSCAR TAMBÉM LEADS QUE JÁ SAÍRAM DA ETAPA "SEM CONTATO" - QUERY ADICIONAL
                logger.debug("🔍 Buscando leads que JÁ SAÍRAM da etapa 'Sem Contato' via atividades...")
                logger.info(f"🔧 SQL QUERY 2B - Buscando leads históricos: SELECT DISTINCT lead_id FROM activities WHERE company_id = '{company_id}' AND tipo = 'mudança_status' AND (status_novo = {sem_contato_stage_id} OR status_anterior = {sem_contato_stage_id})")
                
                # Buscar atividades de mudança de status que envolveram a etapa "Sem Contato"
                historical_activities_query = self.client.table("activities").select("lead_id") \
                    .eq("company_id", company_id) \
                    .eq("tipo", "mudança_status") \
                    .or_(f"status_novo.eq.{sem_contato_stage_id},status_anterior.eq.{sem_contato_stage_id}")
                
                historical_result = historical_activities_query.execute()
                
                historical_lead_ids = set()
                if historical_result.data:
                    historical_lead_ids = {str(activity['lead_id']) for activity in historical_result.data if activity.get('lead_id')}
                    logger.info(f"✅ Encontrados {len(historical_lead_ids)} leads históricos que passaram por 'Sem Contato'")
                    logger.info(f"🔧 RESULTADO SQL QUERY 2B: {len(historical_lead_ids)} lead_ids únicos retornados")
                    
                    # Buscar dados completos desses leads históricos
                    if historical_lead_ids:
                        logger.debug("🔍 Buscando dados completos dos leads históricos...")
                        historical_leads_query = self.client.table("leads").select("id, responsavel_id, status_id, criado_em, atualizado_em") \
                            .eq("company_id", company_id) \
                            .in_("id", list(historical_lead_ids))
                        
                        historical_leads_result = historical_leads_query.execute()
                        
                        if historical_leads_result.data:
                            # Combinar leads atuais + históricos, removendo duplicatas
                            current_lead_ids = {str(lead['id']) for lead in all_leads_data}
                            
                            for historical_lead in historical_leads_result.data:
                                if str(historical_lead['id']) not in current_lead_ids:
                                    all_leads_data.append(historical_lead)
                            
                            logger.info(f"✅ TOTAL COMBINADO: {len(all_leads_data)} leads únicos (atuais + históricos)")
                else:
                    logger.info("ℹ️ Nenhum lead histórico encontrado para etapa 'Sem Contato'")
                    logger.info(f"🔧 RESULTADO SQL QUERY 2B: Nenhuma atividade histórica encontrada")

                if not all_leads_data:
                    logger.warning("⚠️ Nenhum lead encontrado (atual ou histórico) relacionado à etapa 'Sem Contato'")
                    return 0

            except Exception as e:
                logger.error(f"❌ Erro ao buscar leads: {e}")
                return 0

            # 3. BUSCAR TODAS AS ATIVIDADES DA EMPRESA - CONSULTA DIRETA E COMPLETA
            all_activities_data = []
            try:
                logger.debug("🔍 Buscando TODAS as atividades relevantes da empresa no banco...")
                logger.info(f"🔧 SQL QUERY 3 - Buscando atividades: SELECT * FROM activities WHERE company_id = '{company_id}' AND tipo IN ('mudança_responsavel', 'mensagem_enviada', 'mudança_status')")
                
                activities_query = self.client.table("activities").select("*") \
                    .eq("company_id", company_id) \
                    .in_("tipo", ["mudança_responsavel", "mensagem_enviada", "mudança_status"])

                activities_result = activities_query.execute()

                if activities_result.data:
                    all_activities_data = activities_result.data
                    logger.info(f"✅ Encontradas {len(all_activities_data)} atividades relevantes")
                    logger.info(f"🔧 RESULTADO SQL QUERY 3: {len(all_activities_data)} atividades retornadas")
                    
                    # Contar por tipo para debug
                    tipos_count = {}
                    for activity in all_activities_data:
                        tipo = activity.get('tipo', 'desconhecido')
                        tipos_count[tipo] = tipos_count.get(tipo, 0) + 1
                    
                    logger.debug(f"🔧 TIPOS DE ATIVIDADE ENCONTRADAS: {tipos_count}")
                    
                    # Log de algumas atividades para debug
                    if len(all_activities_data) > 0:
                        sample_activities = all_activities_data[:2]
                        for i, activity in enumerate(sample_activities):
                            logger.debug(f"🔧 AMOSTRA ATIVIDADE {i+1}: lead_id={activity.get('lead_id')}, tipo={activity.get('tipo')}, user_id={activity.get('user_id')}")
                            
                else:
                    logger.warning("⚠️ Nenhuma atividade relevante encontrada")
                    logger.info(f"🔧 RESULTADO SQL QUERY 3: Nenhuma atividade encontrada")
                    return 0

            except Exception as e:
                logger.error(f"❌ Erro ao buscar atividades: {e}")
                return 0

            # 4. CONVERTER PARA DATAFRAME E PROCESSAR DATAS
            try:
                activities_df = pd.DataFrame(all_activities_data)
                
                # Converter datas com tratamento de erro
                if 'criado_em' in activities_df.columns:
                    activities_df['criado_em'] = pd.to_datetime(
                        activities_df['criado_em'], errors='coerce', utc=True)

                # Verificar se temos dados válidos
                if activities_df.empty or 'lead_id' not in activities_df.columns:
                    logger.warning("⚠️ DataFrame de atividades vazio ou sem coluna lead_id")
                    return 0

                logger.debug(f"📊 DataFrame preparado: {len(activities_df)} atividades processáveis")

            except Exception as e:
                logger.error(f"❌ Erro ao processar DataFrame de atividades: {e}")
                return 0

            # 5. IDENTIFICAR TODOS OS LEADS ÚNICOS QUE PRECISAM SER ANALISADOS
            unique_lead_ids = set()
            
            try:
                # Leads com atividades relevantes
                lead_ids_from_activities = activities_df['lead_id'].dropna().astype(str).unique()
                for lead_id in lead_ids_from_activities:
                    unique_lead_ids.add(str(lead_id))
                
                # Leads que estão atualmente em "Sem Contato"
                leads_sem_contato = [lead for lead in all_leads_data if lead.get('status_id') == sem_contato_stage_id]
                for lead in leads_sem_contato:
                    unique_lead_ids.add(str(lead['id']))
                
                # Leads que já passaram por "Sem Contato" (buscar por atividades de mudança de status)
                status_activities = activities_df[
                    (activities_df['tipo'] == 'mudança_status') & 
                    ((activities_df['status_novo'] == sem_contato_stage_id) | 
                     (activities_df['status_anterior'] == sem_contato_stage_id))
                ]
                for lead_id in status_activities['lead_id'].dropna().astype(str).unique():
                    unique_lead_ids.add(str(lead_id))

                logger.info(f"🎯 Total de leads únicos para análise: {len(unique_lead_ids)}")

            except Exception as e:
                logger.error(f"❌ Erro ao identificar leads únicos: {e}")
                return 0

            # 6. PROCESSAR TIMELINE DE CADA LEAD INDIVIDUALMENTE
            leads_perdidos_count = 0
            
            logger.info(f"🔧 INICIANDO ANÁLISE INDIVIDUAL DE {len(unique_lead_ids)} LEADS")
            logger.info(f"🔧 FILTRO APLICADO: broker_id = {broker_id}, sem_contato_stage_id = {sem_contato_stage_id}")
            
            for i, lead_id in enumerate(unique_lead_ids):
                try:
                    logger.debug(f"📋 [{i+1}/{len(unique_lead_ids)}] Processando lead {lead_id}")
                    
                    # Filtrar atividades específicas deste lead
                    lead_activities = activities_df[
                        activities_df['lead_id'].astype(str) == str(lead_id)
                    ].sort_values('criado_em')
                    
                    if lead_activities.empty:
                        logger.debug(f"   ⚠️ Lead {lead_id}: sem atividades relevantes")
                        continue
                    
                    logger.debug(f"   🔧 Lead {lead_id}: {len(lead_activities)} atividades encontradas")
                    
                    # Log das atividades deste lead para debug
                    for idx, (_, activity) in enumerate(lead_activities.head(3).iterrows()):
                        logger.debug(f"      [{idx+1}] {activity.get('criado_em')} - {activity.get('tipo')} - user:{activity.get('user_id')}")
                    
                    # Processar timeline do lead
                    perdas_lead = self._process_lead_responsibility_timeline(
                        lead_id, lead_activities, broker_id, sem_contato_stage_id
                    )
                    
                    if perdas_lead > 0:
                        logger.info(f"   🔥 Lead {lead_id}: {perdas_lead} perdas para broker {broker_id}")
                        leads_perdidos_count += perdas_lead
                    else:
                        logger.debug(f"   ✅ Lead {lead_id}: nenhuma perda para broker {broker_id}")

                except Exception as lead_error:
                    logger.error(f"❌ Erro processando lead {lead_id}: {lead_error}")
                    continue

            logger.info(
                f"🎯 RESULTADO FINAL - Broker {broker_id}: {leads_perdidos_count} leads perdidos por inatividade"
            )
            logger.info(f"📊 Análise baseada em {len(all_leads_data)} leads e {len(all_activities_data)} atividades da empresa")
            logger.info(f"🔧 RESUMO DAS QUERIES EXECUTADAS:")
            logger.info(f"    QUERY 1: Etapa 'Sem Contato' → stage_id = {sem_contato_stage_id}")
            logger.info(f"    QUERY 2: Leads da empresa → {len(all_leads_data)} registros")
            logger.info(f"    QUERY 3: Atividades relevantes → {len(all_activities_data)} registros")
            logger.info(f"    PROCESSAMENTO: {len(unique_lead_ids)} leads únicos analisados")
            logger.info(f"    RESULTADO: {leads_perdidos_count} leads perdidos por inatividade")
            
            return leads_perdidos_count

        except Exception as e:
            logger.error(
                f"❌ ERRO GERAL ao calcular leads_perdidos_por_inatividade para broker {broker_id}: {str(e)}"
            )
            import traceback
            logger.error(f"Traceback completo: {traceback.format_exc()}")
            return 0

    def _process_lead_responsibility_timeline(self, lead_id, lead_activities, target_broker_id, sem_contato_stage_id):
        """
        Processa a timeline de responsabilidade de um lead específico seguindo o fluxo correto.
        
        Fluxo:
        1. Lead criado com responsavel_id = 0, entra na etapa "Sem Contato"
        2. Primeiro mudança_responsavel: responsavel_novo assume (responsavel_anterior = 0)
        3. Se não enviar mensagem em 27min, nova mudança_responsavel
        4. responsavel_anterior = quem perdeu, responsavel_novo = quem recebeu
        5. Continua até alguém enviar mensagem OU sair da etapa "Sem Contato"
        
        Args:
            lead_id: ID do lead
            lead_activities: DataFrame com atividades do lead
            target_broker_id: ID do broker alvo
            sem_contato_stage_id: ID da etapa "Sem Contato"
        
        Returns:
            int: quantidade de vezes que o target_broker_id perdeu este lead
        """
        try:
            from datetime import timedelta
            
            logger.debug(f"  🔄 INICIANDO timeline do lead {lead_id} para broker {target_broker_id}")
            logger.info(f"  🔧 TIMELINE QUERY - Lead {lead_id}: Analisando {len(lead_activities)} atividades")
            logger.info(f"  🔧 PARÂMETROS: target_broker_id = {target_broker_id}, sem_contato_stage_id = {sem_contato_stage_id}")
            
            perdas_do_broker = 0
            current_responsible = None
            responsibility_start_time = None
            sla_timeout_minutes = 27
            is_in_sem_contato = False
            
            # Log das atividades ordenadas para debug
            logger.debug(f"  🔧 ATIVIDADES ORDENADAS para lead {lead_id}:")
            for i, (_, activity) in enumerate(lead_activities.iterrows()):
                logger.debug(f"    [{i+1}] {activity['criado_em']} | {activity.get('tipo', 'N/A')} | user: {activity.get('user_id', 'N/A')}")
                if activity.get('tipo') == 'mudança_responsavel':
                    logger.debug(f"        └─ responsavel: {activity.get('responsavel_anterior', 'N/A')} → {activity.get('responsavel_novo', 'N/A')}")
                elif activity.get('tipo') == 'mudança_status':
                    logger.debug(f"        └─ status: {activity.get('status_anterior', 'N/A')} → {activity.get('status_novo', 'N/A')}")
            
            # Processar cada atividade em ordem cronológica
            for idx, (_, activity) in enumerate(lead_activities.iterrows()):
                activity_time = activity['criado_em']
                activity_type = activity.get('tipo', '')
                
                logger.debug(f"    📅 [{idx+1}/{len(lead_activities)}] {activity_time}: {activity_type}")
                logger.info(f"    🔧 PROCESSANDO ATIVIDADE {idx+1}: tipo={activity_type}, is_in_sem_contato={is_in_sem_contato}, current_responsible={current_responsible}")
                
                # EVENTO: Mudança de status - verificar entrada/saída de "Sem Contato"
                if activity_type == 'mudança_status':
                    status_novo = activity.get('status_novo')
                    status_anterior = activity.get('status_anterior') 
                    
                    logger.info(f"      🔧 MUDANÇA STATUS: {status_anterior} → {status_novo} (sem_contato_id: {sem_contato_stage_id})")
                    
                    # Entrada na etapa "Sem Contato"
                    if status_novo == sem_contato_stage_id:
                        is_in_sem_contato = True
                        logger.debug(f"      🚪 Lead ENTROU na etapa 'Sem Contato'")
                        logger.info(f"      🔧 ESTADO ATUALIZADO: is_in_sem_contato = True")
                        
                    # Saída da etapa "Sem Contato"
                    elif status_anterior == sem_contato_stage_id and status_novo != sem_contato_stage_id:
                        is_in_sem_contato = False
                        logger.debug(f"      🚪 Lead SAIU da etapa 'Sem Contato'")
                        logger.info(f"      🔧 ESTADO ATUALIZADO: is_in_sem_contato = False, zerando responsável")
                        # Parar contagem de SLA - lead não está mais em "Sem Contato"
                        current_responsible = None
                        responsibility_start_time = None
                
                # EVENTO: Mudança de responsável (só conta se estiver em "Sem Contato")
                elif activity_type == 'mudança_responsavel':
                    responsavel_anterior = activity.get('responsavel_anterior')
                    responsavel_novo = activity.get('responsavel_novo')
                    
                    logger.info(f"      🔧 MUDANÇA RESPONSÁVEL RAW: {responsavel_anterior} → {responsavel_novo}")
                    logger.info(f"      🔧 ESTADO ATUAL: is_in_sem_contato={is_in_sem_contato}")
                    
                    # Converter para int se necessário
                    if responsavel_anterior is not None:
                        try:
                            responsavel_anterior = int(responsavel_anterior)
                        except (ValueError, TypeError):
                            responsavel_anterior = None
                    
                    if responsavel_novo is not None:
                        try:
                            responsavel_novo = int(responsavel_novo)
                        except (ValueError, TypeError):
                            responsavel_novo = None
                    
                    logger.debug(f"      🔄 Mudança responsável: {responsavel_anterior} → {responsavel_novo}")
                    
                    if is_in_sem_contato:
                        logger.info(f"      🔧 PROCESSANDO mudança em 'Sem Contato': current={current_responsible}, anterior={responsavel_anterior}")
                        
                        # Verificar se o responsável anterior perdeu por inatividade
                        if (current_responsible is not None and 
                            responsibility_start_time is not None and
                            responsavel_anterior == current_responsible):
                            
                            time_diff_minutes = (activity_time - responsibility_start_time).total_seconds() / 60
                            logger.debug(f"      ⏱️  Tempo ativo: {time_diff_minutes:.1f} min")
                            logger.info(f"      🔧 VERIFICAÇÃO SLA: tempo={time_diff_minutes:.1f}min, limite={sla_timeout_minutes}min")
                            
                            # SLA violado (27 minutos sem resposta)
                            if time_diff_minutes >= sla_timeout_minutes:
                                if current_responsible == target_broker_id:
                                    perdas_do_broker += 1
                                    logger.debug(f"      ❌ PERDA CONFIRMADA! Broker {target_broker_id} perdeu lead {lead_id} após {time_diff_minutes:.1f} min")
                                    logger.info(f"      🔥 PERDA REGISTRADA! broker_id={target_broker_id}, total_perdas={perdas_do_broker}")
                                else:
                                    logger.debug(f"      ⚠️  Perda de outro broker ({current_responsible})")
                                    logger.info(f"      🔧 SLA violado mas não é o broker alvo (atual: {current_responsible} vs alvo: {target_broker_id})")
                            else:
                                logger.debug(f"      ✅ Mudança antes de {sla_timeout_minutes} min - SLA ok")
                                logger.info(f"      🔧 SLA OK - mudança dentro do prazo")
                        
                        # Novo responsável assume (só se não for 0)
                        if responsavel_novo and responsavel_novo != 0:
                            current_responsible = responsavel_novo
                            responsibility_start_time = activity_time
                            logger.debug(f"      👤 Novo responsável: {current_responsible} (início: {activity_time})")
                            logger.info(f"      🔧 NOVO RESPONSÁVEL DEFINIDO: {current_responsible}, início_timer={activity_time}")
                        else:
                            logger.info(f"      🔧 Responsável novo é 0 ou None - não iniciando timer")
                    else:
                        logger.info(f"      🔧 IGNORANDO mudança - lead NÃO está em 'Sem Contato'")
                
                # EVENTO: Mensagem enviada (SALVA o SLA - só se estiver em "Sem Contato")
                elif activity_type == 'mensagem_enviada':
                    user_id = activity.get('user_id')
                    
                    logger.info(f"      🔧 MENSAGEM ENVIADA: user_id={user_id}, is_in_sem_contato={is_in_sem_contato}, current_responsible={current_responsible}")
                    
                    if user_id is not None:
                        try:
                            user_id = int(user_id)
                        except (ValueError, TypeError):
                            user_id = None
                    
                    if is_in_sem_contato:
                        # Mensagem enviada pelo responsável atual
                        if (current_responsible is not None and 
                            user_id == current_responsible and
                            responsibility_start_time is not None):
                            
                            time_diff_minutes = (activity_time - responsibility_start_time).total_seconds() / 60
                            logger.debug(f"      💬 MENSAGEM do responsável {user_id} em {time_diff_minutes:.1f} min")
                            logger.debug(f"      ✅ SLA CUMPRIDO - lead salvo da perda")
                            logger.info(f"      🔧 SLA SALVO! responsável {user_id} respondeu em {time_diff_minutes:.1f}min")
                            
                            # Lead foi salvo, não haverá mais perdas por inatividade
                            current_responsible = None
                            responsibility_start_time = None
                        else:
                            logger.debug(f"      💬 Mensagem de usuário {user_id} (não é o responsável atual)")
                            logger.info(f"      🔧 Mensagem de usuário diferente do responsável (user: {user_id} vs responsável: {current_responsible})")
                    else:
                        logger.info(f"      🔧 IGNORANDO mensagem - lead NÃO está em 'Sem Contato'")
                
                else:
                    logger.debug(f"      ℹ️  Atividade tipo '{activity_type}' - não relevante para SLA")
            
            logger.debug(f"  📊 Lead {lead_id} - Perdas do broker {target_broker_id}: {perdas_do_broker}")
            logger.info(f"  🏁 TIMELINE FINALIZADA - Lead {lead_id}: {perdas_do_broker} perdas para broker {target_broker_id}")
            return perdas_do_broker
            
        except Exception as e:
            logger.error(f"❌ Erro processando timeline do lead {lead_id}: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return 0

    def _process_lead_sla_state_machine(self, lead_id, lead_activities,
                                        sem_contato_stage_id,
                                        target_broker_id):
        """
        Processa a máquina de estados SLA para um lead específico.

        Estados:
        - IDLE: Lead não está em "Sem Contato" ou sem responsável
        - COUNTING: Lead em "Sem Contato" com responsável, contando tempo
        - SAVED: Responsável enviou mensagem, SLA cumprido
        """
        try:
            logger.debug(f"\n    *** MÁQUINA DE ESTADOS - LEAD {lead_id} ***")

            perdas_broker = 0
            current_state = "IDLE"
            current_responsible = None
            clock_start_time = None

            # Ordenar atividades por timestamp
            activities_sorted = lead_activities.sort_values('criado_em')

            logger.debug(f"    Atividades ordenadas: {len(activities_sorted)}")

            for idx, (_, activity) in enumerate(activities_sorted.iterrows()):
                activity_time = activity['criado_em']
                activity_type = activity.get('tipo', '')
                user_id = activity.get('user_id')

                logger.debug(
                    f"    [{idx+1}] {activity_time} | {activity_type} | user: {user_id}"
                )

                # EVENTO: Mudança de status para "Sem Contato"
                if (activity_type == 'mudança_status' and
                        activity.get('status_novo') == sem_contato_stage_id):

                    logger.debug(f"        → Lead entrou em 'Sem Contato'")

                    # Buscar quem é o responsável atual
                    # Pode estar na mesma atividade ou precisar buscar próxima mudança de responsável
                    responsible_in_status = self._get_responsible_at_time(
                        lead_activities, activity_time)

                    if responsible_in_status:
                        current_responsible = responsible_in_status
                        current_state = "COUNTING"
                        clock_start_time = activity_time
                        logger.debug(
                            f"        → INICIOU RELÓGIO para responsável {current_responsible}"
                        )
                        logger.debug(f"        → Estado: {current_state}")

                # EVENTO: Mudança de responsável enquanto em "Sem Contato"
                elif (activity_type == 'mudança_responsável'
                      and current_state == "COUNTING"):

                    old_responsible = activity.get('responsavel_anterior')
                    new_responsible = activity.get('responsavel_novo')

                    logger.debug(
                        f"        → Mudança responsável: {old_responsible} → {new_responsible}"
                    )

                    if old_responsible == current_responsible and clock_start_time:
                        # Verificar se passou tempo suficiente (27 min)
                        time_diff_minutes = (activity_time - clock_start_time
                                             ).total_seconds() / 60
                        logger.debug(
                            f"        → Tempo decorrido: {time_diff_minutes:.1f} min"
                        )

                        if time_diff_minutes >= 27:
                            # SLA VIOLADO - contar perda se for o broker alvo
                            if old_responsible == target_broker_id:
                                perdas_broker += 1
                                logger.debug(
                                    f"        → ❌ SLA VIOLADO! Perda contabilizada para broker {target_broker_id}"
                                )
                            else:
                                logger.debug(
                                    f"        → SLA violado, mas não é o broker alvo ({target_broker_id})"
                                )
                        else:
                            logger.debug(
                                f"        → Mudança antes de 27 min - não conta como perda"
                            )

                    # REINICIAR relógio com novo responsável
                    if new_responsible:
                        current_responsible = new_responsible
                        current_state = "COUNTING"
                        clock_start_time = activity_time
                        logger.debug(
                            f"        → REINICIOU RELÓGIO para novo responsável {new_responsible}"
                        )

                # EVENTO: Mensagem enviada pelo responsável atual (SALVA SLA)
                elif (activity_type == 'mensagem_enviada'
                      and current_state == "COUNTING"
                      and user_id == current_responsible):

                    if clock_start_time:
                        time_diff_minutes = (activity_time - clock_start_time
                                             ).total_seconds() / 60
                        logger.debug(
                            f"        → ✅ MENSAGEM ENVIADA pelo responsável {user_id}"
                        )
                        logger.debug(
                            f"        → Tempo até resposta: {time_diff_minutes:.1f} min"
                        )
                        logger.debug(
                            f"        → SLA CUMPRIDO - parando relógio")

                    current_state = "SAVED"
                    clock_start_time = None

                # EVENTO: Mudança de status para fora de "Sem Contato"
                elif (activity_type == 'mudança_status' and
                      activity.get('status_anterior') == sem_contato_stage_id
                      and activity.get('status_novo') != sem_contato_stage_id):

                    logger.debug(f"        → Lead saiu de 'Sem Contato'")
                    logger.debug(
                        f"        → PARANDO todos os relógios - Estado: IDLE")
                    current_state = "IDLE"
                    current_responsible = None
                    clock_start_time = None

            # Verificar se ainda está contando no final (sem mudança final)
            if current_state == "COUNTING" and current_responsible == target_broker_id and clock_start_time:
                from datetime import datetime, timezone
                now = datetime.now(timezone.utc)
                final_time_diff = (now - clock_start_time).total_seconds() / 60

                if final_time_diff >= 27:
                    logger.debug(
                        f"        → ⏰ RELÓGIO AINDA ATIVO - {final_time_diff:.1f} min sem resposta"
                    )
                    # Poderia contar como perda, mas depende da regra de negócio
                    # Por ora, só conta perdas quando há troca explícita
                else:
                    logger.debug(
                        f"        → Relógio ativo há {final_time_diff:.1f} min - ainda dentro do prazo"
                    )

            logger.debug(
                f"    *** FIM MÁQUINA DE ESTADOS - PERDAS: {perdas_broker} ***"
            )
            return perdas_broker

        except Exception as e:
            logger.error(
                f"Erro na máquina de estados para lead {lead_id}: {e}")
            return 0

    def _get_responsible_at_time(self, lead_activities, target_time):
        """
        Busca quem era o responsável pelo lead em um momento específico.
        """
        try:
            # Buscar a mudança de responsável mais recente antes ou no momento target_time
            responsible_changes = lead_activities[
                (lead_activities['tipo'] == 'mudança_responsável')
                & (lead_activities['criado_em'] <= target_time)].sort_values(
                    'criado_em', ascending=False)

            if not responsible_changes.empty:
                latest_change = responsible_changes.iloc[0]
                return latest_change.get('responsavel_novo')

            return None

        except Exception as e:
            logger.error(
                f"Erro ao buscar responsável no tempo {target_time}: {e}")
            return None
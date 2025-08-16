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
                        leads_df_clean.loc[mask, col] = leads_df_clean.loc[
                            mask, col].astype('Int64')

            # The 'id' column in leads table is of type TEXT in SQL, but Kommo API might return it as a number
            # We need to ensure it's converted to string
            if 'id' in leads_df_clean.columns:
                leads_df_clean['id'] = leads_df_clean['id'].astype(str)

            # Convert datetime columns to ISO format
            datetime_columns = [
                'criado_em', 'atualizado_em', 'data_contato',
                'data_criacao_amocrm'
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

            logger.info(
                f"Leads upserted successfully: {len(leads_data)} records processed"
            )
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
                        f"Supabase error querying brokers: {brokers_result.error}"
                    )

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

    def initialize_broker_points(self, company_id):
        """
        Initialize broker points for all brokers in a company
        """
        try:
            logger.info(f"Initializing broker points for company {company_id}")
            
            # Get all brokers for this company
            brokers_result = self.client.table("brokers").select("*").eq("company_id", company_id).execute()
            
            if not brokers_result.data:
                logger.warning(f"No brokers found for company {company_id}")
                return
            
            # Check existing broker points
            existing_points = self.client.table("broker_points").select("id").eq("company_id", company_id).execute()
            existing_broker_ids = {point['id'] for point in existing_points.data} if existing_points.data else set()
            
            # Initialize points for brokers that don't have records yet
            points_to_insert = []
            current_time = datetime.now().isoformat()
            
            for broker in brokers_result.data:
                broker_id = broker['id']
                if broker_id not in existing_broker_ids:
                    points_to_insert.append({
                        'id': broker_id,
                        'company_id': company_id,
                        'nome': broker.get('nome', 'Unknown'),
                        'pontos': 0,
                        'leads_visitados': 0,
                        'propostas_enviadas': 0,
                        'vendas_realizadas': 0,
                        'leads_perdidos': 0,
                        'leads_descartados': 0,
                        'updated_at': current_time
                    })
            
            if points_to_insert:
                result = self.client.table("broker_points").upsert(points_to_insert, on_conflict='id').execute()
                if hasattr(result, "error") and result.error:
                    raise Exception(f"Supabase error: {result.error}")
                    
                logger.info(f"Initialized/updated broker points for {len(points_to_insert)} brokers in company {company_id}")
            else:
                logger.info(f"All brokers already have points initialized for company {company_id}")
                
        except Exception as e:
            logger.error(f"Failed to initialize broker points for company {company_id}: {str(e)}")
            raise

    def update_broker_points(self, brokers=[], leads=[], activities=[], company_id=None, stages=[]):
        """
        Alias for upsert_broker_points to maintain compatibility
        """
        return self.upsert_broker_points(brokers, leads, activities, company_id, stages)

    def upsert_broker_points(self,
                             brokers=[],
                             leads=[],
                             activities=[],
                             company_id=None,
                             stages=[]):
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

            if not isinstance(stages, pd.DataFrame):
                if isinstance(stages, list):
                    stages = pd.DataFrame(stages) if len(
                        stages) > 0 else pd.DataFrame()
                else:
                    stages = pd.DataFrame()

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

            # Use more efficient query to get existing points
            existing_points = self.client.table("broker_points").select(
                "id, pontos, leads_visitados, propostas_enviadas, vendas_realizadas, leads_perdidos, leads_descartados"
            ).eq("company_id", company_id).execute()
            points_dict = {
                point['id']: point
                for point in existing_points.data
            }

            # Initialize batch processing list
            all_broker_points = []

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
                            broker_activities, leads, activities, company_id,
                            broker_id, stages)
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
                        logger.info(
                            f"  - Mapped {rule_name}: {count} → broker_points.{field_name}"
                        )

                        # Log específico para leads_perdidos para debug
                        if rule_name == 'leads_perdidos':
                            logger.info(
                                f"  - 🔥 MAPEANDO leads_perdidos: {count} para broker {broker_name}"
                            )
                            logger.info(
                                f"  - 📋 Valor {count} será salvo na coluna {field_name} da tabela broker_points"
                            )
                            logger.info(
                                f"  - 🎯 broker_points_data['{field_name}'] = {broker_points_data[field_name]}"
                            )

                # Debug final: mostrar todos os dados que serão salvos
                logger.info(
                    f"📊 broker_points_data FINAL para {broker_name}: {broker_points_data}"
                )

                # Store data for batch processing
                all_broker_points.append(broker_points_data)

            # Batch process all broker points for better performance
            if all_broker_points:
                try:
                    # Use upsert for batch processing
                    result = self.client.table("broker_points").upsert(
                        all_broker_points, on_conflict='id').execute()

                    if hasattr(result, "error") and result.error:
                        logger.error(f"Batch upsert error: {result.error}")
                        # Fallback to individual processing if batch fails
                        for broker_data in all_broker_points:
                            try:
                                individual_result = self.client.table("broker_points").upsert(
                                    [broker_data], on_conflict='id').execute()
                                
                                if hasattr(individual_result, "error") and individual_result.error:
                                    logger.error(f"Individual upsert error for broker {broker_data['id']}: {individual_result.error}")
                            except Exception as individual_error:
                                logger.error(f"Individual processing error for broker {broker_data['id']}: {individual_error}")
                    else:
                        logger.info(f"Successfully batch processed {len(all_broker_points)} broker points")
                        
                        # Log specific info for leads_perdidos
                        perdidos_count = sum(1 for bp in all_broker_points if bp.get('leads_perdidos', 0) > 0)
                        if perdidos_count > 0:
                            logger.info(f"✅ {perdidos_count} brokers with leads_perdidos updated in batch")

                except Exception as batch_error:
                    logger.error(f"Batch processing error: {str(batch_error)}")
                    # Continue without failing the entire process

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
                    datetime.now().replace(day=1).isoformat(),  # Início do mês
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
            logger.info(
                "SLA metrics integrated into broker points calculation")

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

    def _calculate_rule_points(self,
                               rule_name,
                               rule_config,
                               broker_leads,
                               broker_activities,
                               all_leads,
                               all_activities,
                               company_id,
                               broker_id=None,
                               stages=None):
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

                visita_stage_ids = stages[stages['stage_name'].str.contains('visita', case=False)]['stage_id']

                    # Filtra as atividades onde o tipo é 'mudança_status' e status_novo corresponde a um desses stage_id
                status_visitas = broker_activities[
                    (broker_activities.get('tipo', '') == 'mudança_status') &
                    (broker_activities.get('status_novo').isin(visita_stage_ids))
                ]

                # Conta o número único de leads nessas atividades
                unique_visitas = status_visitas['lead_id'].nunique() if not status_visitas.empty else 0

                return unique_visitas

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
                    # Filtra apenas os stage_id cujo stage_name contenha 'proposta'
                    proposal_stage_ids = stages[stages['stage_name'].str.contains('proposta', case=False)]['stage_id']

                    # Filtra as atividades onde o tipo é 'mudança_status' e status_novo corresponde a um desses stage_id
                    status_proposals = broker_activities[
                        (broker_activities.get('tipo', '') == 'mudança_status') &
                        (broker_activities.get('status_novo').isin(proposal_stage_ids))
                    ]

                    # Conta o número único de leads nessas atividades
                    unique_proposals = status_proposals['lead_id'].nunique() if not status_proposals.empty else 0

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
                current_broker_id = broker_id

                # Fallback apenas se broker_id for None
                if current_broker_id is None:
                    # Tentar extrair das atividades do broker
                    if not broker_activities.empty and 'user_id' in broker_activities.columns:
                        user_ids = broker_activities['user_id'].dropna().unique()
                        if len(user_ids) > 0:
                            current_broker_id = user_ids[0]

                    # Tentar extrair dos leads
                    elif not broker_leads.empty and 'responsavel_id' in broker_leads.columns:
                        responsavel_ids = broker_leads['responsavel_id'].dropna().unique()
                        if len(responsavel_ids) > 0:
                            current_broker_id = responsavel_ids[0]

                if current_broker_id is None:
                    logger.error("❌ Nenhum broker ID disponível - retornando 0")
                    return 0

                # Converter broker_id para o tipo correto
                try:
                    current_broker_id = int(current_broker_id) if isinstance(
                        current_broker_id, (str, float)) else current_broker_id
                except (ValueError, TypeError):
                    logger.error(f"Erro ao converter broker_id {current_broker_id} para int")
                    return 0

                # Calcular usando função RPC otimizada
                result = self._calculate_leads_perdidos_por_inatividade(
                    current_broker_id, None, company_id)

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
        Calcula leads perdidos por inatividade usando função RPC otimizada do Supabase.

        Args:
            broker_id: ID do corretor
            all_activities: DataFrame (IGNORADO - usa RPC direto)
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

            logger.info(f"🔄 Executando RPC calculate_sla_leads_perdidos para broker {broker_id}")

            # Usar função RPC otimizada do Supabase com conversão de tipos
            response = self.client.rpc('calculate_sla_leads_perdidos', {
                'p_company_id': str(company_id),
                'p_broker_id': int(broker_id)
            }).execute()

            if hasattr(response, 'error') and response.error:
                logger.error(f"❌ Erro na RPC SLA: {response.error}")
                return 0

            leads_perdidos_count = response.data if response.data is not None else 0

            logger.info(f"✅ RPC finalizada - {leads_perdidos_count} leads perdidos para broker {broker_id}")

            return leads_perdidos_count

        except Exception as e:
            logger.error(f"❌ Erro no cálculo SLA para broker {broker_id}: {str(e)}")
            return 0

    def get_sla_calculation_logs(self, company_id, broker_id=None, execution_id=None, limit=100):
        """
        Busca logs detalhados do cálculo de SLA.

        Args:
            company_id: ID da empresa
            broker_id: ID do corretor (opcional)
            execution_id: ID de execução específica (opcional)
            limit: Limite de registros (padrão: 100)

        Returns:
            list: Lista de logs
        """
        try:
            query = self.client.table("sla_calculation_logs").select("*")

            if company_id:
                query = query.eq("company_id", company_id)

            if broker_id:
                query = query.eq("broker_id", broker_id)

            if execution_id:
                query = query.eq("execution_id", execution_id)

            result = query.order("created_at", desc=True).limit(limit).execute()

            if hasattr(result, "error") and result.error:
                logger.error(f"Erro ao buscar logs SLA: {result.error}")
                return []

            return result.data if result.data else []

        except Exception as e:
            logger.error(f"Erro ao buscar logs SLA: {str(e)}")
            return []

    def get_sla_execution_summary(self, company_id, execution_id):
        """
        Busca resumo de uma execução específica do cálculo SLA.

        Args:
            company_id: ID da empresa
            execution_id: ID da execução

        Returns:
            dict: Resumo da execução
        """
        try:
            logs = self.get_sla_calculation_logs(company_id, execution_id=execution_id, limit=1000)

            if not logs:
                return {}

            summary = {
                'execution_id': execution_id,
                'company_id': company_id,
                'broker_id': logs[0].get('broker_id'),
                'broker_name': logs[0].get('broker_name'),
                'total_logs': len(logs),
                'start_time': None,
                'end_time': None,
                'execution_time_ms': None,
                'leads_processed': 0,
                'leads_perdidos': 0,
                'steps': {},
                'errors': [],
                'warnings': []
            }

            for log in logs:
                step = log.get('step_name')
                level = log.get('log_level')

                # Contabilizar steps
                if step not in summary['steps']:
                    summary['steps'][step] = 0
                summary['steps'][step] += 1

                # Capturar tempos
                if step == 'INIT' and not summary['start_time']:
                    summary['start_time'] = log.get('created_at')
                elif step == 'RESULT':
                    summary['end_time'] = log.get('created_at')
                    summary['execution_time_ms'] = log.get('execution_time_ms')
                    summary['leads_processed'] = log.get('leads_processed', 0)

                    # Extrair leads perdidos do additional_data
                    additional_data = log.get('additional_data', {})
                    if isinstance(additional_data, dict):
                        summary['leads_perdidos'] = additional_data.get('leads_perdidos', 0)

                # Coletar erros e warnings
                if level == 'ERROR':
                    summary['errors'].append(log.get('message'))
                elif level == 'WARNING':
                    summary['warnings'].append(log.get('message'))

            return summary

        except Exception as e:
            logger.error(f"Erro ao gerar resumo da execução SLA: {str(e)}")
            return {}



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
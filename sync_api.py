from flask import Flask, jsonify, request
import threading
import logging
import time
import pandas as pd
from datetime import datetime, timedelta
from libs.supabase_db import SupabaseClient
from libs.kommo_api import KommoAPI
from libs.sync_manager import SyncManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Global state
sync_threads = {}
sync_status = {}
supabase = SupabaseClient()
COMPANY_LIST = []

# Configurações otimizadas para sincronização contínua respeitando 7 req/s
SYNC_CONFIG = {
    'base_interval': 180,  # 3 minutos base entre sincronizações
    'max_interval': 900,  # 15 minutos máximo
    'min_interval': 60,  # 1 minuto mínimo
    'health_check_interval': 30,  # 30 segundos para health check
    'api_rate_limit': 0.143,  # ~143ms entre requests (7 req/s com margem)
    'batch_delay': 0.0,  # Sem delay adicional - rate limiting é automático
    'max_retries': 3,
    'backoff_multiplier': 1.5
}


def load_companies():
    """Load all companies from kommo_config"""
    try:
        result = supabase.client.table("kommo_config").select("*").execute()
        return result.data if result.data else []
    except Exception as e:
        logger.error(f"Error loading companies: {e}")
        return []


def adaptive_sync_interval(company_id, last_changes):
    """Calculate adaptive sync interval based on recent activity"""
    base = SYNC_CONFIG['base_interval']

    # Se houve mudanças recentes, sincronize mais frequentemente
    if last_changes.get('total_changes', 0) > 0:
        return max(SYNC_CONFIG['min_interval'], base // 2)

    # Se não houve mudanças, aumente gradualmente o intervalo
    return min(SYNC_CONFIG['max_interval'], base)


def continuous_sync_worker(company_id, config):
    """Worker thread para sincronização contínua de uma empresa"""
    thread_id = f"sync_{company_id}"
    local_supabase = SupabaseClient()

    try:
        logger.info(f"[{company_id}] Starting continuous sync worker")

        # Inicializar componentes
        kommo_api = KommoAPI(api_config=config, supabase_client=local_supabase)
        sync_manager = SyncManager(kommo_api, local_supabase, config)

        # Status inicial
        sync_status[company_id] = {
            'status': 'initializing',
            'last_sync': None,
            'next_sync': None,
            'subdomain': config.get('subdomain', 'unknown'),
            'total_syncs': 0,
            'last_changes': {},
            'errors': 0,
            'thread_health': 'healthy'
        }

        consecutive_errors = 0
        last_changes = {}

        while sync_threads.get(company_id, {}).get('active', False):
            cycle_start = time.time()

            try:
                sync_status[company_id].update({
                    'status':
                    'syncing',
                    'last_sync_start':
                    datetime.now()
                })

                logger.info(
                    f"[{company_id}] Starting sync cycle #{sync_status[company_id]['total_syncs'] + 1}"
                )

                # Fetch data with appropriate strategy
                logger.info(f"[{company_id}] Fetching active users...")
                brokers = kommo_api.get_users(active_only=True)

                logger.info(f"[{company_id}] Fetching leads...")
                df_leads, df_stages = kommo_api.get_leads()

                logger.info(
                    f"[{company_id}] Fetching activities incrementally...")
                activities = kommo_api.get_activities(company_id=company_id)

                logger.info(f"[{company_id}] Saving SLA metrics...")
                # Save SLA metrics
                sla_metrics = local_supabase.save_sla_metrics(
                    company_id=company_id)

                # Add company_id to all DataFrames
                if brokers is not None and not brokers.empty:
                    brokers['company_id'] = company_id
                else:
                    brokers = pd.DataFrame()  # Explicitly handle as None

                if df_leads is not None and not df_leads.empty:
                    df_leads['company_id'] = company_id
                else:
                    df_leads = pd.DataFrame()  # Explicitly handle as None

                if df_stages is not None and not df_stages.empty:
                    df_stages['company_id'] = company_id
                else:
                    df_stages = pd.DataFrame()  # Explicitly handle as None

                if activities is not None and not activities.empty:
                    activities['company_id'] = company_id
                else:
                    activities = pd.DataFrame()  # Explicitly handle as None

                # Handle SLA metrics - it's returned as a list or None, not a DataFrame
                if sla_metrics is not None and len(sla_metrics) > 0:
                    # Convert to DataFrame if it's a list with data
                    sla_metrics = pd.DataFrame(sla_metrics)
                    sla_metrics['company_id'] = company_id
                else:
                    sla_metrics = pd.DataFrame()  # Explicitly handle as empty

                # Log data volumes
                logger.info(
                    f"[{company_id}] Data volumes - Brokers: {len(brokers)}, Leads: {len(df_leads)}, Activities: {len(activities)}, Stages: {len(df_stages)}"
                )

                # Log sample IDs for debugging
                if not df_leads.empty:
                    sample_lead_ids = df_leads['id'].head(5).tolist()
                    logger.info(
                        f"[{company_id}] Sample lead IDs: {sample_lead_ids}")

                if not activities.empty and 'lead_id' in activities.columns:
                    valid_activity_lead_ids = activities['lead_id'].dropna(
                    ).head(5).tolist()
                    logger.info(
                        f"[{company_id}] Sample activity lead_ids: {valid_activity_lead_ids}"
                    )

                # Incremental sync with change detection
                changes_detected = sync_manager.sync_data_incremental(
                    brokers=brokers,
                    leads=df_leads,
                    activities=activities,
                    stages=df_stages,
                    company_id=company_id)

                # Update broker points if there were changes
                if any(changes_detected.values()):
                    logger.info(
                        f"[{company_id}] Changes detected: {changes_detected}")

                    # Filter only brokers with 'Corretor' role for points calculation
                    if not brokers.empty:
                        broker_data = brokers[
                            (brokers['cargo'] == 'Corretor')
                            & (brokers['company_id'] == company_id)].copy()

                        if not broker_data.empty:
                            local_supabase.update_broker_points(
                                brokers=broker_data,
                                leads=df_leads,
                                activities=activities,
                                company_id=company_id)
                            logger.info(
                                f"[{company_id}] Broker points updated for {len(broker_data)} brokers"
                            )
                        else:
                            logger.warning(
                                f"[{company_id}] No brokers with 'Corretor' role found"
                            )
                else:
                    logger.info(
                        f"[{company_id}] No changes detected, skipping points calculation"
                    )

                # Update status
                last_changes = changes_detected
                total_changes = sum(1 for changed in changes_detected.values()
                                    if changed)
                consecutive_errors = 0  # Reset error counter on success

                sync_interval = adaptive_sync_interval(
                    company_id, {'total_changes': total_changes})
                next_sync_time = datetime.now() + timedelta(
                    seconds=sync_interval)

                sync_status[company_id].update({
                    'status':
                    'waiting',
                    'last_sync':
                    datetime.now(),
                    'next_sync':
                    next_sync_time,
                    'total_syncs':
                    sync_status[company_id]['total_syncs'] + 1,
                    'last_changes':
                    changes_detected,
                    'thread_health':
                    'healthy',
                    'last_interval':
                    sync_interval
                })

                cycle_duration = time.time() - cycle_start
                logger.info(
                    f"[{company_id}] Sync completed in {cycle_duration:.2f}s. Next sync in {sync_interval}s"
                )

                # Intelligent waiting with health checks
                wait_time = 0
                while wait_time < sync_interval and sync_threads.get(
                        company_id, {}).get('active', False):
                    time.sleep(
                        min(SYNC_CONFIG['health_check_interval'],
                            sync_interval - wait_time))
                    wait_time += SYNC_CONFIG['health_check_interval']

                    # Update health check timestamp
                    sync_status[company_id][
                        'last_health_check'] = datetime.now()

            except Exception as e:
                consecutive_errors += 1
                sync_status[company_id].update({
                    'status':
                    'error',
                    'last_error':
                    str(e),
                    'errors':
                    sync_status[company_id].get('errors', 0) + 1,
                    'thread_health':
                    'unhealthy' if consecutive_errors >= 3 else 'degraded'
                })

                logger.error(
                    f"[{company_id}] Sync error (attempt {consecutive_errors}): {e}"
                )

                # Exponential backoff for errors
                error_delay = min(
                    SYNC_CONFIG['base_interval'] *
                    (SYNC_CONFIG['backoff_multiplier']**consecutive_errors),
                    SYNC_CONFIG['max_interval'])

                # If too many consecutive errors, increase delay significantly
                if consecutive_errors >= SYNC_CONFIG['max_retries']:
                    error_delay = SYNC_CONFIG['max_interval'] * 2
                    logger.error(
                        f"[{company_id}] Too many consecutive errors, backing off for {error_delay}s"
                    )

                time.sleep(error_delay)

    except Exception as fatal_error:
        logger.critical(
            f"[{company_id}] Fatal error in sync worker: {fatal_error}")
        sync_status[company_id].update({
            'status': 'failed',
            'thread_health': 'dead',
            'fatal_error': str(fatal_error)
        })

    finally:
        logger.info(f"[{company_id}] Sync worker terminated")
        sync_status[company_id]['status'] = 'stopped'


def start_company_sync(company_id, config):
    """Start continuous sync for a specific company"""
    if company_id in sync_threads and sync_threads[company_id].get(
            'active', False):
        logger.info(f"[{company_id}] Sync already running")
        return False

    # Create and start thread
    sync_threads[company_id] = {
        'active':
        True,
        'thread':
        threading.Thread(target=continuous_sync_worker,
                         args=(company_id, config),
                         name=f"sync_worker_{company_id}",
                         daemon=True)
    }

    sync_threads[company_id]['thread'].start()
    logger.info(f"[{company_id}] Continuous sync started")
    return True


def stop_company_sync(company_id):
    """Stop continuous sync for a specific company"""
    if company_id in sync_threads:
        sync_threads[company_id]['active'] = False
        logger.info(f"[{company_id}] Sync stop requested")
        return True
    return False


def global_sync_manager():
    """Global manager that ensures all companies are continuously syncing"""
    logger.info("Starting global sync manager")

    while True:
        try:
            # Load current companies
            current_companies = load_companies()
            current_company_ids = {
                str(company['company_id'])
                for company in current_companies
            }

            # Start sync for new companies
            for company in current_companies:
                company_id = str(company['company_id'])

                # Check if sync thread is running and healthy
                if (company_id not in sync_threads
                        or not sync_threads[company_id].get('active', False)
                        or not sync_threads[company_id]['thread'].is_alive()):

                    logger.info(f"[{company_id}] Starting/restarting sync")
                    start_company_sync(company_id, company)

            # Stop sync for removed companies
            for company_id in list(sync_threads.keys()):
                if company_id not in current_company_ids:
                    logger.info(
                        f"[{company_id}] Company removed, stopping sync")
                    stop_company_sync(company_id)
                    del sync_threads[company_id]
                    if company_id in sync_status:
                        del sync_status[company_id]

            # Health check and restart unhealthy threads
            for company_id, thread_info in list(sync_threads.items()):
                if not thread_info['thread'].is_alive():
                    logger.warning(
                        f"[{company_id}] Thread died, restarting...")
                    company_config = next(
                        (c for c in current_companies
                         if str(c['company_id']) == company_id), None)
                    if company_config:
                        stop_company_sync(company_id)
                        start_company_sync(company_id, company_config)

            # Update global company list
            global COMPANY_LIST
            COMPANY_LIST = current_companies

        except Exception as e:
            logger.error(f"Error in global sync manager: {e}")

        # Wait before next management cycle
        time.sleep(SYNC_CONFIG['health_check_interval'])


@app.route('/status')
def get_status():
    """Get detailed status of all sync operations"""
    global_status = {
        'total_companies':
        len(COMPANY_LIST),
        'active_syncs':
        len([
            s for s in sync_status.values()
            if s.get('status') not in ['stopped', 'failed']
        ]),
        'healthy_threads':
        len([
            s for s in sync_status.values()
            if s.get('thread_health') == 'healthy'
        ]),
        'config':
        SYNC_CONFIG,
        'companies': {}
    }

    for company_id, status in sync_status.items():
        global_status['companies'][company_id] = {
            'status': status.get('status', 'unknown'),
            'last_sync': status.get('last_sync'),
            'next_sync': status.get('next_sync'),
            'subdomain': status.get('subdomain'),
            'total_syncs': status.get('total_syncs', 0),
            'last_changes': status.get('last_changes', {}),
            'errors': status.get('errors', 0),
            'thread_health': status.get('thread_health', 'unknown'),
            'last_health_check': status.get('last_health_check'),
            'last_interval': status.get('last_interval')
        }

    return jsonify(global_status)


@app.route('/start', methods=['POST'])
def start_global_sync():
    """Start the global sync manager (called automatically on startup)"""
    return jsonify({
        'status': 'already_running',
        'message': 'Continuous sync is always active'
    })


@app.route('/stop/<company_id>', methods=['POST'])
def stop_specific_sync(company_id):
    """Stop sync for a specific company"""
    if stop_company_sync(company_id):
        return jsonify({'status': 'stopped', 'company_id': company_id})
    else:
        return jsonify({'status': 'not_found', 'company_id': company_id}), 404


@app.route('/restart/<company_id>', methods=['POST'])
def restart_specific_sync(company_id):
    """Restart sync for a specific company"""
    # Find company config
    company_config = next(
        (c for c in COMPANY_LIST if str(c['company_id']) == company_id), None)
    if not company_config:
        return jsonify({
            'status': 'company_not_found',
            'company_id': company_id
        }), 404

    # Stop and restart
    stop_company_sync(company_id)
    if start_company_sync(company_id, company_config):
        return jsonify({'status': 'restarted', 'company_id': company_id})
    else:
        return jsonify({
            'status': 'failed_to_restart',
            'company_id': company_id
        }), 500


@app.route('/webhook', methods=['POST'])
def webhook():
    """Handle Kommo webhook requests - process all incoming messages"""
    try:
        # Get raw request data for detailed logging
        raw_data = request.get_data(as_text=True)
        content_type = request.content_type
        headers = dict(request.headers)

        logger.info(f"=== WEBHOOK RECEIVED ===")
        logger.info(f"Content-Type: {content_type}")
        logger.info(f"Headers: {headers}")
        logger.info(f"Raw data (first 500 chars): {raw_data[:500]}")

        # Handle different content types
        payload = None

        if content_type and 'application/json' in content_type:
            payload = request.get_json()
            logger.info("Parsed as JSON from content-type")
        elif content_type and 'application/x-www-form-urlencoded' in content_type:
            # Get form data
            form_data = request.form.to_dict()
            logger.info(f"Form data keys: {list(form_data.keys())}")
            logger.info(
                f"Form data sample: {dict(list(form_data.items())[:5])}")

            if 'payload' in form_data:
                import json
                payload = json.loads(form_data['payload'])
                logger.info("Parsed JSON from 'payload' form field")
            else:
                # Kommo sends data in flat format, convert to nested structure
                payload = form_data
                logger.info("Using form data directly as payload")
        else:
            # Try to parse as JSON anyway (some webhooks don't set proper content-type)
            try:
                import json
                payload = json.loads(raw_data)
                logger.info("Parsed as JSON from raw data")
            except Exception as json_err:
                logger.error(f"Failed to parse as JSON: {json_err}")
                payload = None

        if not payload:
            logger.error(f"Could not parse webhook payload from any method")
            return jsonify({
                'status': 'error',
                'message': 'Could not parse payload'
            }), 400

        logger.info(f"Parsed payload structure: {type(payload)}")
        logger.info(
            f"Payload keys (first 10): {list(payload.keys())[:10] if isinstance(payload, dict) else 'Not a dict'}"
        )

        # Detect webhook type and format
        webhook_type = None
        webhook_data = {}

        # Check if this is Kommo flat format (form data with keys like "account[subdomain]", "message[add][0][id]")
        if isinstance(payload, dict) and any('[' in key and ']' in key
                                             for key in payload.keys()):
            logger.info("Detected Kommo flat format")

            # Parse flat format keys to extract webhook type and data
            for key, value in payload.items():
                if '[' not in key:
                    continue

                # Extract the main type (account, message, leads, etc.)
                main_type = key.split('[')[0]
                if not webhook_type:
                    webhook_type = main_type

                # Skip account-level keys for now, focus on the actual data
                if main_type == 'account':
                    continue

                # Parse nested structure from flat keys
                # Example: "message[add][0][id]" -> message.add[0].id
                parts = key.replace(main_type, '').strip('[]').split('][')
                if len(parts) >= 3:  # [add][0][field]
                    action = parts[0]  # add, update, delete
                    index = int(parts[1]) if parts[1].isdigit() else 0
                    field = parts[2]

                    # Handle nested fields like author[id]
                    if len(parts) > 3:
                        nested_field = parts[3]
                        field = f"{field}.{nested_field}"

                    # Initialize structure
                    if action not in webhook_data:
                        webhook_data[action] = []

                    # Ensure we have enough objects in the array
                    while len(webhook_data[action]) <= index:
                        webhook_data[action].append({})

                    # Set the field value, handling nested fields
                    if '.' in field:
                        main_field, sub_field = field.split('.', 1)
                        if main_field not in webhook_data[action][index]:
                            webhook_data[action][index][main_field] = {}
                        webhook_data[action][index][main_field][
                            sub_field] = value
                    else:
                        webhook_data[action][index][field] = value

            logger.info(f"Parsed webhook type: {webhook_type}")
            logger.info(
                f"Parsed webhook data structure: {list(webhook_data.keys())}")

        else:
            # Standard format
            webhook_type = next(iter(payload.keys()))
            webhook_data = payload[webhook_type]
            logger.info(f"Standard format - Webhook type: {webhook_type}")

        if not webhook_type:
            logger.error("Could not determine webhook type")
            return jsonify({
                'status': 'error',
                'message': 'Could not determine webhook type'
            }), 400

        logger.info(f"Final webhook type: {webhook_type}")
        logger.info(f"Final webhook data: {webhook_data}")

        # Extract data objects
        data_objects = []

        if isinstance(webhook_data, dict):
            if 'add' in webhook_data:
                data_objects = webhook_data['add']
                logger.info(
                    f"Found 'add' data with {len(data_objects)} objects")
            elif 'update' in webhook_data:
                data_objects = webhook_data['update']
                logger.info(
                    f"Found 'update' data with {len(data_objects)} objects")
            elif 'delete' in webhook_data:
                data_objects = webhook_data['delete']
                logger.info(
                    f"Found 'delete' data with {len(data_objects)} objects")
            else:
                # Some webhooks might send data directly
                if isinstance(webhook_data, list):
                    data_objects = webhook_data
                    logger.info(
                        f"Using webhook data directly as object list with {len(data_objects)} objects"
                    )
                elif isinstance(webhook_data, dict):
                    data_objects = [webhook_data]
                    logger.info("Using webhook data directly as single object")
        elif isinstance(webhook_data, list):
            data_objects = webhook_data
            logger.info(
                f"Webhook data is already a list with {len(data_objects)} objects")

        if not data_objects:
            logger.warning(
                f"No data objects found in webhook. Saving raw webhook for debugging."
            )
            # Still save the webhook for debugging purposes
            webhook_record = {
                'webhook_type': webhook_type,
                'payload_id': None,
                'raw_payload': payload
            }

            try:
                result = supabase.client.table("from_webhook").insert(
                    webhook_record).execute()
                logger.info("Empty webhook saved for debugging")
            except Exception as db_err:
                logger.error(f"Failed to save empty webhook: {db_err}")

            return jsonify({
                'status': 'success',
                'message': 'No data to process, but webhook logged'
            })

        # Process the first object
        first_object = data_objects[0] if isinstance(data_objects,
                                                     list) else data_objects
        logger.info(f"Processing first object: {first_object}")

        # Extract fields for from_webhook table
        webhook_record = {
            'webhook_type':
            webhook_type,
            'payload_id':
            first_object.get('id') if isinstance(first_object, dict) else None,
            'chat_id':
            first_object.get('chat_id')
            if isinstance(first_object, dict) else None,
            'talk_id':
            first_object.get('talk_id')
            if isinstance(first_object, dict) else None,
            'contact_id':
            first_object.get('contact_id')
            if isinstance(first_object, dict) else None,
            'text':
            first_object.get('text')
            if isinstance(first_object, dict) else None,
            'created_at':
            first_object.get('created_at')
            if isinstance(first_object, dict) else None,
            'element_type':
            first_object.get('element_type')
            if isinstance(first_object, dict) else None,
            'entity_type':
            first_object.get('entity_type')
            if isinstance(first_object, dict) else None,
            'element_id':
            first_object.get('element_id')
            if isinstance(first_object, dict) else None,
            'entity_id':
            first_object.get('entity_id')
            if isinstance(first_object, dict) else None,
            'message_type':
            first_object.get('type')
            if isinstance(first_object, dict) else None,
            'origin':
            first_object.get('origin')
            if isinstance(first_object, dict) else None,
            'raw_payload':
            payload
        }

        # Extract author information if present
        if isinstance(first_object, dict):
            author = first_object.get('author', {})
            if author and isinstance(author, dict):
                webhook_record.update({
                    'author_id':
                    author.get('id'),
                    'author_type':
                    author.get('type'),
                    'author_name':
                    author.get('name'),
                    'author_avatar_url':
                    author.get('avatar_url')
                })

        logger.info(f"Prepared webhook record for database:")
        for key, value in webhook_record.items():
            if key != 'raw_payload':  # Don't log the full payload again
                logger.info(f"  {key}: {value}")

        # Link message to broker before saving
        linked_record = supabase.link_webhook_message_to_broker(webhook_record)

        # Save to database
        result = supabase.client.table("from_webhook").insert(
            linked_record).execute()

        if hasattr(result, "error") and result.error:
            logger.error(f"Error saving webhook to database: {result.error}")
            return jsonify({
                'status': 'error',
                'message': 'Database error'
            }), 500

        logger.info(f"Webhook {webhook_type} saved successfully")
        if linked_record.get('broker_id'):
            logger.info(
                f"Message linked to broker: {linked_record['broker_id']}")
        if linked_record.get('lead_id'):
            logger.info(f"Message linked to lead: {linked_record['lead_id']}")
        logger.info(f"=== WEBHOOK PROCESSING COMPLETE ===")
        return jsonify({'status': 'success'})

    except Exception as e:
        logger.error(f"Error processing webhook: {str(e)}")
        logger.error(f"Exception type: {type(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/sla-logs/<company_id>')
def get_sla_logs(company_id):
    """Endpoint para consultar logs do cálculo SLA"""
    try:
        broker_id = request.args.get('broker_id')
        execution_id = request.args.get('execution_id')
        limit = int(request.args.get('limit', 100))

        logs = supabase.get_sla_calculation_logs(
            company_id=company_id,
            broker_id=int(broker_id) if broker_id else None,
            execution_id=execution_id,
            limit=limit
        )

        return jsonify({
            'status': 'success',
            'company_id': company_id,
            'total_logs': len(logs),
            'logs': logs
        })

    except Exception as e:
        logger.error(f"Erro ao buscar logs SLA: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


@app.route('/sla-summary/<company_id>/<execution_id>')
def get_sla_execution_summary(company_id, execution_id):
    """Endpoint para consultar resumo de execução do cálculo SLA"""
    try:
        summary = supabase.get_sla_execution_summary(company_id, execution_id)

        return jsonify({
            'status': 'success',
            'summary': summary
        })

    except Exception as e:
        logger.error(f"Erro ao buscar resumo SLA: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


@app.route('/broker-points-status/<company_id>')
def get_broker_points_status(company_id):
    """Get broker points calculation status for a specific company"""
    try:
        # Check if there's an active sync for this company
        if company_id in sync_status:
            company_sync_status = sync_status[company_id]
            
            # Check if currently calculating points
            is_calculating = (
                company_sync_status.get('status') == 'syncing' or 
                company_sync_status.get('last_sync_start') and 
                not company_sync_status.get('last_sync')
            )
            
            # Get last broker points update timestamp
            points_result = supabase.client.table("broker_points").select(
                "updated_at"
            ).eq("company_id", company_id).order(
                "updated_at", desc=True
            ).limit(1).execute()
            
            last_points_update = None
            if points_result.data:
                last_points_update = points_result.data[0]['updated_at']
            
            # Count total brokers and calculated points
            brokers_result = supabase.client.table("brokers").select(
                "id"
            ).eq("company_id", company_id).execute()
            
            points_count_result = supabase.client.table("broker_points").select(
                "id"
            ).eq("company_id", company_id).execute()
            
            total_brokers = len(brokers_result.data) if brokers_result.data else 0
            calculated_points = len(points_count_result.data) if points_count_result.data else 0
            
            # Calculate completion percentage
            completion_percentage = (calculated_points / total_brokers * 100) if total_brokers > 0 else 0
            
            return jsonify({
                'status': 'calculating' if is_calculating else 'finished',
                'company_id': company_id,
                'is_calculating': is_calculating,
                'total_brokers': total_brokers,
                'calculated_points': calculated_points,
                'completion_percentage': round(completion_percentage, 2),
                'last_update': last_points_update,
                'sync_status': company_sync_status.get('status'),
                'last_sync': company_sync_status.get('last_sync'),
                'last_sync_start': company_sync_status.get('last_sync_start')
            })
        else:
            # Company not found in sync status, check database directly
            points_result = supabase.client.table("broker_points").select(
                "updated_at"
            ).eq("company_id", company_id).order(
                "updated_at", desc=True
            ).limit(1).execute()
            
            last_points_update = None
            if points_result.data:
                last_points_update = points_result.data[0]['updated_at']
            
            # Count total brokers and calculated points
            brokers_result = supabase.client.table("brokers").select(
                "id"
            ).eq("company_id", company_id).execute()
            
            points_count_result = supabase.client.table("broker_points").select(
                "id"
            ).eq("company_id", company_id).execute()
            
            total_brokers = len(brokers_result.data) if brokers_result.data else 0
            calculated_points = len(points_count_result.data) if points_count_result.data else 0
            
            # Calculate completion percentage
            completion_percentage = (calculated_points / total_brokers * 100) if total_brokers > 0 else 0
            
            return jsonify({
                'status': 'finished',
                'company_id': company_id,
                'is_calculating': False,
                'total_brokers': total_brokers,
                'calculated_points': calculated_points,
                'completion_percentage': round(completion_percentage, 2),
                'last_update': last_points_update
            })
            
    except Exception as e:
        logger.error(f"Error getting broker points status for company {company_id}: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500rs_result.data else 0
            calculated_points = len(points_count_result.data) if points_count_result.data else 0
            
            return jsonify({
                'status': 'success',
                'company_id': company_id,
                'calculation_status': {
                    'is_calculating': is_calculating,
                    'sync_status': company_sync_status.get('status', 'unknown'),
                    'last_sync': company_sync_status.get('last_sync'),
                    'last_sync_start': company_sync_status.get('last_sync_start'),
                    'total_syncs': company_sync_status.get('total_syncs', 0),
                    'errors': company_sync_status.get('errors', 0),
                    'thread_health': company_sync_status.get('thread_health', 'unknown')
                },
                'broker_points': {
                    'total_brokers': total_brokers,
                    'calculated_points': calculated_points,
                    'completion_percentage': (calculated_points / total_brokers * 100) if total_brokers > 0 else 0,
                    'last_update': last_points_update
                }
            })
        else:
            return jsonify({
                'status': 'success',
                'company_id': company_id,
                'calculation_status': {
                    'is_calculating': False,
                    'sync_status': 'not_started',
                    'message': 'No sync process found for this company'
                },
                'broker_points': {
                    'total_brokers': 0,
                    'calculated_points': 0,
                    'completion_percentage': 0,
                    'last_update': None
                }
            })
            
    except Exception as e:
        logger.error(f"Error getting broker points status for company {company_id}: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


@app.route('/reset-sync/<company_id>', methods=['POST'])
def reset_sync_timestamp(company_id):
    """Reset last_sync timestamp to start fresh sync"""
    try:
        from datetime import datetime, timezone, timedelta
        
        # Set last_sync to 7 days ago to avoid overload
        reset_date = datetime.now(timezone.utc) - timedelta(days=7)
        
        result = supabase.client.table("kommo_config").update({
            "last_sync": reset_date.isoformat()
        }).eq("company_id", company_id).eq("active", True).execute()
        
        if hasattr(result, "error") and result.error:
            return jsonify({
                'status': 'error',
                'message': f"Database error: {result.error}"
            }), 500
            
        logger.info(f"Reset last_sync for company {company_id} to {reset_date.isoformat()}")
        
        return jsonify({
            'status': 'success',
            'company_id': company_id,
            'new_last_sync': reset_date.isoformat(),
            'message': 'Sync timestamp reset successfully'
        })

    except Exception as e:
        logger.error(f"Error resetting sync timestamp: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


@app.route('/debug-events/<company_id>')
def debug_events(company_id):
    """Debug endpoint to test event fetching directly"""
    try:
        # Find company config
        company_config = next(
            (c for c in COMPANY_LIST if str(c['company_id']) == company_id), None)
        if not company_config:
            return jsonify({
                'status': 'error',
                'message': f'Company {company_id} not found'
            }), 404
            
        # Initialize API
        from libs.kommo_api import KommoAPI
        kommo_api = KommoAPI(api_config=company_config, supabase_client=supabase)
        
        # Get parameters from request
        event_type = request.args.get('type', 'outgoing_chat_message')
        from_timestamp = request.args.get('from', None)
        
        if from_timestamp:
            try:
                from_timestamp = int(from_timestamp)
            except ValueError:
                return jsonify({
                    'status': 'error',
                    'message': 'Invalid timestamp format'
                }), 400
        else:
            # Use last 24 hours
            from datetime import datetime, timezone, timedelta
            from_timestamp = int((datetime.now(timezone.utc) - timedelta(hours=24)).timestamp())
        
        # Test API call directly
        params = {
            "page": 1,
            "limit": 50,
            "filter[entity]": "lead",
            "filter[type]": event_type,
            "filter[created_at][from]": from_timestamp,
            "order[created_at]": "desc"
        }
        
        response = kommo_api._make_request("events", params=params)
        
        events_count = 0
        if response and response.get("_embedded") and response["_embedded"].get("events"):
            events_count = len(response["_embedded"]["events"])
            
        from datetime import datetime, timezone
        return jsonify({
            'status': 'success',
            'company_id': company_id,
            'event_type': event_type,
            'from_timestamp': from_timestamp,
            'from_date': datetime.fromtimestamp(from_timestamp, tz=timezone.utc).isoformat(),
            'api_url': kommo_api.api_url,
            'params': params,
            'events_found': events_count,
            'raw_response_keys': list(response.keys()) if response else None,
            'has_embedded': bool(response and response.get("_embedded")),
            'has_events': bool(response and response.get("_embedded") and response["_embedded"].get("events"))
        })
        
    except Exception as e:
        logger.error(f"Error in debug endpoint: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


if __name__ == '__main__':
    # Start global sync manager in background
    global_manager_thread = threading.Thread(target=global_sync_manager,
                                             name="global_sync_manager",
                                             daemon=True)
    global_manager_thread.start()
    logger.info("Global sync manager started")

    # Start Flask app
    logger.info("Starting Flask API server on port 5002")
    app.run(host='0.0.0.0', port=5002, debug=False, threaded=True)
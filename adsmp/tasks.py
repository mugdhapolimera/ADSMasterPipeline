from __future__ import absolute_import, unicode_literals
from past.builtins import basestring
import os
import time
import shutil
import adsputils
import math
from adsmp import app as app_module
from adsmp import solr_updater
from adsmp import templates
# from adsmp import s3_utils
from kombu import Queue
from adsmsg.msg import Msg
from sqlalchemy import create_engine, MetaData, Table, exc, insert
from sqlalchemy.orm import sessionmaker
from adsmp.models import SitemapInfo, Records
import math
from collections import defaultdict
import pdb
from sqlalchemy.orm import load_only
from celery.exceptions import Retry



# ============================= INITIALIZATION ==================================== #

proj_home = os.path.realpath(os.path.join(os.path.dirname(__file__), '../'))
app = app_module.ADSMasterPipelineCelery('master-pipeline', proj_home=proj_home, local_config=globals().get('local_config', {}))
logger = app.logger
chunked = app.chunked

app.conf.CELERY_QUEUES = (
    Queue('update-record', app.exchange, routing_key='update-record'),
    Queue('index-records', app.exchange, routing_key='index-records'),
    Queue('rebuild-index', app.exchange, routing_key='rebuild-index'),
    Queue('delete-records', app.exchange, routing_key='delete-records'),
    Queue('index-solr', app.exchange, routing_key='index-solr'),
    Queue('index-metrics', app.exchange, routing_key='index-metrics'),
    Queue('index-data-links-resolver', app.exchange, routing_key='index-data-links-resolver'),
    Queue('manage-sitemap', app.exchange, routing_key='manage-sitemap'),
    Queue('generate-single-sitemap', app.exchange, routing_key='generate-single-sitemap'),
    Queue('update-sitemap-files', app.exchange, routing_key='update-sitemap-files'),
    Queue('update-scixid', app.exchange, routing_key='update-scixid'),
    Queue('boost-request', app.exchange, routing_key='boost-request'),
    Queue('augment-record', app.exchange, routing_key='augment-record'),
)


# ============================= TASKS ============================================= #
@app.task(queue='augment-record')
def task_augment_record(msg):
    """Receives payload to augment the record.

    @param msg: protobuff that contains at minimum
        - bibcode
        - and specific payload
    """
    # logger.debug('Updating record: %s', msg)
    logger.debug('Updating record: %s', msg)
    status = app.get_msg_status(msg)
    logger.debug(f'Message status: {status}')
    type = app.get_msg_type(msg)
    logger.debug(f'Message type: {type}')
    bibcodes = []

    if status == 'active':
        # save into a database
        # passed msg may contain details on one bibcode or a list of bibcodes
        if type == 'nonbib_records':
            for m in msg.nonbib_records:
                m = Msg(m, None, None) # m is a raw protobuf, TODO: return proper instance from .nonbib_records
                bibcodes.append(m.bibcode)
                record = app.update_storage(m.bibcode, 'nonbib_data', m.toJSON())
                if record:
                    logger.debug('Saved record from list: %s', record)
        elif type == 'metrics_records':
            for m in msg.metrics_records:
                m = Msg(m, None, None)
                bibcodes.append(m.bibcode)
                record = app.update_storage(m.bibcode, 'metrics', m.toJSON(including_default_value_fields=True))
                if record:
                    logger.debug('Saved record from list: %s', record)
        elif type == 'augment':
            bibcodes.append(msg.bibcode)
            record = app.update_storage(msg.bibcode, 'augment',
                                        msg.toJSON(including_default_value_fields=True))
            if record:
                logger.debug('Saved augment message: %s', msg)
        elif type == 'classify':
            bibcodes.append(msg.bibcode)
            logger.debug(f'message to JSON: {msg.toJSON(including_default_value_fields=True)}')
            payload = msg.toJSON(including_default_value_fields=True)
            payload = payload['collections']
            record = app.update_storage(msg.bibcode, 'classify',payload)
            if record:
                logger.debug('Saved classify message: %s', msg)
        else:
            # here when record has a single bibcode
            bibcodes.append(msg.bibcode)
            record = app.update_storage(msg.bibcode, type, msg.toJSON())
            if record:
                logger.debug('Saved record: %s', record)
        if record:                        
            # Send payload to Boost pipeline
            if type != 'boost' and not app._config.get('TESTING_MODE', False):
                try:
                    task_boost_request.apply_async(args=(msg.bibcode,))
                except Exception as e:
                    app.logger.exception('Error generating boost request message for bibcode %s: %s', msg.bibcode, e)
    else:
        logger.error('Received a message with unclear status: %s', msg)

@app.task(queue='update-record')
def task_update_record(msg):
    """Receives payload to update the record.

    @param msg: protobuff that contains at minimum
        - bibcode
        - and specific payload
    """
    # logger.debug('Updating record: %s', msg)
    logger.debug('Updating record: %s', msg)
    status = app.get_msg_status(msg)
    logger.debug(f'Message status: {status}')
    type = app.get_msg_type(msg)
    logger.debug(f'Message type: {type}')
    bibcodes = []

    if status == 'deleted':
        if type == 'metadata':
            task_delete_documents(msg.bibcode)
        elif type == 'nonbib_records':
            for m in msg.nonbib_records: # TODO: this is very ugly, we are repeating ourselves...
                bibcodes.append(m.bibcode)
                record = app.update_storage(m.bibcode, 'nonbib_data', None)
                if record:
                    logger.debug('Deleted %s, result: %s', type, record)
        elif type == 'metrics_records':
            for m in msg.metrics_records:
                bibcodes.append(m.bibcode)
                record = app.update_storage(m.bibcode, 'metrics', None)
                if record:
                    logger.debug('Deleted %s, result: %s', type, record)
        else:
            bibcodes.append(msg.bibcode)
            record = app.update_storage(msg.bibcode, type, None)
            if record:
                logger.debug('Deleted %s, result: %s', type, record)

    elif status == 'active':
        # save into a database
        # passed msg may contain details on one bibcode or a list of bibcodes
        if type == 'nonbib_records':
            for m in msg.nonbib_records:
                m = Msg(m, None, None) # m is a raw protobuf, TODO: return proper instance from .nonbib_records
                bibcodes.append(m.bibcode)
                record = app.update_storage(m.bibcode, 'nonbib_data', m.toJSON())
                if record:
                    logger.debug('Saved record from list: %s', record)
                    _generate_boost_request(m, type)
        elif type == 'metrics_records':
            for m in msg.metrics_records:
                m = Msg(m, None, None)
                bibcodes.append(m.bibcode)
                record = app.update_storage(m.bibcode, 'metrics', m.toJSON(including_default_value_fields=True))
                if record:
                    logger.debug('Saved record from list: %s', record)
                    _generate_boost_request(m, type)
        elif type == 'augment':
            bibcodes.append(msg.bibcode)
            record = app.update_storage(msg.bibcode, 'augment',
                                        msg.toJSON(including_default_value_fields=True))
            if record:
                logger.debug('Saved augment message: %s', msg)
                _generate_boost_request(msg, type)
        elif type == 'classify':
            bibcodes.append(msg.bibcode)
            logger.debug(f'message to JSON: {msg.toJSON(including_default_value_fields=True)}')
            payload = msg.toJSON(including_default_value_fields=True)
            payload = payload['collections']
            record = app.update_storage(msg.bibcode, 'classify',payload)
            if record:
                logger.debug('Saved classify message: %s', msg)
                _generate_boost_request(msg, type)
        elif type == 'boost_responses':
            # Boost Pipeline answers a batched request with a list; store each
            # response the same way a single 'boost' message is stored. A record
            # that fails must not take the rest of the batch with it, so failures
            # are logged and skipped rather than aborting the loop.
            retry_queue = []
            for m in msg.boost_responses:
                m = Msg(m, None, None)
                bibcodes.append(m.bibcode)
                try:
                    record = app.update_storage(m.bibcode, 'boost',
                                                m.toJSON(including_default_value_fields=True))
                    if record:
                        logger.debug('Saved boost record from list: %s', record)
                except Exception as e:
                    logger.exception('Error saving boost record for bibcode %s: %s',
                                     m.bibcode, e)
                    retry_queue.append(m)

            # Retry the failures once; most are transient and a second attempt is
            # cheaper than losing the record until the next full boost run.
            failed = 0
            if retry_queue:
                logger.info('Retrying %d failed boost record(s)', len(retry_queue))
                for m in retry_queue:
                    try:
                        app.update_storage(m.bibcode, 'boost',
                                           m.toJSON(including_default_value_fields=True))
                    except Exception as e:
                        failed += 1
                        logger.error('Boost record failed on retry for %s: %s',
                                     m.bibcode, e)
            if failed:
                logger.error('Failed to save %d of %d boost record(s) in batch',
                             failed, len(msg.boost_responses))
        else:
            # here when record has a single bibcode
            bibcodes.append(msg.bibcode)
            record = app.update_storage(msg.bibcode, type, msg.toJSON())
            if record:
                logger.debug('Saved record: %s', record)
                _generate_boost_request(msg, type)
            if type == 'metadata':
                # with new bib data we request to augment the affiliation
                # that pipeline will eventually respond with a msg to task_update_record
                logger.debug('requesting affilation augmentation for %s', msg.bibcode)
                app.request_aff_augment(msg.bibcode)
    else:
        logger.error('Received a message with unclear status: %s', msg)

def _generate_boost_request(msg, msg_type):
    # Send payload to Boost pipeline
    if msg_type not in app._config.get('IGNORED_BOOST_PAYLOAD_TYPES', ['boost']) and not app._config.get('TESTING_MODE', False):
        try:
            task_boost_request.apply_async(args=(msg.bibcode,))
        except Exception as e:
            app.logger.exception('Error generating boost request message for bibcode %s: %s', msg.bibcode, e)
    else:
        app.logger.debug("Message for bibcode %s has type: %s, Skipping.".format(msg.bibcode, msg_type))

@app.task(queue='update-scixid')
def task_update_scixid(bibcodes, flag):
    """Receives bibcodes to add scix id to the record.

    @param 
    bibcodes: single bibcode or list of bibcodes to update
    flag: 'update' - update records to have a new scix_id if they do not already have one
          'force' - force reset scix_id and assign new scix_ids to all bibcodes
          'reset' - reset scix_id to None for all bibcodes
    """
    logger.info('Starting task_update_scixid with flag: %s for %s bibcodes', flag, len(bibcodes))
    if flag not in ['update', 'force', 'reset']:
        logger.error('task_update_scixid flag can only have the values "update" or "force"')
        return

    for bibcode in bibcodes:
        logger.debug('Processing bibcode: %s with flag: %s', bibcode, flag)

        with app.session_scope() as session:
            r = session.query(Records).filter_by(bibcode=bibcode).first()
            if r is None:
                logger.error('Bibcode %s does not exist in Records DB', bibcode)
                continue

            logger.debug('Current scix_id for %s: %s', bibcode, r.scix_id)
            logger.debug('Has bib_data: %s', bool(r.bib_data))

            if flag == 'update':
                if not r.scix_id:
                    if r.bib_data:
                        attempted_scix_id = "scix:" + app.generate_scix_id(r.bib_data)
                        r.scix_id = attempted_scix_id
                        try:
                            session.commit()
                            logger.debug('Bibcode %s has been assigned a new scix id: %s', bibcode, r.scix_id)
                        except exc.IntegrityError as e:
                            session.rollback()
                            app.log_scix_id_collision(bibcode, attempted_scix_id, e)
                    else:
                        r.scix_id = None
                        session.commit()
                        logger.debug('Bibcode %s has no bib_data, scix_id is set to None', bibcode)
                else:
                    logger.debug('Bibcode %s already has a scix id assigned: %s', bibcode, r.scix_id)

            if flag == 'force': 
                if r.bib_data:
                    old_id = r.scix_id
                    attempted_scix_id = "scix:" + app.generate_scix_id(r.bib_data)
                    r.scix_id = attempted_scix_id
                    try:
                        session.commit()
                        logger.debug('Bibcode %s scix_id changed from %s to %s', bibcode, old_id, r.scix_id)
                    except exc.IntegrityError as e:
                        session.rollback()
                        app.log_scix_id_collision(bibcode, attempted_scix_id, e)
                else:
                    r.scix_id = None
                    session.commit()
                    logger.debug('Bibcode %s has no bib_data, scix_id is set to None', bibcode)

            if flag == 'reset':
                old_id = r.scix_id
                r.scix_id = None
                session.commit()
                logger.debug('Bibcode %s scix_id reset from %s to None', bibcode, old_id)

@app.task(queue='rebuild-index')
def task_rebuild_index(bibcodes, solr_targets=None):
    """part of feature that rebuilds the entire solr index from scratch

    note that which collection to update is part of the url in solr_targets
    """
    reindex_records(bibcodes, force=True, update_solr=True, update_metrics=False, update_links=False, commit=False,
                    ignore_checksums=True, solr_targets=solr_targets, update_processed=False, priority=0)


@app.task(queue='index-records')
def task_index_records(bibcodes, force=False, update_solr=True, update_metrics=True, update_links=True, commit=False,
                       ignore_checksums=False, solr_targets=None, update_processed=True, priority=0):
    """
    Sends data to production systems: solr, metrics and resolver links
    Only does send if data has changed
    This task is (normally) called by the cronjob task
    (that one, quite obviously, is in turn started by cron)
    Use code also called by task_rebuild_index,
    """
    reindex_records(bibcodes, force=force, update_solr=update_solr, update_metrics=update_metrics, update_links=update_links, commit=commit,
                    ignore_checksums=ignore_checksums, solr_targets=solr_targets, update_processed=update_processed)


@app.task(queue='index-solr')
def task_index_solr(solr_records, solr_records_checksum, commit=False, solr_targets=None, update_processed=True):
    app.index_solr(solr_records, solr_records_checksum, solr_targets, commit=commit, update_processed=update_processed)


@app.task(queue='index-metrics')
def task_index_metrics(metrics_records, metrics_records_checksum, update_processed=True):
    # todo: create insert and update lists before queuing?
    app.index_metrics(metrics_records, metrics_records_checksum)


@app.task(queue='index-data-links-resolver')
def task_index_data_links_resolver(links_data_records, links_data_records_checksum, update_processed=True):
    app.index_datalinks(links_data_records, links_data_records_checksum, update_processed=update_processed)


def reindex_records(bibcodes, force=False, update_solr=True, update_metrics=True, update_links=True, commit=False,
                    ignore_checksums=False, solr_targets=None, update_processed=True, priority=0):
    """Receives bibcodes that need production store updated
    Receives bibcodes and checks the database if we have all the
    necessary pieces to push to production store. If not, then postpone and
    send later.
    we consider a record to be ready for solr if these pieces were updated
    (and were updated later than the last 'processed' timestamp):
        - bib_data
        - nonbib_data
        - orcid_claims
    if the force flag is true only bib_data is needed
    for solr, 'fulltext' is not considered essential; but updates to fulltext will
    trigger a solr_update (so it might happen that a document will get
    indexed twice; first with only metadata and later on incl fulltext)
    """

    if isinstance(bibcodes, basestring):
        bibcodes = [bibcodes]

    if not (update_solr or update_metrics or update_links):
        raise Exception('Hmmm, I dont think I let you do NOTHING, sorry!')

    logger.debug('Running index-records for: %s', bibcodes)
    solr_records = []
    metrics_records = []
    links_data_records = []
    solr_records_checksum = []
    metrics_records_checksum = []
    links_data_records_checksum = []
    links_url = app.conf.get('LINKS_RESOLVER_UPDATE_URL')

    if update_solr:
        fields = None  # Load all the fields since solr records grab data from almost everywhere
    else:
        # Optimization: load only fields that will be used
        fields = ['bibcode', 'augments_updated', 'bib_data_updated', 'fulltext_updated', 'metrics_updated', 'nonbib_data_updated', 'orcid_claims_updated', 'processed']
        if update_metrics:
            fields += ['metrics', 'metrics_checksum']
        if update_links:
            fields += ['nonbib_data', 'bib_data', 'datalinks_checksum']

    # check if we have complete record
    for bibcode in bibcodes:
        r = app.get_record(bibcode, load_only=fields)

        if r is None:
            logger.error('The bibcode %s doesn\'t exist!', bibcode)
            continue

        augments_updated = r.get('augments_updated', None)
        bib_data_updated = r.get('bib_data_updated', None)
        fulltext_updated = r.get('fulltext_updated', None)
        metrics_updated = r.get('metrics_updated', None)
        nonbib_data_updated = r.get('nonbib_data_updated', None)
        orcid_claims_updated = r.get('orcid_claims_updated', None)

        year_zero = '1972'
        processed = r.get('processed', adsputils.get_date(year_zero))
        if processed is None:
            processed = adsputils.get_date(year_zero)

        is_complete = all([bib_data_updated, orcid_claims_updated, nonbib_data_updated])

        if is_complete or (force is True and bib_data_updated):
            if force is False and all([
                    augments_updated and augments_updated < processed,
                    bib_data_updated and bib_data_updated < processed,
                    nonbib_data_updated and nonbib_data_updated < processed,
                    orcid_claims_updated and orcid_claims_updated < processed
                   ]):
                logger.debug('Nothing to do for %s, it was already indexed/processed', bibcode)
                continue
            if force:
                logger.debug('Forced indexing of: %s (metadata=%s, orcid=%s, nonbib=%s, fulltext=%s, metrics=%s, augments=%s)' %
                             (bibcode, bib_data_updated, orcid_claims_updated, nonbib_data_updated, fulltext_updated,
                              metrics_updated, augments_updated))
            # build the solr record
            if update_solr:
                solr_payload = solr_updater.transform_json_record(r)

                # ADS microservices assume the identifier field exists and contains the canonical bibcode:
                if 'identifier' not in solr_payload:
                    solr_payload['identifier'] = []
                if 'bibcode' in solr_payload and solr_payload['bibcode'] not in solr_payload['identifier']:
                    solr_payload['identifier'].append(solr_payload['bibcode'])
                logger.debug('Built SOLR record for %s', solr_payload['bibcode'])
                solr_checksum = app.checksum(solr_payload)
                if ignore_checksums or r.get('solr_checksum', None) != solr_checksum:
                    solr_records.append(solr_payload)
                    solr_records_checksum.append(solr_checksum)
                else:
                    logger.debug('Checksum identical, skipping solr update for: %s', bibcode)

            # get data for metrics
            if update_metrics:
                metrics_payload = r.get('metrics', None)
                metrics_checksum = app.checksum(metrics_payload or '')
                if (metrics_payload and ignore_checksums) or (metrics_payload and r.get('metrics_checksum', None) != metrics_checksum):
                    metrics_payload['bibcode'] = bibcode
                    logger.debug('Got metrics: %s', metrics_payload)
                    metrics_records.append(metrics_payload)
                    metrics_records_checksum.append(metrics_checksum)
                else:
                    logger.debug('Checksum identical or no metrics data available, skipping metrics update for: %s', bibcode)

            if update_links and links_url:
                datalinks_payload = app.generate_links_for_resolver(r)
                if datalinks_payload:
                    datalinks_checksum = app.checksum(datalinks_payload)
                    if ignore_checksums or r.get('datalinks_checksum', None) != datalinks_checksum:
                        links_data_records.append(datalinks_payload)
                        links_data_records_checksum.append(datalinks_checksum)
        else:
            # if forced and we have at least the bib data, index it
            if force is True:
                logger.warn('%s is missing bib data, even with force=True, this cannot proceed', bibcode)
            else:
                logger.debug('%s not ready for indexing yet (metadata=%s, orcid=%s, nonbib=%s, fulltext=%s, metrics=%s, augments=%s)' %
                             (bibcode, bib_data_updated, orcid_claims_updated, nonbib_data_updated, fulltext_updated,
                              metrics_updated, augments_updated))
    if solr_records:
        task_index_solr.apply_async(
            args=(solr_records, solr_records_checksum,),
            kwargs={
               'commit': commit,
               'solr_targets': solr_targets,
               'update_processed': update_processed
            }
        )
    if metrics_records:
        task_index_metrics.apply_async(
            args=(metrics_records, metrics_records_checksum,),
            kwargs={
               'update_processed': update_processed
            }
        )
    if links_data_records:
        task_index_data_links_resolver.apply_async(
            args=(links_data_records, links_data_records_checksum,),
            kwargs={
               'update_processed': update_processed
            }
        )


@app.task(queue='delete-records')
def task_delete_documents(bibcode):
    """Delete document from SOLR and from our storage.
    @param bibcode: string
    """
    logger.debug('To delete: %s', bibcode)
    app.delete_by_bibcode(bibcode)
    deleted, failed = solr_updater.delete_by_bibcodes([bibcode], app.conf['SOLR_URLS'])
    if len(failed):
        logger.error('Failed deleting documents from solr: %s', failed)
    if len(deleted):
        logger.debug('Deleted SOLR docs: %s', deleted)

    if app.metrics_delete_by_bibcode(bibcode):
        logger.debug('Deleted metrics record: %s', bibcode)
    else:
        logger.debug('Failed to deleted metrics record: %s', bibcode)

@app.task(queue='manage-sitemap')
def task_cleanup_invalid_sitemaps():
    """
    Cleanup invalid sitemap entries using efficient batch processing.
    
    This task processes all sitemap records in batches and removes those that
    no longer meet inclusion criteria based on SOLR status, bib_data availability,
    and processing staleness.
    
    Returns:
        dict: Cleanup results with removed count and processing stats
    """
    
    logger.info('Starting sitemap cleanup task')
    
    batch_size = app.conf.get('SITEMAP_BOOTSTRAP_BATCH_SIZE', 50000)
    total_removed = 0
    all_files_to_update = set()
    batch_count = 0
    total_processed = 0
    
    last_id = 0  
    while True:
        with app.session_scope() as session:
            # Get batch of sitemap records with associated Records data using keyset pagination
            batch_query = (
                session.query(
                    SitemapInfo.id,  
                    SitemapInfo.bibcode,
                    (Records.bib_data.isnot(None)).label('has_bib_data'),
                    Records.bib_data_updated,
                    Records.solr_processed,
                    Records.status
                )
                .outerjoin(Records, SitemapInfo.bibcode == Records.bibcode)
                .filter(SitemapInfo.id > last_id)  
                .order_by(SitemapInfo.id)          
                .limit(batch_size)
            )
            
            batch_records = batch_query.all()
            
            if not batch_records:
                break
                
            batch_count += 1
            total_processed += len(batch_records)
            
            # Update last_id for next iteration
            last_id = batch_records[-1].id
            
            # Check which records should be removed
            bibcodes_to_remove = []
            
            for record_data in batch_records:
                # Convert to dict for should_include_in_sitemap function
                record_dict = {
                    'bibcode': record_data.bibcode,
                    'has_bib_data': record_data.has_bib_data,
                    'bib_data_updated': record_data.bib_data_updated,
                    'solr_processed': record_data.solr_processed,
                    'status': record_data.status
                }
                
                # Check if record should still be in sitemap
                if not app.should_include_in_sitemap(record_dict):
                    bibcodes_to_remove.append(record_data.bibcode)
            
            # Remove invalid records from this batch
            if bibcodes_to_remove:
                logger.info('Batch %d: removing %d invalid records (processed %d total)', 
                           batch_count, len(bibcodes_to_remove), total_processed)
                
                # Use our existing remove action helper
                removed_count, files_to_delete, files_to_update = app._execute_remove_action(session, bibcodes_to_remove)
                session.commit()  # Commit database changes first

                app.delete_sitemap_files(files_to_delete, app.sitemap_dir)
                
                total_removed += removed_count
                all_files_to_update.update(files_to_update)
            else:
                logger.debug('Batch %d: no invalid records found (processed %d total)', 
                           batch_count, total_processed)
                
    # Synchronously flag files to regenerate (one row per file)
    flagged = 0
    if all_files_to_update:
        with app.session_scope() as flag_session:
            for filename in sorted(all_files_to_update):
                flagged += app.flag_one_row_for_filename(flag_session, filename)
                flag_session.commit()
    
    cleanup_result = {
        'total_processed': total_processed,
        'invalid_removed': total_removed,
        'batches_processed': batch_count,
        'files_regenerated': total_removed > 0,
        'files_flagged': flagged
    }
    
    logger.info('Sitemap cleanup completed: %s', cleanup_result)
    return cleanup_result

@app.task(queue='manage-sitemap') 
def task_manage_sitemap(bibcodes, action):
    """
    Executes the actions below for the given bibcodes
    
    Actions:
    - 'add': add/update record info to sitemap table if bibdata_updated is newer than filename_lastmoddate
    - 'force-update': force update sitemap table entries for given bibcodes
    - 'remove': remove bibcodes from sitemap table (TODO: not implemented)
    - 'delete-table': delete all contents of sitemap table and backup files
    - 'update-robots': force update robots.txt files for all sites
    """

    sitemap_dir = app.sitemap_dir
    if isinstance(bibcodes, basestring):
            bibcodes = [bibcodes]
    total_bibcodes = len(bibcodes)
    batch_size = app.conf.get('SITEMAP_BOOTSTRAP_BATCH_SIZE', 50000)
    total_batches = math.ceil(total_bibcodes / batch_size)

    if action == 'delete-table':
        # reset and empty all entries in sitemap table
        app.delete_contents(SitemapInfo)

        # move all sitemap files to a backup directory
        app.backup_sitemap_files(sitemap_dir)
        return

    elif action == 'remove':
    
        logger.info('Removing %d bibcodes from sitemaps using batch processing (batch size: %d)', 
                    total_bibcodes, batch_size)
        
        total_removed = 0
        all_files_to_delete = set()
        all_files_to_update = set()
        with app.session_scope() as session:
            # Process removes in batches with commit per batch
            for batch_num, batch in enumerate(chunked(bibcodes, batch_size), 1):
                logger.info('Processing remove batch %d/%d (%d bibcodes)', 
                            batch_num, total_batches, len(batch))
                
            
                removed_count, files_to_delete, files_to_update = app._execute_remove_action(session, batch)
                session.commit()  # Commit per batch 
                
                total_removed += removed_count
                all_files_to_delete.update(files_to_delete)
                all_files_to_update.update(files_to_update)
                
                logger.info('Batch %d completed: %d bibcodes removed, %d files marked for deletion', 
                        batch_num, removed_count, len(files_to_delete))
        
        # Delete all empty files after all batches are processed
        app.delete_sitemap_files(all_files_to_delete, sitemap_dir)
        
        # Synchronously flag files to regenerate (one row per file)
        flagged = 0
        with app.session_scope() as flag_session:
            for filename in sorted(all_files_to_update):
                flagged += app.flag_one_row_for_filename(flag_session, filename)
                flag_session.commit()

        logger.info('Remove operation completed: %d total bibcodes removed, %d empty files deleted, %d files flagged', 
                    total_removed, len(all_files_to_delete), flagged)
        return

    elif action == 'update-robots':
        logger.info('Force updating robots.txt files for all sites')
        success = update_robots_files(True)
        if success:
            logger.info('robots.txt files updated successfully')
        else:
            logger.error('Failed to update robots.txt files')
            raise Exception('Failed to update robots.txt files')
        return
    
    elif action == 'bootstrap':
        logger.info('Bootstrapping sitemaps for all existing records...')
        
        with app.session_scope() as session:
            # Check if SitemapInfo already has records
            existing_sitemap_count = session.query(SitemapInfo).count()
            if existing_sitemap_count > 0:
                logger.warning(f'SitemapInfo table already has {existing_sitemap_count} records')
                logger.warning('Use "force-update" action if you want to update existing sitemap records')
                return
            
            max_records_per_sitemap = app.conf.get('MAX_RECORDS_PER_SITEMAP', 10000)
            processed = 0
            successful_count = 0
            failed_count = 0
            last_id = 0
            
            # Pre-calculate sitemap filename assignments
            current_file_index = 1
            records_in_current_file = 0
            
            logger.info('Processing records in bulk batches of %d...', batch_size)
            logger.info('Max records per sitemap file: %d', max_records_per_sitemap)
            logger.info('Using keyset pagination for efficient processing of large datasets')
            
            while True:
                # Use keyset pagination instead of OFFSET/LIMIT for O(1) performance
                records_batch = (
                    session.query(Records.id, Records.bibcode, Records.bib_data_updated, 
                                Records.bib_data, Records.solr_processed, Records.status)
                    .filter(Records.id > last_id)
                    .order_by(Records.id)
                    .limit(batch_size)
                    .all()
                )
                
                if not records_batch:
                    break  # No more records
                
                # Prepare bulk insert data
                bulk_data = []
                for record in records_batch:
                    try:
                        # Apply SOLR filtering - convert record to dict for should_include_in_sitemap
                        record_dict = {
                            'bibcode': record.bibcode,
                            'has_bib_data': bool(record.bib_data),
                            'bib_data_updated': record.bib_data_updated,
                            'solr_processed': record.solr_processed,
                            'status': record.status
                        }
                        
                        # Check if record should be included in sitemap based on SOLR status
                        if not app.should_include_in_sitemap(record_dict):
                            logger.debug('Skipping %s in bootstrap: does not meet sitemap inclusion criteria', record.bibcode)
                            failed_count += 1
                            continue
                        
                        # Calculate sitemap filename
                        if records_in_current_file >= max_records_per_sitemap:
                            current_file_index += 1
                            records_in_current_file = 0
                        
                        sitemap_filename = f'sitemap_bib_{current_file_index}.xml'
                        records_in_current_file += 1
                        
                        # Create bulk insert record
                        bulk_data.append({
                            'record_id': record.id,
                            'bibcode': record.bibcode,
                            'bib_data_updated': record.bib_data_updated,
                            'scix_id': None,
                            'sitemap_filename': sitemap_filename,
                            'filename_lastmoddate': None,
                            'update_flag': True
                        })
                        successful_count += 1
                        
                    except Exception as e:
                        logger.error('Failed to prepare record %s: %s', record.bibcode, str(e))
                        failed_count += 1
                        continue
                
                # Bulk insert the entire batch 
                if bulk_data:
                    try:
                        insert_sitemap = insert(SitemapInfo)
                        session.execute(insert_sitemap, bulk_data)
                        session.commit()
                        logger.debug('Bulk inserted %d records', len(bulk_data))
                    except Exception as e:
                        logger.error('Bulk insert failed for batch: %s', str(e))
                        session.rollback()
                        failed_count += len(bulk_data)
                        successful_count -= len(bulk_data)
                
                # Update last_id for next iteration
                last_id = records_batch[-1].id
                processed += len(records_batch)
                
                # Log progress
                if processed % (batch_size * 5) == 0 or not records_batch:  # Log every 5 batches
                    logger.info('Bootstrapped %d records - %d successful, %d failed', 
                              processed, successful_count, failed_count)
            
            logger.info('Bootstrap completed: %d successful, %d failed out of %d total records', 
                       successful_count, failed_count, processed)
            logger.info('All records marked with update_flag=True')
        return
    
    elif action in ['add', 'force-update']:
        overall_successful_count = 0
        overall_failed_count = 0
        overall_new_count = 0
        overall_updated_count = 0
        all_sitemap_records = []
        
        # Use single session across all batches for state consistency
        with app.session_scope() as session:
            # Get initial state once
            current_state = app.get_current_sitemap_state(session)
            total_batches = math.ceil(total_bibcodes / batch_size)
            
            for batch_num, batch in enumerate(chunked(bibcodes, batch_size), 1):
                logger.info('Processing batch %d/%d (%d records)', 
                            batch_num, total_batches, len(batch))
                
                batch_stats, updated_state = app._process_sitemap_batch(
                    batch, action, session, current_state
                )
                
                # Commit after each batch to ensure state updates are visible
                session.commit()
                
                # Update state for next batch
                current_state = updated_state
                
                # Accumulate statistics
                overall_successful_count += batch_stats['successful']
                overall_failed_count += batch_stats['failed']
                overall_new_count += batch_stats['new_records']
                overall_updated_count += batch_stats['updated_records']
                all_sitemap_records.extend(batch_stats['sitemap_records'])
                
                logger.info('Batch %d completed: %d successful (%d new, %d updated), %d failed',
                            batch_num, batch_stats['successful'], 
                            batch_stats['new_records'], batch_stats['updated_records'],
                            batch_stats['failed'])

        logger.debug('Sitemap population completed: %d successful, %d failed out of %d total bibcodes',
                    overall_successful_count, overall_failed_count, total_bibcodes)
        logger.debug('Records breakdown: %d new, %d updated (total: %d)', 
                    overall_new_count, overall_updated_count, len(all_sitemap_records))

# TODO: Need to query github to find when above dirs are updated: https://docs.github.com/en/rest?apiVersion=2022-11-28
# TODO: Need to generate an API token from ADStailor (maybe)
def update_robots_files(force_update=False):
    """Update robots.txt files"""
    
    try:
        # Get sites configuration
        sites_config = app.conf.get('SITES', {})
        if not sites_config:
            logger.error('No SITES configuration found for robots.txt generation')
            return False
            
        sitemap_dir = app.sitemap_dir
        updated_sites = 0
        
        for site_key, site_config in sites_config.items():
            try:
                # Create site-specific directory if it doesn't exist
                site_output_dir = os.path.join(sitemap_dir, site_key)
                if not os.path.exists(site_output_dir):
                    os.makedirs(site_output_dir)
                
                robots_filepath = os.path.join(site_output_dir, 'robots.txt')
                sitemap_url = site_config.get('sitemap_url', '')
                
                # Check if robots.txt needs updating
                needs_update = force_update or not os.path.exists(robots_filepath)
                
                if not needs_update:
                    # Check if content has changed
                    try:
                        with open(robots_filepath, 'r', encoding='utf-8') as f:
                            existing_content = f.read()
                        expected_content = templates.render_robots_txt(sitemap_url)
                        needs_update = existing_content.strip() != expected_content.strip()
                    except Exception:
                        needs_update = True  # If we can't read it, recreate it
                
                if needs_update:
                    robots_content = templates.render_robots_txt(sitemap_url)
                    with open(robots_filepath, 'w', encoding='utf-8') as f:
                        f.write(robots_content)
                    
                    logger.info('Updated robots.txt for site %s at %s', 
                              site_config.get('name', site_key), robots_filepath)
                    updated_sites += 1
                else:
                    logger.debug('robots.txt for site %s is up to date', site_config.get('name', site_key))
                    
            except Exception as e:
                logger.error('Failed to update robots.txt for site %s: %s', site_key, str(e))
                continue
        
        logger.info('Robots.txt update completed: %d sites updated', updated_sites)
        return True
        
    except Exception as e:
        logger.error('Failed to update robots.txt files: %s', str(e))
        return False

def update_sitemap_index():
    """Generate sitemap index files for all configured sites"""
    
    try:
        # Get sites configuration
        sites_config = app.conf.get('SITES', {})
        if not sites_config:
            logger.error('No SITES configuration found for sitemap index generation')
            return False
            
        sitemap_dir = app.sitemap_dir
        updated_sites = 0
        
        with app.session_scope() as session:
            # Get all distinct sitemap filenames that actually have records in database
            sitemap_files = (
                session.query(SitemapInfo.sitemap_filename.distinct())
                .filter(SitemapInfo.sitemap_filename.isnot(None))
                .all()
            )
            
            # Flatten the list of tuples into a list of strings
            sitemap_filenames = [filename[0] for filename in sitemap_files] if sitemap_files else []
            
            if not sitemap_filenames:
                logger.info('No sitemap files found in database - generating empty index files')
                # Still need to generate empty index files to clear old entries
            
            for site_key, site_config in sites_config.items():
                try:
                    # Create site-specific directory if it doesn't exist
                    site_output_dir = os.path.join(sitemap_dir, site_key)
                    if not os.path.exists(site_output_dir):
                        os.makedirs(site_output_dir)
                    
                    # Build sitemap entries for this site
                    base_url = site_config.get('base_url', 'https://ui.adsabs.harvard.edu/')
                    sitemap_base_url = site_config.get('sitemap_url', base_url.rstrip('/'))
                    sitemap_entries = []
                    
                    # Add static sitemap first (if it exists)
                    static_filename = f'sitemap_static_{site_key}.xml'
                    static_template_path = os.path.join(os.path.dirname(__file__), 'templates', static_filename)
                    if os.path.exists(static_template_path):
                        # Copy static sitemap to output directory
                        static_output_path = os.path.join(site_output_dir, 'sitemap_static.xml')
                        shutil.copy2(static_template_path, static_output_path)
                        
                        # Add to index with current date
                        lastmod_date = time.strftime('%Y-%m-%d', time.gmtime())
                        entry = templates.format_sitemap_entry(sitemap_base_url, 'sitemap_static.xml', lastmod_date)
                        sitemap_entries.append(entry)
                        logger.info('Added static sitemap to index for %s', site_key)
                    
                    for filename in sitemap_filenames:
                        # Check if the actual sitemap file exists for this site
                        sitemap_filepath = os.path.join(site_output_dir, filename)
                        if os.path.exists(sitemap_filepath):
                            # Get file modification time
                            mtime = os.path.getmtime(sitemap_filepath)
                            lastmod_date = time.strftime('%Y-%m-%d', time.gmtime(mtime))
                            
                            # Create sitemap entry using proper sitemap base URL
                            entry = templates.format_sitemap_entry(sitemap_base_url, filename, lastmod_date)
                            sitemap_entries.append(entry)
                    
                    # Always generate sitemap index content, even if empty
                    index_content = templates.render_sitemap_index(''.join(sitemap_entries))
                    
                    # Write sitemap index file
                    index_filepath = os.path.join(site_output_dir, 'sitemap_index.xml')
                    with open(index_filepath, 'w', encoding='utf-8') as f:
                        f.write(index_content)
                    
                    if sitemap_entries:
                        logger.info('Generated sitemap index for site %s with %d entries at %s', 
                                  site_config.get('name', site_key), len(sitemap_entries), index_filepath)
                    else:
                        logger.info('Generated empty sitemap index for site %s at %s', 
                                  site_config.get('name', site_key), index_filepath)
                    updated_sites += 1
                        
                except Exception as e:
                    logger.error('Failed to generate sitemap index for site %s: %s', site_key, str(e))
                    continue
        
        logger.info('Sitemap index generation completed: %d sites updated', updated_sites)
        return True
        
    except Exception as e:
        logger.error('Failed to generate sitemap index files: %s', str(e))
        return False
 
@app.task(queue='generate-single-sitemap')
def task_generate_single_sitemap(sitemap_filename, record_ids):
    """Worker task: Generate a single sitemap file for all configured sites given record ids"""
    
    logger.info('Generating sitemap file: %s (%d records)', sitemap_filename, len(record_ids))
    
    try:
        # Get sites configuration
        sites_config = app.conf.get('SITES', {})
        if not sites_config:
            logger.error('No SITES configuration found')
            return False
            
        sitemap_dir = app.sitemap_dir
        
        with app.session_scope() as session:
            # Get all records for this specific file
            file_records = (
                session.query(SitemapInfo)
                .filter(SitemapInfo.id.in_(record_ids))
                .all()
            )
            
            if not file_records:
                logger.warning('No records found for sitemap file %s', sitemap_filename)
                return False
            
            # Generate file for each configured site
            successful_sites = 0
            for site_key, site_config in sites_config.items():
                try:
                    # Create site-specific directory only if it doesn't exist
                    # Ex: /app/logs/sitemap/ads/ or /app/logs/sitemap/scix/
                    site_output_dir = os.path.join(sitemap_dir, site_key) 
                    if not os.path.exists(site_output_dir):
                        os.makedirs(site_output_dir)
                    
                    # Ex: /app/logs/sitemap/ads/sitemap_bib_1.xml or /app/logs/sitemap/scix/sitemap_bib_1.xml
                    site_filepath = os.path.join(site_output_dir, sitemap_filename)
                    
                    # Ex: https://ui.adsabs.harvard.edu/abs/{bibcode}
                    # Get site-specific URL pattern
                    abs_url_pattern = site_config.get('abs_url_pattern', 'https://ui.adsabs.harvard.edu/abs/{bibcode}')
                    
                    url_entries = []
                    for info in file_records:
                        # Grab the lastmod date from the bib_data_updated field, if present
                        lastmod_date = info.bib_data_updated.date() if info.bib_data_updated else adsputils.get_date().date()
                        # Use site-specific URL pattern
                        # Ex: https://ui.adsabs.harvard.edu/abs/2023ApJ...123..456A/abstract
                        url_entry = templates.format_url_entry(info.bibcode, lastmod_date, abs_url_pattern)
                        url_entries.append(url_entry)
                    
                    # Write the site-specific XML file
                    # Ex: <url><loc>https://ui.adsabs.harvard.edu/abs/2023ApJ...123..456A/abstract</loc><lastmod>2023-01-01</lastmod></url>
                    sitemap_content = templates.render_sitemap_file(''.join(url_entries))
                    with open(site_filepath, 'w', encoding='utf-8') as file:
                        file.write(sitemap_content)
                
                    
                    logger.debug('Successfully generated %s for site %s with %d records', 
                               site_filepath, site_config.get('name', site_key), len(url_entries))
                    successful_sites += 1
                    
                except Exception as e:
                    logger.error('Failed to generate sitemap file %s for site %s: %s', 
                               sitemap_filename, site_key, str(e))
                    continue
            
            # Update database records only after all sites are processed successfully
            if successful_sites > 0:
                for info in file_records:
                    # Filename lastmoddate is updated to current date and time
                    info.filename_lastmoddate = adsputils.get_date()
                    info.update_flag = False
                
                session.commit()
                logger.info('Completed sitemap file: %s (%d records, %d sites)', 
                           sitemap_filename, len(file_records), successful_sites)
                return True
            else:
                logger.error('Failed to generate %s for any sites', sitemap_filename)
                return False
            
    except Exception as e:
        logger.error('Failed to generate sitemap file %s: %s', sitemap_filename, str(e))
        return False

@app.task(queue='update-sitemap-files', bind=True)
def task_generate_sitemap_index(self, retry_count=0):
    """Generate sitemap index files after individual sitemap files are complete
    
    This task will automatically retry if there are still pending file generation tasks,
    waiting for all files to be complete before generating the index.
    
    Uses progressive backoff: starts with 10s delays, increases to 30s after 20 retries,
    then 60s after 50 retries. Can handle up to 100 retries (~90 minutes total wait time).
    """
    # Override the default max_retries from config
    max_retries = app.conf.get('SITEMAP_INDEX_MAX_RETRIES', 100)
    self.max_retries = max_retries
    
    try:
        logger.info('Generating sitemap index files (attempt %d/%d)', retry_count + 1, max_retries)
        
        with app.session_scope() as session:
            pending_count = session.query(SitemapInfo).filter(SitemapInfo.update_flag == True).count()
            
            if pending_count > 0:
                if retry_count >= max_retries - 1:
                    logger.warning('Reached max retries (%d) with %d records still pending - generating index anyway', 
                                  max_retries, pending_count)
                    # Fall through to generate index with whatever files are complete
                else:
                    if retry_count < 20:
                        retry_delay = 10  # First 20 retries: 10s each (3m 20s)
                    elif retry_count < 50:
                        retry_delay = 30  # Next 30 retries: 30s each (15m)
                    else:
                        retry_delay = 60  # Remaining retries: 60s each (50m)
                    
                    logger.info('Found %d records still pending file generation - will retry in %ds (attempt %d/%d)', 
                               pending_count, retry_delay, retry_count + 1, max_retries)
                    raise self.retry(countdown=retry_delay, kwargs={'retry_count': retry_count + 1})
        
        success = update_sitemap_index()
        
        if success:
            logger.info('Sitemap index generation completed successfully')
        else:
            logger.error('Sitemap index generation failed')
            
        return success
        
    except Retry:
        # Re-raise Retry exceptions without logging as errors
        raise
    except Exception as e:
        logger.error('Error in sitemap index generation task: %s', str(e))
        raise

@app.task(queue='update-sitemap-files') 
def task_update_sitemap_files(previous_result=None):
    """Orchestrator task: Updates robots.txt first, then spawns parallel tasks for each sitemap file
    
    Args:
        previous_result: Result from previous task in chain (ignored, but required for chaining)
    """
    
    try:
        logger.info('Starting sitemap workflow')
        
        # Step 1: Update robots.txt files (only if necessary)
        logger.info('Updating robots.txt files...')
        success = update_robots_files(True)  # Simple direct call
        
        if not success:
            logger.warning('robots.txt update failed, but continuing with sitemap generation')
        
        # Step 2: Generate sitemap files
        logger.info('Starting sitemap file generation')
        
        # Find distinct sitemap files that need updating
        with app.session_scope() as session:
            files_needing_update_subquery = (
                session.query(SitemapInfo.sitemap_filename.distinct())
                .filter(SitemapInfo.update_flag == True)
            )
            # Get all records for all the sitemap files above (even if they do not need updating)
            all_records = (
                session.query(SitemapInfo)
                .filter(SitemapInfo.sitemap_filename.in_(files_needing_update_subquery))
                .all()
            )
            
            if not all_records:
                logger.info('No sitemap files need updating')
                # Still regenerate index to reflect current database state (in case files were removed)
                logger.info('Regenerating sitemap index to reflect current database state')
                success = update_sitemap_index()
                if success:
                    logger.info('Sitemap index regeneration completed')
                else:
                    logger.error('Sitemap index regeneration failed')
                return
            
            logger.info('Found %d records to process in sitemap files', len(all_records))
            
            # Group records by filename. Ex: {'sitemap_bib_1.xml': [1, 2, 3], 'sitemap_bib_2.xml': [4, 5, 6]}
            files_dict = defaultdict(list)
            for record in all_records:
                files_dict[record.sitemap_filename].append(record.id)  
        
        # Spawn parallel Celery tasks - one per file
        logger.info('Starting file generation for %d sitemap files (%d total records)', len(files_dict), len(all_records))

        tasks = []

        for sitemap_filename, record_ids in files_dict.items():
            # Spawn each task independently
            task_generate_single_sitemap.apply_async(args=(sitemap_filename, record_ids))
            logger.debug('Spawned task for %s with %d records', sitemap_filename, len(record_ids))
        
        # Trigger index generation immediately
        # The index task will automatically retry if files aren't ready yet
        if files_dict:
            task_generate_sitemap_index.apply_async()
            logger.info('Index generation task submitted (will wait for files to complete)')
        
        logger.info('Sitemap file generation tasks submitted')
        
    except Exception as e:
        logger.error('Error in orchestrator task: %s', str(e))
        raise

@app.task(queue='boost-request')
def task_boost_request(bibcodes):
    """Process boost requests for bibcodes (single or multiple)
    
    @param bibcodes: string or list of strings - the bibcode(s) to process
    """
    # Normalize input to always be a list
    if isinstance(bibcodes, str):
        bibcodes = [bibcodes]
    
    # Pass the entire list to generate_boost_request_message for batch processing
    result = app.generate_boost_request_message(bibcodes)
    
    logger.info('Boost requests for %s bibcode(s) sent to boost pipeline', len(bibcodes))
    
    return result


if __name__ == '__main__':
    app.start()

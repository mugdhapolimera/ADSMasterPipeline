from __future__ import absolute_import, unicode_literals
from past.builtins import basestring
import os
from itertools import chain, islice
from . import exceptions
from adsmp.models import ChangeLog, IdentifierMapping, MetricsBase, MetricsModel, Records, SitemapInfo
from adsmsg import OrcidClaims, DenormalizedRecord, FulltextUpdate, MetricsRecord, NonBibRecord, NonBibRecordList, MetricsRecordList, AugmentAffiliationResponseRecord, AugmentAffiliationRequestRecord, ClassifyRequestRecord, ClassifyRequestRecordList, ClassifyResponseRecord, ClassifyResponseRecordList, BoostRequestRecord, BoostRequestRecordList, BoostResponseRecord, BoostResponseRecordList,Status as AdsMsgStatus
from adsmsg.msg import Msg
from adsputils import ADSCelery, create_engine, sessionmaker, scoped_session, contextmanager
from sqlalchemy.orm import load_only as _load_only
from sqlalchemy import Table, bindparam, func
import adsputils
import json
from adsmp import solr_updater
from adsmp import templates
from adsputils import serializer
from sqlalchemy import exc
from multiprocessing.util import register_after_fork
import zlib
import requests
from copy import deepcopy
import sys
from sqlalchemy.dialects.postgresql import insert
import csv
from datetime import timedelta
from SciXPipelineUtils import scix_id

class ADSMasterPipelineCelery(ADSCelery):

    def __init__(self, app_name, *args, **kwargs):
        ADSCelery.__init__(self, app_name, *args, **kwargs)
        # this is used for bulk/efficient updates to metrics db
        self._metrics_engine = self._metrics_session = None
        if self._config.get('METRICS_SQLALCHEMY_URL', None):
            self._metrics_engine = create_engine(self._config.get('METRICS_SQLALCHEMY_URL', 'sqlite:///'),
                                                 echo=self._config.get('SQLALCHEMY_ECHO', False))
            _msession_factory = sessionmaker()
            self._metrics_session = scoped_session(_msession_factory)
            self._metrics_session.configure(bind=self._metrics_engine)

            MetricsBase.metadata.bind = self._metrics_engine
            self._metrics_table = Table('metrics', MetricsBase.metadata, autoload=True, autoload_with=self._metrics_engine)
            register_after_fork(self._metrics_engine, self._metrics_engine.dispose)

            insert_columns = {
                'an_refereed_citations': bindparam('an_refereed_citations', required=False),
                'an_citations': bindparam('an_citations', required=False),
                'author_num': bindparam('author_num', required=False),
                'bibcode': bindparam('bibcode'),
                'citations': bindparam('citations', required=False),
                'citation_num': bindparam('citation_num', required=False),
                'downloads': bindparam('downloads', required=False),
                'reads': bindparam('reads', required=False),
                'refereed': bindparam('refereed', required=False, value=False),
                'refereed_citations': bindparam('refereed_citations', required=False),
                'refereed_citation_num': bindparam('refereed_citation_num', required=False),
                'reference_num': bindparam('reference_num', required=False),
                'rn_citations': bindparam('rn_citations', required=False),
                'rn_citation_data': bindparam('rn_citation_data', required=False),
            }
            self._metrics_table_upsert = insert(MetricsModel).values(insert_columns)
            # on insert conflict we specify which columns update
            update_columns = {
                'an_refereed_citations': getattr(self._metrics_table_upsert.excluded, 'an_refereed_citations'),
                'an_citations': getattr(self._metrics_table_upsert.excluded, 'an_citations'),
                'author_num': getattr(self._metrics_table_upsert.excluded, 'author_num'),
                'citations': getattr(self._metrics_table_upsert.excluded, 'citations'),
                'citation_num': getattr(self._metrics_table_upsert.excluded, 'citation_num'),
                'downloads': getattr(self._metrics_table_upsert.excluded, 'downloads'),
                'reads': getattr(self._metrics_table_upsert.excluded, 'reads'),
                'refereed': getattr(self._metrics_table_upsert.excluded, 'refereed'),
                'refereed_citations': getattr(self._metrics_table_upsert.excluded, 'refereed_citations'),
                'refereed_citation_num': getattr(self._metrics_table_upsert.excluded, 'refereed_citation_num'),
                'reference_num': getattr(self._metrics_table_upsert.excluded, 'reference_num'),
                'rn_citations': getattr(self._metrics_table_upsert.excluded, 'rn_citations'),
                'rn_citation_data': getattr(self._metrics_table_upsert.excluded, 'rn_citation_data')}
            self._metrics_table_upsert = self._metrics_table_upsert.on_conflict_do_update(index_elements=['bibcode'], set_=update_columns)

    @property
    def sitemap_dir(self):
        """Get the sitemap directory from configuration"""
        return self.conf.get('SITEMAP_DIR', '/app/logs/sitemap/')

    def flag_one_row_for_filename(self, session, filename):
        """Flag exactly one sitemap row for the given filename if none already flagged.
        Returns number of rows flagged (0 or 1)."""
        if not filename:
            return 0

        already = (
            session.query(SitemapInfo.id)
            .filter(
                SitemapInfo.sitemap_filename == filename,
                SitemapInfo.update_flag.is_(True),
            )
            .limit(1)
            .scalar()
        )
        if already:
            return 0

        row_id = (
            session.query(SitemapInfo.id)
            .filter(
                SitemapInfo.sitemap_filename == filename,
                SitemapInfo.update_flag.is_(False),
            )
            .order_by(SitemapInfo.id.asc())
            .limit(1)
            .scalar()
        )
        if row_id is None:
            return 0

        return session.query(SitemapInfo).filter(
            SitemapInfo.id == row_id,
            SitemapInfo.update_flag.is_(False),
        ).update({ 'update_flag': True }, synchronize_session=False)

    def update_storage(self, bibcode, type, payload):
        """Update the document in the database, every time
        empty the solr/metrics processed timestamps.

        returns the sql record as a json object or an error string """
        if not isinstance(payload, basestring):
            payload = json.dumps(payload)

        with self.session_scope() as session:
            record = session.query(Records).filter_by(bibcode=bibcode).first()
            if record is None:
                record = Records(bibcode=bibcode)
                session.add(record)
            now = adsputils.get_date()
            oldval = None
            if type == 'metadata' or type == 'bib_data':
                oldval = record.bib_data
                record.bib_data = payload
                record.bib_data_updated = now
            elif type == 'nonbib_data':
                oldval = record.nonbib_data
                record.nonbib_data = payload
                record.nonbib_data_updated = now
            elif type == 'orcid_claims':
                oldval = record.orcid_claims
                record.orcid_claims = payload
                record.orcid_claims_updated = now
            elif type == 'fulltext':
                oldval = 'not-stored'
                record.fulltext = payload
                record.fulltext_updated = now
            elif type == 'metrics':
                oldval = 'not-stored'
                record.metrics = payload
                record.metrics_updated = now
            elif type == 'augment':
                # payload contains new value for affilation fields
                # r.augments holds a dict, save it in database
                oldval = 'not-stored'
                record.augments = payload
                record.augments_updated = now
            elif type == 'classify':
                # payload contains new value for collections field
                # r.augments holds a list, save it in database
                oldval = 'not-stored'
                record.classifications = payload
                record.classifications_updated = now
            elif type == 'boost':
                # payload contains new value for boost fields
                # r.augments holds a dict, save it in database
                oldval = 'not-stored'
                record.boost_factors = payload
                record.boost_factors_updated = now
            else:
                raise Exception('Unknown type: %s' % type)
            session.add(ChangeLog(key=bibcode, type=type, oldvalue=oldval))
            record.updated = now
            out = record.toJSON()                      
            attempted_scix_id = None
            try:
                session.flush()
                if not record.scix_id and record.bib_data:
                    attempted_scix_id = "scix:" + str(self.generate_scix_id(record.bib_data))
                    record.scix_id = attempted_scix_id
                    out = record.toJSON()
                session.commit()
                return out
            except exc.IntegrityError as e:
                session.rollback()
                if attempted_scix_id:
                    self.log_scix_id_collision(bibcode, attempted_scix_id, e)
                else:
                    self.logger.exception(
                        'IntegrityError in update_storage for bibcode %s, type %s '
                        '(not a scix_id collision)', bibcode, type)
                raise

    def generate_scix_id(self, bib_data):
        if self._config.get('SCIX_ID_GENERATION_FIELDS', None):
            user_fields = self._config.get('SCIX_ID_GENERATION_FIELDS')
        else:
            user_fields = None
        return scix_id.generate_scix_id(bib_data, user_fields = user_fields) 

    def log_scix_id_collision(self, bibcode, attempted_scix_id, original_error=None):
        """Query the database for the record that already holds attempted_scix_id
        and log both the existing and new (bibcode, scix_id) pairs so curators
        can identify which records are clashing."""
        try:
            with self.session_scope() as session:
                existing = session.query(Records).filter_by(scix_id=attempted_scix_id).first()
                if existing:
                    self.logger.error(
                        'SciX ID collision detected: '
                        'existing record in DB: (bibcode=%s, scix_id=%s), '
                        'new record that failed to commit: (bibcode=%s, scix_id=%s). '
                        'Original error: %s',
                        existing.bibcode, existing.scix_id,
                        bibcode, attempted_scix_id,
                        original_error
                    )
                else:
                    self.logger.error(
                        'IntegrityError for bibcode %s with scix_id %s, '
                        'but no existing record found with that scix_id '
                        '(possible race condition or other constraint violation). '
                        'Original error: %s',
                        bibcode, attempted_scix_id,
                        original_error
                    )
        except Exception:
            self.logger.exception(
                'Failed to query database for scix_id collision details '
                '(bibcode=%s, attempted_scix_id=%s)',
                bibcode, attempted_scix_id
            )

    def delete_by_bibcode(self, bibcode):
        with self.session_scope() as session:
            r = session.query(Records).filter_by(bibcode=bibcode).first()
            # Get affected sitemap files BEFORE deletion so we can mark them for regeneration
            affected_files = session.query(SitemapInfo.sitemap_filename)\
                               .filter_by(bibcode=bibcode)\
                               .distinct().all()
            
            if r is not None:
                
                # First delete any associated SitemapInfo records
                session.query(SitemapInfo).filter_by(bibcode=bibcode).delete(synchronize_session=False)
                # Then delete the Records entry and create ChangeLog
                session.add(ChangeLog(key='bibcode:%s' % bibcode, type='deleted', oldvalue=serializer.dumps(r.toJSON())))
                session.delete(r)
                
                # Mark affected sitemap files for regeneration synchronously (one row per file)
                for (filename,) in sorted({f for f in affected_files if f}):
                    if self.flag_one_row_for_filename(session, filename):
                        session.commit()
                
                session.commit()
                return True
            
            # Handle orphaned SitemapInfo records (if Records was already deleted)
            s = session.query(SitemapInfo).filter_by(bibcode=bibcode).first()
            if s is not None:
                # Get the filename before deleting the orphaned record
                orphaned_filename = s.sitemap_filename
                session.delete(s)
                
                # Mark the file for regeneration if it has a filename (synchronously)
                if orphaned_filename:
                    if self.flag_one_row_for_filename(session, orphaned_filename):
                        session.commit()
                
                session.commit()
                return True
            
            # Neither Records nor SitemapInfo found
            return None

    def rename_bibcode(self, old_bibcode, new_bibcode):
        assert old_bibcode and new_bibcode
        assert old_bibcode != new_bibcode

        with self.session_scope() as session:
            r = session.query(Records).filter_by(bibcode=old_bibcode).first()
            if r is not None:

                t = session.query(IdentifierMapping).filter_by(key=old_bibcode).first()
                if t is None:
                    session.add(IdentifierMapping(key=old_bibcode, target=new_bibcode))
                else:
                    while t is not None:
                        target = t.target
                        t.target = new_bibcode
                        t = session.query(IdentifierMapping).filter_by(key=target).first()

                session.add(ChangeLog(key='bibcode:%s' % new_bibcode, type='renamed', oldvalue=r.bibcode, permanent=True))
                r.bibcode = new_bibcode
                session.commit()
            else:
                self.logger.error('Rename operation, bibcode doesnt exist: old=%s, new=%s', old_bibcode, new_bibcode)

    def get_record(self, bibcode, load_only=None):
        if isinstance(bibcode, list):
            out = []
            with self.session_scope() as session:
                q = session.query(Records).filter(Records.bibcode.in_(bibcode))
                if load_only:
                    q = q.options(_load_only(*load_only))
                for r in q.all():
                    out.append(r.toJSON(load_only=load_only))
            return out
        else:
            with self.session_scope() as session:
                q = session.query(Records).filter_by(bibcode=bibcode)
                if load_only:
                    q = q.options(_load_only(*load_only))
                r = q.first()
                if r is None:
                    return None
                return r.toJSON(load_only=load_only)

    def get_changelog(self, bibcode):
        out = []
        with self.session_scope() as session:
            r = session.query(Records).filter_by(bibcode=bibcode).first()
            if r is not None:
                out.append(r.toJSON())
            to_collect = [bibcode]
            while len(to_collect):
                for x in session.query(IdentifierMapping).filter_by(key=to_collect.pop()).yield_per(100):
                    to_collect.append(x.target)
                    out.append(x.toJSON())
        return out

    def get_msg_type(self, msg):
        """Identifies the type of this supplied message.

        :param: Protobuf instance
        :return: str
        """

        if isinstance(msg, OrcidClaims):
            return 'orcid_claims'
        elif isinstance(msg, DenormalizedRecord):
            return 'metadata'
        elif isinstance(msg, FulltextUpdate):
            return 'fulltext'
        elif isinstance(msg, NonBibRecord):
            return 'nonbib_data'
        elif isinstance(msg, NonBibRecordList):
            return 'nonbib_records'
        elif isinstance(msg, MetricsRecord):
            return 'metrics'
        elif isinstance(msg, MetricsRecordList):
            return 'metrics_records'
        elif isinstance(msg, AugmentAffiliationResponseRecord):
            return 'augment'
        elif isinstance(msg, ClassifyResponseRecord):
            return 'classify'
        elif isinstance(msg, BoostResponseRecord):
            return 'boost'
        elif isinstance(msg, BoostResponseRecordList):
            return 'boost_responses'
        else:
            raise exceptions.IgnorableException('Unkwnown type {0} submitted for update'.format(repr(msg)))

    def get_msg_status(self, msg):
        """Identifies the type of this supplied message.

        :param: Protobuf instance
        :return: str
        """

        if isinstance(msg, Msg):
            status = msg.status
            if status == 1:
                return 'deleted'
            else:
                return 'active'
        else:
            return 'unknown'

    def index_solr(self, solr_docs, solr_docs_checksum, solr_urls, commit=False, update_processed=True):
        """Sends documents to solr. It will update
        the solr_processed timestamp for every document which succeeded.

        :param: solr_docs - list of json objects (solr documents)
        :param: solr_urls - list of strings, solr servers.
        """
        self.logger.debug('Updating solr: num_docs=%s solr_urls=%s', len(solr_docs), solr_urls)
        # batch send solr update
        out = solr_updater.update_solr(solr_docs, solr_urls, ignore_errors=True)
        errs = [x for x in out if x != 200]

        if len(errs) == 0:
            if update_processed:
                self.mark_processed([x['bibcode'] for x in solr_docs], 'solr', checksums=solr_docs_checksum, status='success')
        else:
            self.logger.error('%s docs failed indexing', len(errs))
            failed_bibcodes = []
            # recover from errors by sending docs one by one
            for doc, checksum in zip(solr_docs, solr_docs_checksum):
                try:
                    self.logger.error('trying individual update_solr %s', doc)
                    solr_updater.update_solr([doc], solr_urls, ignore_errors=False, commit=commit)
                    if update_processed:
                        self.mark_processed((doc['bibcode'],), 'solr', checksums=(checksum,), status='success')
                    self.logger.debug('%s success', doc['bibcode'])
                except Exception as e:
                    # if individual insert fails,
                    # and if 'body' is in excpetion we assume Solr failed on body field
                    # then we try once more without fulltext
                    # this bibcode needs to investigated as to why fulltext/body is failing
                    failed_bibcode = doc['bibcode']           
                    if 'body' in str(e) or 'not all arguments converted during string formatting' in str(e):
                        tmp_doc = dict(doc)
                        tmp_doc.pop('body', None)
                        try:
                            solr_updater.update_solr([tmp_doc], solr_urls, ignore_errors=False, commit=commit)
                            if update_processed:
                                self.mark_processed((doc['bibcode'],), 'solr', checksums=(checksum,), status='success')
                            self.logger.debug('%s success without body', doc['bibcode'])
                        except Exception as e:
                            self.logger.exception('Failed posting bibcode %s to Solr even without fulltext (urls: %s)', failed_bibcode, solr_urls)
                            failed_bibcodes.append(failed_bibcode)
                    else:
                        # here if body not in error message do not retry, just note as a fail
                        self.logger.error('Failed posting individual bibcode %s to Solr\nurls: %s, offending payload %s, error is %s', failed_bibcode, solr_urls, doc, e)
                        failed_bibcodes.append(failed_bibcode)
            # finally update postgres record
            if failed_bibcodes and update_processed:
                self.mark_processed(failed_bibcodes, 'solr', checksums=None, status='solr-failed')

    def mark_processed(self, bibcodes, type, checksums=None, status=None):
        """
        Updates the timesstamp for all documents that match the bibcodes.
        Optionally also sets the status (which says what actually happened
        with the document) and the checksum.
        Parameters bibcodes and checksums are expected to be lists of equal
        size with correspondence one to one, except if checksum is None (in
        this case, checksums are not updated).
        """

        # avoid updating whole database (when the set is empty)
        if len(bibcodes) < 1:
            return

        now = adsputils.get_date()
        updt = {'processed': now}
        if status:
            updt['status'] = status
        self.logger.debug('Marking docs as processed: now=%s, num bibcodes=%s', now, len(bibcodes))
        if type == 'solr':
            timestamp_column = 'solr_processed'
            checksum_column = 'solr_checksum'
        elif type == 'metrics':
            timestamp_column = 'metrics_processed'
            checksum_column = 'metrics_checksum'
        elif type == 'links':
            timestamp_column = 'datalinks_processed'
            checksum_column = 'datalinks_checksum'
        else:
            raise ValueError('invalid type value of %s passed, must be solr, metrics or links' % type)
        updt[timestamp_column] = now
        checksums = checksums or [None] * len(bibcodes)
        for bibcode, checksum in zip(bibcodes, checksums):
            updt[checksum_column] = checksum
            with self.session_scope() as session:
                session.query(Records).filter_by(bibcode=bibcode).update(updt, synchronize_session=False)
        session.commit()

    def get_metrics(self, bibcode):
        """Helper method to retrieve data from the metrics db

        @param bibcode: string
        @return: JSON structure if record was found, {} else
        """

        if not self._metrics_session:
            raise Exception('METRCIS_SQLALCHEMU_URL not set!')

        with self.metrics_session_scope() as session:
            x = session.query(MetricsModel).filter(MetricsModel.bibcode == bibcode).first()
            if x:
                return x.toJSON()
            else:
                return {}

    @contextmanager
    def metrics_session_scope(self):
        """Provides a transactional session - ie. the session for the
        current thread/work of unit.

        Use as:

            with session_scope() as session:
                o = ModelObject(...)
                session.add(o)
        """

        if self._metrics_session is None:
            raise Exception('DB not initialized properly, check: METRICS_SQLALCHEMY_URL')

        # create local session (optional step)
        s = self._metrics_session()

        try:
            yield s
            s.commit()
        except:
            s.rollback()
            raise
        finally:
            s.close()

    def index_metrics(self, batch, batch_checksum, update_processed=True):
        """Writes data into the metrics DB.
        :param: batch - list of json objects to upsert into the metrics db
        :return: tupple (list-of-processed-bibcodes, exception)
        It tries hard to avoid raising exceptions; it will return the list
        of bibcodes that were successfully updated. It will also update
        metrics_processed timestamp in the records table for every bibcode that succeeded.
        """
        if not self._metrics_session:
            raise Exception('You cant do this! Missing METRICS_SQLALACHEMY_URL?')

        self.logger.debug('Updating metrics db: len(batch)=%s', len(batch))
        with self.metrics_session_scope() as session:
            if len(batch):
                trans = session.begin_nested()
                try:
                    # bulk upsert
                    trans.session.execute(self._metrics_table_upsert, batch)
                    trans.commit()
                    if update_processed:
                        self.mark_processed([x['bibcode'] for x in batch], 'metrics', checksums=batch_checksum, status='success')
                except exc.SQLAlchemyError as e:
                    # recover from errors by upserting data one by one
                    trans.rollback()
                    self.logger.error('Metrics insert batch failed, will upsert one by one %s recs', len(batch))
                    failed_bibcodes = []
                    for x, checksum in zip(batch, batch_checksum):
                        try:
                            trans.session.execute(self._metrics_table_upsert, [x])
                            trans.commit()
                            if update_processed:
                                self.mark_processed((x['bibcode'],), 'metrics', checksums=(checksum,), status='success')
                        except Exception as e:
                            failed_bibcode = x['bibcode']
                            self.logger.exception('Failed posting individual bibcode %s to metrics', failed_bibcode)
                            failed_bibcodes.append(failed_bibcode)
                    if failed_bibcodes and update_processed:
                        self.mark_processed(failed_bibcodes, 'metrics', checksums=None, status='metrics-failed')
                except Exception as e:
                    trans.rollback()
                    self.logger.error('DB failure: %s', e)
                    if update_processed:
                        self.mark_processed([x['bibcode'] for x in batch], 'metrics', checksums=None, status='metrics-failed')

    def index_datalinks(self, links_data, links_data_checksum, update_processed=True):
        # todo is failed right?
        links_url = self.conf.get('LINKS_RESOLVER_UPDATE_URL')
        api_token = self.conf.get('ADS_API_TOKEN', '')
        if len(links_data):
            # bulk put request
            bibcodes = [x['bibcode'] for x in links_data]
            r = requests.put(links_url, data=json.dumps(links_data), headers={'Authorization': 'Bearer {}'.format(api_token)})
            if r.status_code == 200:
                self.logger.info('sent %s datalinks to %s including %s', len(links_data), links_url, links_data[0])
                if update_processed:
                    self.mark_processed(bibcodes, 'links', checksums=links_data_checksum, status='success')
            else:
                # recover from errors by issuing put requests one by one
                self.logger.error('error sending links to %s, error = %s', links_url, r.text)
                failed_bibcodes = []
                for data, checksum in zip(links_data, links_data_checksum):
                    r = requests.put(links_url, data=json.dumps([data]), headers={'Authorization': 'Bearer {}'.format(api_token)})
                    if r.status_code == 200:
                        self.logger.info('sent 1 datalinks to %s for bibcode %s', links_url, data.get('bibcode'))
                        if update_processed:
                            self.mark_processed(bibcodes, 'links', checksums=links_data_checksum, status='success')
                    else:
                        self.logger.error('error sending individual links to %s for bibcode %s, error = %s', links_url, data.get('bibcode'), r.text)
                        failed_bibcodes.append(data['bibcode'])
                if failed_bibcodes and update_processed:
                    self.mark_processed(failed_bibcodes, 'links', status='links-failed')

    def metrics_delete_by_bibcode(self, bibcode):
        with self.metrics_session_scope() as session:
            r = session.query(MetricsModel).filter_by(bibcode=bibcode).first()
            if r is not None:
                session.delete(r)
                session.commit()
                return True

    def checksum(self, data, ignore_keys=('mtime', 'ctime', 'update_timestamp')):
        """
        Compute checksum of the passed in data. Preferred situation is when you
        give us a dictionary. We can clean it up, remove the 'ignore_keys' and
        sort the keys. Then compute CRC on the string version. You can also pass
        a string, in which case we simple return the checksum.
        
        @param data: string or dict
        @param ignore_keys: list of patterns, if they are found (anywhere) in
            the key name, we'll ignore this key-value pair
        @return: checksum
        """
        assert isinstance(ignore_keys, tuple)

        if isinstance(data, basestring):
            if sys.version_info > (3,):
                data_str = data.encode('utf-8')
            else:
                data_str = unicode(data)
            return hex(zlib.crc32(data_str) & 0xffffffff)
        else:
            data = deepcopy(data)
            # remove all the modification timestamps
            for k, v in list(data.items()):
                for x in ignore_keys:
                    if x in k:
                        del data[k]
                        break
            if sys.version_info > (3,):
                data_str = json.dumps(data, sort_keys=True).encode('utf-8')
            else:
                data_str = json.dumps(data, sort_keys=True)
            return hex(zlib.crc32(data_str) & 0xffffffff)

    def request_aff_augment(self, bibcode, data=None):
        """send aff data for bibcode to augment affiliation pipeline

        set data parameter to provide test data"""
        if data is None:
            rec = self.get_record(bibcode)
            if rec is None:
                self.logger.warning('request_aff_augment called but no data at all for bibcode {}'.format(bibcode))
                return
            bib_data = rec.get('bib_data', None)
            if bib_data is None:
                self.logger.warning('request_aff_augment called but no bib data for bibcode {}'.format(bibcode))
                return
            aff = bib_data.get('aff', None)
            author = bib_data.get('author', '')
            data = {
                'bibcode': bibcode,
                "aff": aff,
                "author": author,
            }
        if data and data['aff']:
            message = AugmentAffiliationRequestRecord(**data)
            self.forward_message(message)
            self.logger.debug('sent augment affiliation request for bibcode {}'.format(bibcode))
        else:
            self.logger.debug('request_aff_augment called but bibcode {} has no aff data'.format(bibcode))

    def prepare_bibcode(self, bibcode):
        """prepare data for classifier pipeline
        
        Parameters
        ----------
        bibcode = reference ID for record (Needs to include SciXID)

        """
        rec = self.get_record(bibcode)
        if rec is None:
            self.logger.warning('request_classifier called but no data at all for bibcode {}'.format(bibcode))
            return
        bib_data = rec.get('bib_data', None)
        if bib_data is None:
            self.logger.warning('request_classifier called but no bib data for bibcode {}'.format(bibcode))
            return
        title = bib_data.get('title', '')
        abstract = bib_data.get('abstract', '')
        data = {
            'bibcode': bibcode,
            'title': title,
            'abstract': abstract,
        }
        return data

    def request_classify(self, bibcode=None,scix_id = None, filename=None,mode='auto', batch_size=500, data=None, check_boolean=False, operation_step=None):
        """ send classifier request for bibcode to classifier pipeline

        set data parameter to provide test data

        Parameters
        ----------
        bibcode = reference ID for record (Needs to include SciXID)
        scix_id = reference ID for record
        filename : filename of input file with list of records to classify 
        mode : 'auto' (default) assumes single record input from master, 'manual' assumes multiple records input at command line
        batch_size : size of batch for large input files
        check_boolean : Used for testing - writes the message to file
        operation_step: string - defines mode of operation: classify, classify_verify, or verify
        
        """
        self.logger.info('request_classify called with bibcode={}, filename={}, mode={}, batch_size={}, data={}, validate={}'.format(bibcode, filename, mode, batch_size, data, check_boolean))

        if not self._config.get('OUTPUT_TASKNAME_CLASSIFIER'):
            self.logger.warning('request_classifier called but no classifier taskname in config')
            return
        if not self._config.get('OUTPUT_CELERY_BROKER_CLASSIFIER'):
            self.logger.warning('request_classifier called but no classifier broker in config')
            return

        if bibcode is not None and mode == 'auto':
            if data is None:
                data = self.prepare_bibcode(bibcode)
            if data and data.get('title'):
                data['operation_step'] = operation_step
                self.logger.DEBUG(f'Converting {data} to protobuf')
                message = ClassifyRequestRecordList()
                entry = message.classify_requests.add()
                entry.bibcode = data.get('bibcode')
                title = data.get('title')
                if isinstance(title, (list,tuple)):
                    title = title[0]
                entry.title = title
                entry.abstract = data.get('abstract')
                entry.operation_step = data.get('operation_step')
                output_taskname=self._config.get('OUTPUT_TASKNAME_CLASSIFIER')
                output_broker=self._config.get('OUTPUT_CELERY_BROKER_CLASSIFIER')
                self.logger.DEBUG('Sending message for batch - bibcode only input')
                self.logger.DEBUG('sending message {}'.format(message))
                self.forward_message(message, pipeline='classifier')
                self.logger.debug('sent classifier request for bibcode {}'.format(bibcode))
            else:
                self.logger.debug('request_classifier called but bibcode {} has no title data'.format(bibcode))
        if filename is not None and mode == 'manual':
            batch_idx = 0
            batch_list = []
            self.logger.info('request_classifier called with filename {}'.format(filename))
            with open(filename, 'r') as f:
                reader = csv.DictReader(f)
                bibcodes =  [row for row in reader]
            while batch_idx < len(bibcodes):
                bibcodes_batch = bibcodes[batch_idx:batch_idx+batch_size]
                for record in bibcodes_batch:
                    if record.get('title') or record.get('abstract'):
                        data = record
                    else:
                        data = self.prepare_bibcode(record['bibcode'])
                    if data and data.get('title'):
                        batch_list.append(data)
                if len(batch_list) > 0:
                    message = ClassifyRequestRecordList() 
                    for item in batch_list:
                        entry = message.classify_requests.add()
                        entry.bibcode = item.get('bibcode')
                        title = item.get('title')
                        if isinstance(title, (list, tuple)):
                            title = title[0]
                        entry.title = title
                        entry.abstract = item.get('abstract')
                        entry.operation_step = operation_step
                        entry.output_path = filename.split('.')[0]
                    output_taskname=self._config.get('OUTPUT_TASKNAME_CLASSIFIER')
                    output_broker=self._config.get('OUTPUT_CELERY_BROKER_CLASSIFIER')
                    if check_boolean is True:
                        # Save message to file 
                        # with open('classifier_request.json', 'w') as f:
                        #     f.write(str(message))
                        json_message = MessageToJson(message)
                        with open('classifier_request.json', 'w') as f:
                            f.write(json_message)
                    else:
                        self.logger.info('Sending message for batch')
                        self.logger.info('sending message {}'.format(message))
                        self.forward_message(message, pipeline='classifier')
                        self.logger.debug('sent classifier request for batch {}'.format(batch_idx))

                batch_idx += batch_size
                batch_list = []

    def _encode_boost_bytes_field(self, value):
        """Encode a value for BoostRequestRecord bytes fields (bib_data, metrics)."""
        if value is None or value == '':
            return b''
        if isinstance(value, bytes):
            return value
        if isinstance(value, dict):
            return json.dumps(value).encode('utf-8')
        if isinstance(value, str):
            return value.encode('utf-8')
        return str(value).encode('utf-8')

    def _populate_boost_request_entry(self, entry, rec, metrics, classifications,
                                      run_id=None, output_path=None):
        """Populate a BoostRequestRecord protobuf entry from record data."""
        entry.bibcode = rec.get('bibcode', '')
        entry.scix_id = rec.get('scix_id', '')
        entry.status = AdsMsgStatus.updated
        entry.bib_data = self._encode_boost_bytes_field(rec.get('bib_data', ''))
        entry.metrics = self._encode_boost_bytes_field(metrics)

        if classifications:
            entry.classifications.extend(
                classifications if isinstance(classifications, list) else []
            )

        if run_id is not None:
            entry.run_id = run_id
        if output_path:
            entry.output_path = output_path

    def _get_info_for_boost_entry(self, bibcode):
        rec = self.get_record(bibcode) or {}
        metrics = {}
        try:
            metrics = self.get_metrics(bibcode) or {}
        except Exception:
            # get_metrics raises when METRICS_SQLALCHEMY_URL is not configured
            pass

        if not metrics:
            # Fall back to the metrics stored on the record itself. The metrics
            # pipeline writes these via task_update_record, and they carry the
            # refereed flag that the Boost Pipeline needs; without this the
            # refereed boost is always 0 wherever the metrics db is not set up.
            metrics = rec.get('metrics') or {}

        # Collections come from the classifier and are stored on the record.
        # classifications is in Records._json_fields, so toJSON returns it
        # already decoded; guard anyway in case a raw string comes through.
        classifications = rec.get('classifications') or []
        if isinstance(classifications, str):
            try:
                classifications = json.loads(classifications)
            except ValueError:
                classifications = [classifications]
        if not isinstance(classifications, list):
            classifications = list(classifications)

        entry = None
        if rec:
            entry = (rec, metrics, classifications)
        return entry

    def generate_boost_request_message(self, bibcodes, run_id=None, output_path=None):
        """Build and send boost request message to Boost Pipeline.
        
        Parameters
        ----------
        bibcodes : list
            List of bibcodes to send. Can be a single-item list for one bibcode.
        run_id : int, optional
            Optional job/run identifier added to each entry.
        output_path : str, optional
            Optional output path hint added to each entry.

        Returns
        -------
        int
            Number of bibcodes successfully processed.
        """
        
        if not self._config.get('OUTPUT_TASKNAME_BOOST'):
            self.logger.warning('generate_boost_request_message called but no boost taskname in config')
            return 0
        if not self._config.get('OUTPUT_CELERY_BROKER_BOOST'):
            self.logger.warning('generate_boost_request_message called but no boost broker in config')
            return 0

        # Normalize input to always be a list
        if not bibcodes:
            self.logger.warning('generate_boost_request_message called without bibcodes')
            return 0
        
        if isinstance(bibcodes, str):
            bibcodes = [bibcodes]
        
        if not isinstance(bibcodes, list):
            self.logger.warning('generate_boost_request_message called with invalid bibcodes type: %s', type(bibcodes))
            return 0

        if not bibcodes:
            self.logger.warning('No bibcodes to process')
            return 0

        self.logger.info('Processing %d bibcode(s) for boost request', len(bibcodes))

        message = BoostRequestRecordList()
        sent_count = 0

        for bibcode in bibcodes:
            if not bibcode:
                continue

            try:
                (rec, metrics, classifications) = self._get_info_for_boost_entry(bibcode)
                if not rec:
                    self.logger.debug('Skipping bibcode with no data: %s', bibcode)
                    continue

                entry = message.boost_requests.add()
                self._populate_boost_request_entry(
                    entry, rec, metrics, classifications, run_id, output_path
                )
                sent_count += 1

            except Exception as e:
                self.logger.error('Error retrieving record data for bibcode %s: %s', bibcode, e)
                continue

        if sent_count > 0:
            try:
                message.status = AdsMsgStatus.updated
                output_taskname = self._config.get('OUTPUT_TASKNAME_BOOST')
                output_broker = self._config.get('OUTPUT_CELERY_BROKER_BOOST')
                self.logger.debug('output_taskname: {}'.format(output_taskname))
                self.logger.debug('output_broker: {}'.format(output_broker))
                self.logger.debug('sending message with %d boost request(s)', sent_count)

                self.forward_message(message, pipeline='boost')
                self.logger.info('Sent boost request for %d record(s) to Boost Pipeline', sent_count)

                return sent_count

            except Exception as e:
                self.logger.exception('Error sending boost request: %s', e)
                return 0

        self.logger.warning('No valid records to send for boost request')
        return 0

    def generate_links_for_resolver(self, record):
        """use nonbib or bib elements of database record and return links for resolver and checksum"""
        # nonbib data has something like
        #  "data_links_rows": [{"url": ["http://arxiv.org/abs/1902.09522"]
        # bib data has json string for:
        # "links_data": [{"access": "open", "instances": "", "title": "", "type": "preprint",

        #                 "url": "http://arxiv.org/abs/1902.09522"}]

        resolver_record = None   # default value to return
        bibcode = record.get('bibcode')
        nonbib = record.get('nonbib_data', {})
        if type(nonbib) is not dict:
            nonbib = {}    # in case database has None or something odd
        nonbib_links = nonbib.get('data_links_rows', None)
        if nonbib_links:
            # when avilable, prefer link info from nonbib
            resolver_record = {'bibcode': bibcode,
                               'data_links_rows': nonbib_links}
        else:
            # as a fallback, use link from bib/direct ingest
            bib = record.get('bib_data', {})
            if type(bib) is not dict:
                bib = {}
            bib_links_record = bib.get('links_data', None)
            if bib_links_record:
                try:
                    bib_links_data = json.loads(bib_links_record[0])
                    url = bib_links_data.get('url', None)
                    if url:
                        # need to change what direct sends
                        url_pdf = url.replace('/abs/', '/pdf/')
                        resolver_record = {'bibcode': bibcode,
                                           'data_links_rows': [{'url': [url],
                                                                'title': [''], 'item_count': 0,
                                                                'link_type': 'ESOURCE',
                                                                'link_sub_type': 'EPRINT_HTML'},
                                                               {'url': [url_pdf],
                                                                'title': [''], 'item_count': 0,
                                                                'link_type': 'ESOURCE',
                                                                'link_sub_type': 'EPRINT_PDF'}]}
                except (KeyError, ValueError):
                    # here if record holds unexpected value
                    self.logger.error('invalid value in bib data, bibcode = {}, type = {}, value = {}'.format(bibcode, type(bib_links_record), bib_links_record))
        return resolver_record

    def should_include_in_sitemap(self, record):
        """
        Determine if a record should be included in the sitemap based on SOLR processing status.
        Logs the reasoning for inclusion/exclusion decisions.
        
        Include record in sitemap if:
        1. Has bib_data (will be processed by SOLR)
        2. SOLR processing succeeded (not solr-failed or retrying)
        3. If processed, processing isn't too stale
        
        Args:
            record: Dictionary with record data including has_bib_data, status, timestamps
            
        Returns:
            bool: True if record should be included in sitemap, False otherwise
        """
        
        
        # Extract values from record dictionary
        bibcode = record.get('bibcode', None)
        has_bib_data = record.get('has_bib_data', None)
        bib_data_updated = record.get('bib_data_updated')
        solr_processed = record.get('solr_processed') 
        status = record.get('status')
        
        # Must have bibliographic data
        if not has_bib_data or not bibcode:
            self.logger.debug('Excluding %s from sitemap: No bibcode or has_bib_data is False', bibcode)
            return False
        
         # Exclude if SOLR failed or if record is being retried (previously failed)
        if status in ['solr-failed', 'retrying']:
            self.logger.warning('Excluding %s from sitemap: SOLR failed or retrying (status: %s)', bibcode, status)
            return False
        
        # If never processed by SOLR, include it (will be processed soon)
        if not solr_processed:
            self.logger.debug('Including %s in sitemap: Has bib_data, pending SOLR processing', bibcode)
            return True
    
        
        # If we have both timestamps, check staleness (but still include in sitemap)
        if bib_data_updated and solr_processed:
            processing_lag = bib_data_updated - solr_processed
            if processing_lag > timedelta(days=5):
                self.logger.debug('SOLR processing might be stale for %s by %s (bib_data_updated: %s, solr_processed: %s) - keeping in sitemap',
                            bibcode, processing_lag, bib_data_updated, solr_processed)
                return True
        
        # All good - include in sitemap
        self.logger.debug('Including %s in sitemap: Valid (status: %s, solr_processed: %s)', bibcode, status, solr_processed)
        return True
    
    def get_records_bulk(self, bibcodes, session, load_only=None):
        """Get multiple records efficiently using provided session.
        
        :param bibcodes: list of bibcodes
        :param session: database session to use
        :param load_only: list of fields to load
        :return: dict mapping bibcode to record data
        """
        if not bibcodes:
            return {}
        
        query = session.query(Records).filter(Records.bibcode.in_(bibcodes))
        if load_only:
            query = query.options(_load_only(*load_only))
        
        records = query.all()
        
        # Convert to dictionary for fast lookup
        records_dict = {}
        for record in records:
            record_data = {}
            for field in (load_only or ['id', 'bibcode', 'bib_data', 'bib_data_updated', 'solr_processed', 'status']):
                record_data[field] = getattr(record, field, None)
            # Add has_bib_data boolean for sitemap checks
            record_data['has_bib_data'] = bool(record_data.get('bib_data'))
            records_dict[record.bibcode] = record_data
        
        return records_dict
    
    
    def get_sitemap_info_bulk(self, bibcodes, session):
        """Get multiple sitemap infos efficiently using provided session.
        
        :param bibcodes: list of bibcodes
        :param session: database session to use
        :return: dict mapping bibcode to sitemap info data
        """
        if not bibcodes:
            return {}
        
        sitemap_infos = session.query(SitemapInfo).filter(SitemapInfo.bibcode.in_(bibcodes)).all()
        return {info.bibcode: info.toJSON() for info in sitemap_infos}
    
    
    def get_current_sitemap_state(self, session):
        """Get current sitemap state using provided session.
        
        Finds the last file being filled (highest index number).
        If that file is full (>= MAX_RECORDS_PER_SITEMAP), returns state for next file.
        
        :param session: Database session to use
        :return: dict with current filename, count, and index
        """
        max_records = self.conf.get('MAX_RECORDS_PER_SITEMAP', 50000)
        
        # Get all files with their counts
        results = session.query(
            SitemapInfo.sitemap_filename,
            func.count(SitemapInfo.id).label('record_count')
        ).filter(
            SitemapInfo.sitemap_filename.isnot(None)
        ).group_by(
            SitemapInfo.sitemap_filename
        ).all()
        
        if results:
            # Sort by numeric index (not alphabetically)
            def get_file_index(result):
                return int(result.sitemap_filename.split('_bib_')[1].split('.')[0])
            
            sorted_results = sorted(results, key=get_file_index)
            last_result = sorted_results[-1]
            
            filename = last_result.sitemap_filename
            count = last_result.record_count
            index = get_file_index(last_result)
            
            # If last file is full, prepare for next file
            if count >= max_records:
                return {
                    'filename': f'sitemap_bib_{index + 1}.xml',
                    'count': 0,
                    'index': index + 1
                }
            
            # Last file has room, use it
            return {
                'filename': filename,
                'count': count,
                'index': index
            }
        
        # No files exist yet, start with file 1
        return {
            'filename': 'sitemap_bib_1.xml',
            'count': 0,
            'index': 1
        }
    
    def _process_sitemap_batch(self, bibcodes, action, session, sitemap_state):
        """Process a batch of bibcodes with provided session and state.
        
        :param bibcodes: list of bibcodes to process
        :param action: 'add' or 'force-update'
        :param session: database session to use
        :param sitemap_state: current sitemap state dict
        :return: tuple (successful_count, failed_count, sitemap_records, updated_state)
        """
        fields = ['id', 'bibcode', 'bib_data', 'bib_data_updated', 'solr_processed', 'status']
        
        # Get all data for this batch in bulk using the provided session
        records_data = self.get_records_bulk(bibcodes, session, load_only=fields)
        sitemap_infos = self.get_sitemap_info_bulk(bibcodes, session)
        
        # Use provided sitemap state
        current_filename = sitemap_state['filename']
        current_count = sitemap_state['count']
        current_index = sitemap_state['index']
        max_records = self.conf.get('MAX_RECORDS_PER_SITEMAP', 50000)
        
        # Process records
        new_records = []
        update_records = []
        sitemap_records = []
        successful_count = 0
        failed_count = 0
        
        for bibcode in bibcodes:
            try:
                record = records_data.get(bibcode)
                if record is None:
                    self.logger.error('The bibcode %s doesn\'t exist!', bibcode)
                    failed_count += 1
                    continue
                                
                # Check if record should be included in sitemap based on SOLR status 
                if not self.should_include_in_sitemap(record):
                    self.logger.debug('Skipping %s: does not meet sitemap inclusion criteria', bibcode)
                    failed_count += 1
                    continue
                
                sitemap_info = sitemap_infos.get(bibcode)
                
                # Create sitemap record data structure
                sitemap_record = {
                    'record_id': record.get('id'),
                    'bibcode': record.get('bibcode'),
                    'bib_data_updated': record.get('bib_data_updated', None),
                    'filename_lastmoddate': None,
                    'sitemap_filename': None,
                    'scix_id': None,
                    'update_flag': False
                }
                
                if sitemap_info is None:
                    # New sitemap record - assign filename
                    if current_count >= max_records:
                        current_index += 1
                        current_filename = f'sitemap_bib_{current_index}.xml'
                        current_count = 0
                    
                    sitemap_record['sitemap_filename'] = current_filename
                    sitemap_record['update_flag'] = True
                    # filename_lastmoddate stays None for new records until files are generated
                    new_records.append(sitemap_record)
                    sitemap_records.append((sitemap_record['record_id'], sitemap_record['bibcode']))
                    current_count += 1
                    
                else:
                    # Existing sitemap record - update it
                    sitemap_record['filename_lastmoddate'] = sitemap_info.get('filename_lastmoddate', None)
                    sitemap_record['sitemap_filename'] = sitemap_info.get('sitemap_filename', None)
                    
                    bib_data_updated = sitemap_record.get('bib_data_updated', None)
                    file_modified = sitemap_record.get('filename_lastmoddate', None)
                    
                    # Determine if update_flag should be True
                    if action == 'force-update':
                        sitemap_record['update_flag'] = True
                        # Update filename_lastmoddate to bib_data_updated when forcing update
                        sitemap_record['filename_lastmoddate'] = bib_data_updated
                    elif action == 'add':
                        # Sitemap file has never been generated OR data updated since last generation
                        if file_modified is None or (file_modified and bib_data_updated and bib_data_updated > file_modified):
                            sitemap_record['update_flag'] = True
                            # Update filename_lastmoddate to bib_data_updated when updating
                            sitemap_record['filename_lastmoddate'] = bib_data_updated
                    
                    update_records.append((sitemap_record, sitemap_info))
                    sitemap_records.append((sitemap_record['record_id'], sitemap_record['bibcode']))
                
                successful_count += 1
                self.logger.debug('Successfully processed sitemap for bibcode: %s', bibcode)
                
            except Exception as e:
                failed_count += 1
                self.logger.error('Failed to process sitemap for bibcode %s: %s', bibcode, str(e))
                continue
        
        # Bulk database operations using the provided session
        try:
            if new_records:
                self.bulk_insert_sitemap_records(new_records, session)
            if update_records:
                self.bulk_update_sitemap_records(update_records, session)
        except Exception as e:
            self.logger.error('Failed to perform bulk database operations: %s', str(e))
            raise
        
        # Return updated state for next batch
        updated_state = {
            'filename': current_filename,
            'count': current_count,
            'index': current_index
        }
        
        # Return counts of new vs updated records
        batch_stats = {
            'successful': successful_count,
            'failed': failed_count,
            'new_records': len(new_records),
            'updated_records': len(update_records),
            'sitemap_records': sitemap_records
        }
        
        return batch_stats, updated_state
    
    
    def bulk_insert_sitemap_records(self, sitemap_records, session):
        """Bulk insert sitemap records using provided session.
        
        :param sitemap_records: list of sitemap record dictionaries
        :param session: database session to use
        """
        if not sitemap_records:
            return
        
        session.bulk_insert_mappings(SitemapInfo, sitemap_records)
    
    def bulk_update_sitemap_records(self, update_records, session):
        """Bulk update sitemap records using provided session.
        
        :param update_records: list of tuples (sitemap_record, sitemap_info)
        :param session: database session to use
        """
        if not update_records:
            return
            
        # Prepare bulk update data
        update_mappings = []
        for sitemap_record, sitemap_info in update_records:
            # Use the primary key 'id' from sitemap_info for bulk update
            update_data = {'id': sitemap_info['id']}
            
            # Add fields that need updating
            for field in ['bib_data_updated', 'sitemap_filename', 'filename_lastmoddate']:
                if field in sitemap_record:
                    update_data[field] = sitemap_record[field]
            
            # Set update_flag from sitemap_record (will be True for records needing regeneration)
            update_data['update_flag'] = sitemap_record.get('update_flag', False)
            update_mappings.append(update_data)
        
        session.bulk_update_mappings(SitemapInfo, update_mappings)

        
    def delete_contents(self, table):
        """Delete all contents of the table

        :param table: string, name of the table
        """
        with self.session_scope() as session:
            session.query(table).delete()
            session.commit()
    
    def backup_sitemap_files(self, directory):
        """Backup the sitemap files to the given directory

        :param directory: string, directory to backup the sitemap files
        """

        # move dir to /app/tmp 
        date = adsputils.get_date()
        tmpdir = '/app/logs/tmp/sitemap_{}_{}_{}-{}/'.format(date.year, date.month, date.day, date.time())
        os.system('mkdir -p {}'.format(tmpdir))
        os.system('mv {}/* {}/'.format(directory, tmpdir))
        return
    
    def _execute_remove_action(self, session, bibcodes_to_remove):
        """
        Efficiently remove bibcodes from sitemaps using optimized bulk operations.
        
        Args:
            session: Database session
            bibcodes_to_remove: List of bibcodes to remove from sitemaps
            
        Returns:
            tuple: (removed_count: int, files_to_delete: set, files_to_update: set)
        """
        if not bibcodes_to_remove:
            return 0, set(), set()

        # Get affected files before deletion
        file_counts_before = dict(
            session.query(SitemapInfo.sitemap_filename, func.count(SitemapInfo.id))
            .filter(
                SitemapInfo.bibcode.in_(bibcodes_to_remove),
                SitemapInfo.sitemap_filename.isnot(None)
            )
            .group_by(SitemapInfo.sitemap_filename)
            .all()
        )
        
        if not file_counts_before:
            return 0, set(), set()
        
        affected_files = set(file_counts_before.keys())
        
        # Bulk delete target records
        removed_count = session.query(SitemapInfo).filter(
            SitemapInfo.bibcode.in_(bibcodes_to_remove)
        ).delete(synchronize_session=False)
        
        # Get file counts after deletion 
        file_counts_after = dict(
            session.query(SitemapInfo.sitemap_filename, func.count(SitemapInfo.id))
            .filter(SitemapInfo.sitemap_filename.in_(affected_files))
            .group_by(SitemapInfo.sitemap_filename)
            .all()
        )
        
        # Identify empty files and files that still have records
        files_to_delete = set(file for file in affected_files if file not in file_counts_after)
        files_to_update = affected_files - files_to_delete

        self.logger.info('Removed %d bibcodes from %d files, %d files now empty, %d files pending regeneration flag', 
                         removed_count, len(affected_files), len(files_to_delete), len(files_to_update))

        return removed_count, files_to_delete, files_to_update


    def delete_sitemap_files(self, files_to_delete, sitemap_dir):
        """
        Helper function to delete empty sitemap files from all sites.
        
        Args:
            files_to_delete: Set of sitemap filenames to delete
            sitemap_dir: Base sitemap directory path
        """
        if not files_to_delete:
            return
        
        sites_config = self.conf.get('SITES', {})
        deleted_count = 0
        
        for filename in files_to_delete:
            for site_key in sites_config.keys():
                site_filepath = os.path.join(sitemap_dir, site_key, filename)
                if os.path.exists(site_filepath):
                    os.remove(site_filepath)
                    self.logger.info('Deleted empty sitemap file: %s', site_filepath)
                    deleted_count += 1
        
        self.logger.info('Deleted %d empty sitemap files across all sites', deleted_count)


    def chunked(self, iterable, chunk_size):
        """Yield successive chunks from iterable without copying data"""
        iterator = iter(iterable)
        while True:
            chunk = list(islice(iterator, chunk_size))
            if not chunk:
                break
            yield chunk
#!/usr/bin/python
# git repository backup script for the T3
# written 2013 by Daniel Meister <dmeister@phys.ethz.ch>
# for details see https://wiki.chipp.ch/twiki/bin/view/CmsTier3/RemoteRepositoryBackup

# imports
from RRBackup import *
import logging

# constants to define backup policy
DAYS = 7
WEEKS = 5
MONTHS = 12
# constants to define paths
CFGFILE = '/swshare/rrbackup/config/backup.list'
STORAGEPATH = '/swshare/rrbackup/store/'
LOGFILE =  '/swshare/rrbackup/log/backup.log'
LOGLEVEL = logging.DEBUG

today = datetime.date.today()

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=LOGLEVEL,
    filename=LOGFILE
    )

try:

    bp = BackupPolicy(today,DAYS,WEEKS,MONTHS)
    keep = bp.getkeep()
    logging.debug('keeping %d elements: %s' % (len(keep), ','.join(sorted(keep))))

    # loop over all repositories listed in CFGFILE
    list = RepositoryList(CFGFILE)
    for repo, exception in list:
        try:
            if exception: raise exception
        except RepositoryDefinitionException, e:
            # unparsable definition cannot continue with this
            logging.warning(
                'the following line of %s could not be processed: %s' %
                (CFGFILE,' '.join(e.failed.split())))
            continue
        except RepositoryDuplicateException, e:
            # unparsable definition cannot continue with this
            logging.error(
                'repository id "%s" already taken; skipping line: %s' %
                (e.id, ' '.join(e.line.split())))
            continue
        except RepositoryActiveException, e:
            # just assume active to be on the safe side
            logging.warning(
                '%(id)s: unknown active value "%(val)s"; assuming IS active' %
                {'id': repo.id, 'val': e.val})

        logging.info('%(id)s: starting backup of repository at %(url)s' % (repo)) 
        store = BackupStorage(STORAGEPATH,repo)
        store.ensure()
        logging.info(
            '%(id)s: repository is%(active)s active' %
            {'id': repo.id, 'active':{True:'',False:' NOT'}[repo.active]})
        if repo.active:
            # why do we have such an old version of Python...
            try:
                try:
                    backup = RepositoryBackup(repo)
                    logging.debug('%(id)s: download remote content' % repo)
                    backup.retrieve()
                    logging.debug(
                        '%(id)s: downloaded repository to %(path)s' %
                        {'id': repo.id, 'path': backup.tmp})
                    logging.debug('%(id)s: store downloaded repository' % repo)
                    backup.save(store.fullpath(today))
                except RepositoryNameException, e:
                    logging.error('%(id)s: could not extract name of repository from URL %(url)s' % e.repo)
                    continue
                except RepositoryFetchException, e:
                    logging.error('%(id)s: could not fetch repository from URL %(url)s' % e.repo)
                    continue
                except RepositoryStoreException, e:
                    logging.error(
                        '%(id)s: could not store repository at %(path)s' %
                        {'id': repo.id, 'path': e.path})
                    continue
            finally:
                try:
                    backup.clean()
                except RepositoryCleanException, e:
                    logging.warning('%(id)s: could cleanup temporary directory' % e.repo)
                    

        have = store.list()
        logging.debug(
            '%(id)s: have %(len)d elements: %(list)s' %
            {'id': repo.id, 'len': len(have), 'list':','.join(sorted(have))})
        delete = have - keep
        logging.info(
            '%(id)s: deleting %(len)d elements: %(list)s' %
            {'id': repo.id, 'len': len(delete), 'list':','.join(sorted(delete))})
        store.remove(delete)
        
except Exception, e:
    logging.critical('unhandeled exception (%s): %s' % (e.__class__.__name__, e))


''' without caring about exceptions...
bp = BackupPolicy(today,DAYS,WEEKS,MONTHS)
keep = bp.getkeep()
list = RepositoryList(CFGFILE)
for repo in list:
    store = BackupStorage(STORAGEPATH,repo)
    store.ensure()
    if repo.active:
        backup = RepositoryBackup(repo)
        backup.retrieve()
        backup.save(store.fullpath(today))
    have = store.list()
    delete = have - keep
    store.remove(delete)
'''



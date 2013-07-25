#!/usr/bin/python
# git repository backup script for the T3
# written 2013 by Daniel Meister <dmeister@phys.ethz.ch>
# for details see https://wiki.chipp.ch/twiki/bin/view/CmsTier3/RemoteRepositoryBackup

# imports
import datetime
import tempfile
import os
import subprocess
import re
import shutil

# classes
class BackupPolicy:
    '''
    class defining the backup policy
    '''
    def __init__(self,today,days,weeks,months):
        self.today = today
        self.DAYS = days
        self.WEEKS = weeks
        self.MONTHS = months

    def getkeep(self):
        keep = set()

        for i in range(self.DAYS):
            keep.add((self.today - datetime.timedelta(i)).isoformat())

        prev_monday = self.today - datetime.timedelta(self.today.weekday())
        for i in range(self.WEEKS):
            keep.add((prev_monday - datetime.timedelta(7 * i)).isoformat())
            
        first_monday = prev_monday - datetime.timedelta(self.today.day/7 * 7)
        for i in range(self.MONTHS):
            keep.add(first_monday.isoformat())
            first_monday = first_monday - datetime.timedelta(7)
            first_monday = first_monday - datetime.timedelta(first_monday.day/7 * 7)

        return keep

class Repository:
    '''
    repository definition consisting of the tuple (id, url, active)
    could be extended to handle different repository types
    '''
    def __init__(self,id,url,active):
        self.id = id
        self.url = url
        self.active = active

    def __getitem__(self,attr):
        return getattr(self,attr)

    def __repr__(self):
        return 'Repository: %s at %s' % (self.id, self.url)

    def name(self):
        r = re.compile(".*/([^/]+\.git)")
        m = r.search(self.url)
        if m:
            return m.group(1)
        raise RepositoryNameException(self)
        

class RepositoryList:
    '''
    iterator that reads list of repositories to backup from config file
    '''
    def __init__(self,file):
        self.lines = [line.strip() for line in open(file)]
        self.ids = []

    def __iter__(self):
        self.index = -1
        return self

    def next(self):
        # skip lines starting with '#'
        while True:
            if self.index >= len(self.lines) - 1:
                raise StopIteration
            else:
                self.index += 1
                curr = self.lines[self.index]
                if not curr or curr.startswith('#'):
                    continue
                field = curr.split()
                if not len(field) == 3:
                    return (None, RepositoryDefinitionException(curr))
                id = field[0]
                url = field[1]
                active = field[2]
                if id in self.ids:
                    return (None, RepositoryDuplicateException(id,curr)) 
                self.ids.append(id)
                repo = Repository(id,url,active != '0')
                if not active in ['0','1']:
                    return (repo, RepositoryActiveException(active))
                return (repo,None)
        

class BackupStorage:
    '''
    class to handle the storage of the backups
    for the moment just writing to NFS share but could be easily extended
    for e.g. storage on dCache
    '''
    def __init__(self,base,repo):
        self.folder = '/'.join([base,repo.id])

    def ensure(self):
        if not os.path.isdir(self.folder):
            os.mkdir(self.folder)

    def fullpath(self,date):
        return self.folder + '/' + date.isoformat() + '.tar.gz'

    def list(self):
        return set([ f.split('.')[0] for f in os.listdir(self.folder) ])

    def remove(self,list):
        for item in list:
            os.remove(self.folder + '/' + item + '.tar.gz')

class RepositoryBackup:
    '''
    interface class to bash commands that can actually perform the backup
    '''
    def __init__(self,repo):
        self.repo = repo

    def retrieve(self):
        self.tmp = tempfile.mkdtemp()
        ret = subprocess.call([
            'git','clone',
            '--mirror','--quiet',
            self.repo.url,self.tmp + '/' + self.repo.name()
            ])
        if not ret == 0:
            raise RepositoryFetchException(self.repo)
        
    def save(self,fullpath):
        reponame = self.repo.name()
        tarpath = '/'.join([self.tmp,reponame + '.tar'])
        gzpath = tarpath + '.gz'
        try:
            ret = subprocess.call([
                    'tar',
                    '-C',self.tmp,
                    '-cf',tarpath,reponame
                    ])
            if not ret == 0:
                raise RepositoryStoreException(self.repo,fullpath)
            ret = subprocess.call(['gzip',tarpath])
            if not ret == 0:
                raise RepositoryStoreException(self.repo,fullpath)
            shutil.move(gzpath,fullpath)
        except OSError, e:
            raise RepositoryStoreException(self.repo,fullpath)            
        try:
            stat = os.stat(fullpath)
        except OSError:
            raise RepositoryStoreException(self.repo,fullpath)
        else:
            if not stat.st_size > 0:
                raise RepositoryStoreException(self.repo,fullpath)

    # remove temporary files
    def clean(self):
        pass

class RepositoryNameException(Exception):
    def __init__(self,repo):
        self.repo = repo
        self.args = (repo)
class RepositoryFetchException(Exception):
    def __init__(self,repo):
        self.repo = repo
        self.args = (repo)
class RepositoryStoreException(Exception):
    def __init__(self,repo,path):
        self.repo = repo
        self.path = path
        self.args = (repo,path)
class RepositoryDefinitionException(Exception):
    def __init__(self,failed):
        self.failed = failed
        self.args = (failed)
class RepositoryDuplicateException(Exception):
    def __init__(self,id,line):
        self.id = id
        self.line = line
        self.args = (id,line)
class RepositoryActiveException(Exception):
    def __init__(self,val):
        self.val = val
        self.args = (val)



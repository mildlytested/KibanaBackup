"""
Backup Elastisearch objects through the api

usage:
  ElasticBackup.py --config=<filename>
"""
import json
import requests
import os, sys
import configparser
from docopt import docopt


def GetElasticAPI (config, url_api ):
  headers = {'Content-Type': 'application/json',}

  if config['tls']:
    method = 'https://'
  else:
    method = 'http://'

  if config['auth']:
    authentication = config['username'] + ":" + config['password'] + '@'
  else:
    authentication = ''
 
  url = method + authentication + config['server'] + ":" + config['port'] + '/' + url_api

  try:
    if config['tls']:
      r = requests.get (url, headers=headers, verify=config['cert'] )
    else:
      r = requests.get (url, headers=headers ) 

    message = json.loads(r.text)
    if r.status_code != 200:
      raise
    return message
  except:
    print ("Failed to get elasticsaerch api : %s" % url_api)      

def WriteFileJSON(config, FileName, message):
  FilePath = config['backup_folder'] +'/' + FileName
  with open(FilePath, 'w') as outfile:
    json.dump(message, outfile)

#Write ndjson file from byte array
def WriteFileObject(config, FileName, message):
  FilePath = config['backup_folder'] +'/' + FileName
  outfile = open(FilePath, 'wb')
  outfile.write(message)


def GetTemplates(config):
  Alltempalates_json = GetElasticAPI (config, "_template" )

  FileName = 'templates_all.json'
  WriteFileJSON(config, FileName, Alltempalates_json)

  FileName = 'templates_list.txt'
  WriteFileJSON(config, FileName, list( Alltempalates_json.keys() ))

  TemplateList = list( Alltempalates_json.keys() )

  for item in TemplateList:
    item_json = GetElasticAPI (config, "_template/" + item )

    FileName = 'template_' + item + '.json'
    WriteFileJSON(config, FileName, item_json )

def GetPipelines (config):
  Allpipelines_json = GetElasticAPI (config, "_ingest/pipeline" )

  FileName = 'pipelines_all.json'
  WriteFileJSON(config, FileName, Allpipelines_json)

  FileName = 'pipelines_list.txt'
  WriteFileJSON(config, FileName, list( Allpipelines_json.keys() ))

  PipelineList =  list( Allpipelines_json.keys() )

  for item in PipelineList:
    item_json = GetElasticAPI (config, "_ingest/pipeline/" + item )

    FileName = 'pipeline_' + item + '.json'
    WriteFileJSON(config, FileName, item_json )

def GetSecurity (config):
  SecurityUser = GetElasticAPI (config, "_security/user" )
  FileName = 'security_user.json'
  WriteFileJSON(config, FileName, SecurityUser )

  SecurityPrivilege = GetElasticAPI (config, "_security/privilege" )
  FileName = 'security_privilege.json'
  WriteFileJSON(config, FileName, SecurityPrivilege )

  SecurityRole = GetElasticAPI (config, "_security/role" )
  FileName = 'security_role.json'
  WriteFileJSON(config, FileName, SecurityRole )

  SecurityApiKey = GetElasticAPI (config, "_security/api_key" )
  FileName = 'security_apikey.json'
  WriteFileJSON(config, FileName, SecurityApiKey )

def GetTransform (config):
  Transforms = GetElasticAPI (config, "_transform" )
  FileName = 'transforms.json'
  WriteFileJSON(config, FileName, Transforms )

def GetUsage (config):
  XpackUsage = GetElasticAPI (config, "_xpack/usage" )
  FileName = 'xpack_usage.json'
  WriteFileJSON(config, FileName, XpackUsage )

def GetWatcher (config):
  Watches_json = GetElasticAPI (config, ".watches/_search" )
  FileName = 'watches_search.json'
  WriteFileJSON(config, FileName, Watches_json )

  WatchesList = []
  for item in Watches_json['hits']['hits']:
    WatchesList.append(item['_id'])

  FileName = 'watches_list.txt'
  WriteFileJSON(config, FileName, WatchesList )

  for item in WatchesList:
    Watch_json = GetElasticAPI (config, "_watcher/watch/" + item )
    FileName = 'watch_' + item + '.json'
    WriteFileJSON(config, FileName, Watch_json )
    
    
#load config
def LoadConfig(ConfigFile):
  try:
    if os.path.isfile(ConfigFile):
      config = configparser.ConfigParser()
      config.read(ConfigFile)
      if 'ElasticBackup' in config._sections:
        config_dict = {s:dict(config.items(s)) for s in config.sections()}
        RequiredConfig = ['server', 'port', 'backup_folder', 'tls', 'auth']
        for item in RequiredConfig:
          if item not in config_dict['ElasticBackup']:
            print ("Unable to verify configuration file")
            print ("Missing %s from configuration file" % item)
            sys.exit()

        return config_dict['ElasticBackup']
    sys.exit()
  except:
    sys.exit()


def main():

  options = docopt(__doc__)
  if options['--config']:
    ConfigFile=options['--config']
    config = LoadConfig(ConfigFile)

    if not os.path.exists(config['backup_folder']):
      os.mkdir(config['backup_folder'])

    if os.path.exists(config['backup_folder']):
      GetTemplates(config)
      GetPipelines (config)
      GetSecurity (config)
      GetTransform (config)
      GetUsage (config)
      GetWatcher (config)


if __name__ == "__main__":
  main()


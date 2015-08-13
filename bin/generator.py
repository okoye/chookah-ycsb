'''
automatically generate workload files based on certain passed in properties

@author Chuka
'''

import optparse
import pystache
import math


def generate_configuration(options):
  record_count = int(options.rc)
  operation_count = int(options.oc)
  insert_count = int(options.ic)
  hosts = options.h
  thread_count = options.tc
  template_dir = options.template
  output_file = options.output

  assert(record_count >= insert_count)

  #read template file for processing
  template = ''.join([line for line in open(template_dir)])

  num_config_files = int(math.ceil(record_count/insert_count))
  
  raw_input('there will be %s config files generated, press enter to continue '%num_config_files)
  
  for i in xrange(num_config_files):
    context = {
      'recordcount':record_count,
      'operationcount': operation_count,
      'insertstart': i * insert_count,
      'insertcount': insert_count,
      'fieldlength': 200,
      'fieldcount': 10,
      'workload': 'com.yahoo.ycsb.workloads.CoreWorkload',
      'readallfields': 'true',
      'readproportion': 0.9,
      'updateproportion': 0.1,
      'scanproportion': 0,
      'insertproportion': 0,
      'requestdistribution': 'uniform',
      'hosts': hosts,
      'threadcount':thread_count,
      'readconsistencylevel': 'QUORUM',
      'writeconsistencylevel': 'QUORUM'
    }
    
    result = pystache.render(template, context)
    filename = ''.join([output_file, '_', '%s'%i])
    f = open(filename, 'w')
    f.write(result)

###################Start######################
if __name__ == '__main__':
  parser = optparse.OptionParser()
  parser.add_option('-r', '--recordcount', help='no of records',
                    dest='rc', default=500000000)
  parser.add_option('-o', '--operationcount', help='no of operations',
                    dest='oc', default=10000000)
  parser.add_option('-i', '--insertcount', help='no of records per agent',
                    dest='ic', default=25000000)
  parser.add_option('-n', '--hosts', help='hosts seperated by commas',
                    dest='h', default='queenbee')
  parser.add_option('-c', '--threadcount', help='number of concurrent threads',
                    dest='tc', default=100)
  parser.add_option('-t', '--template', help='path to template file',
                    dest='template')
  parser.add_option('-f', '--output', help='output file name',
                    dest='output', default='workload')
  (opts, args) = parser.parse_args()

  generate_configuration(opts)

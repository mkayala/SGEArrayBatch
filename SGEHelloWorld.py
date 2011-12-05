#!/usr/bin/env python
# encoding: utf-8
"""
File Description

File: SGEHelloWorld.py
Created 2011-12-04

Example of running an array job with SGEArrayBatch
"""

import sys, os
from optparse import OptionParser

import SGEArrayBatch as SGE

def main(argv):
    """Callable from Command Line"""
    if argv is None:
        argv = sys.argv;
    
    usageStr = \
        """usage: %prog [options] directory 
        
        Hello World SGEArrayBatch Script.
        Set directory to set the job cwd.
        
        """
        
    parser = OptionParser(usage=usageStr)
    parser.add_option('--name', dest='name', default='SGEHelloWorld')
    parser.add_option('--memfree', dest='memfree', default='20M')
    parser.add_option('--queue', dest='queue', default='my.q')
    (options, args) = parser.parse_args(argv[1:])
     
    if len(args) == 1:
        runScripts(args[0], options)
    else:
        parser.print_help()
        sys.exit(2)    

def runScripts(directory, options):
    """Parse and run the script"""
    cmdStr = """
    
    echo $HOSTNAME
    echo $a $b > $outdir/$a.$b.txt
    """
    outdir = os.path.join(directory, 'output')
    name = options.name;
    
    argDict = {'a': ['Hello', 'Goodbye', 'Tschus'],
               'b': ['World', 'Earth'],
               'outdir':[outdir],
               };
    
    JG = SGE.JobGroup(name=name, command=cmdStr, queue=options.queue,
                      arguments = argDict, memfree=options.memfree);
    
    SGE.build_submission(directory, [JG]);

if __name__ == '__main__':
    sys.exit(main(sys.argv))




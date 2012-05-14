"""
# SGEArrayBatch.py
# Kenny Daily
#
# Modified from:
# sge.py
#
# Creative Commons Attribution License
# http://creativecommons.org/licenses/by/2.5/
#
# Trevor Strohman
#    First release: December 2005
#    Modified from  version:  11 January 2006
#
# Bug finders: Fernando Diaz
#
# At https://github.com/mkayala/SGEArrayBatch since December 2011.
"""
import os
import os.path
import time

class Job:
    """The basic structure of a single Grid Engine Job. 
    """
    def __init__(self, name, command, queue, memfree,
                 priority=None, project=None, rsc_reqs=[]):
        self.name = name
        self.queue = queue
        self.memfree = memfree
        self.command = command
        self.script = command
        self.priority = priority
        self.project = project
        self.rsc_reqs = rsc_reqs
        
        self.dependencies = []
        self.submitted = 0
        
    def addDependency(self, job):
        self.dependencies.append(job)
       
class JobGroup:    
    """A set of jobs sharing some common goal with different arguments.
    
    The arguments dictionary specifies variables to change for each job task.
    """   
    def __init__(self, name, command, queue, memfree, arguments={},
                 priority=None, project=None, rsc_reqs=[]):
        self.name = name
        self.queue = queue
        self.memfree = memfree
        self.command = command
        self.dependencies = []
        self.submitted = 0
        self.arguments = arguments
        self.priority = priority
        self.project = project
        self.rsc_reqs = rsc_reqs
        
        self.generateScript()
        
    def generateScript(self):
        """Generate logic for replacing arguments and parameters.
        """
        self.script = ""
        
        # total number of jobs in this group
        total = 1
        
        # for now, SGE_TASK_ID becomes TASK_ID, but we base it at zero
        self.script += """let "TASK_ID=$SGE_TASK_ID - 1"\n"""

        # build the array definitions
        for key in self.arguments.keys():
            values = self.arguments[key]
            line = ("%s_ARRAY=(" % (key))
            for value in values:
                line += "\'"
                line += value
                line += "\' "
            line += ")\n"
            self.script += line
            total *= len(values)
        self.script += "\n"

        # now, build the decoding logic in the script
        for key in self.arguments.keys():
            count = len(self.arguments[key])
            self.script += """let "%s_INDEX=$TASK_ID %% %d"\n""" \
                % (key,count)
                    
            self.script += """%s=${%s_ARRAY[$%s_INDEX]}\n""" % (key,
                                                                key,
                                                                key)
            
            self.script += """let "TASK_ID=$TASK_ID / %d"\n""" % (count)
            
        # now, run the job
        self.script += "\n"
        self.script += self.command
        self.script += "\n"
            
        # set the number of tasks in this group
        self.tasks = total
        
    def addDependency(self, job):
        self.dependencies.append(job)


def build_directories(directory):
    """Build a set of directories for handling output.
    
    Separate output directories are created for the output of the program,
    stderr, stdout, and the script files for the Job or JobGroup.
    """
    subdirectories = [ "output", "stderr", "stdout", "jobs" ];

    directories = [ os.path.join(directory, subdir)
                    for subdir in subdirectories ]
    
    needed = filter(lambda x: not os.path.exists(x), directories)
    map(os.mkdir, needed)

def build_job_scripts(directory, jobs):
    """Builds and writes the job script file for a list of Jobs (or JobGroups).    
    """
    for job in jobs:
        scriptPath = os.path.join(directory, "jobs", job.name)
        scriptFile = file(scriptPath, "w")
        scriptFile.write("#!/bin/bash\n")
        scriptFile.write("#$ -S /bin/bash\n")
        scriptFile.write(job.script + "\n")
        scriptFile.close()
        os.chmod(scriptPath, 0755)
        job.scriptPath = scriptPath
        
def extract_submittable_jobs(waiting):
    """Return a list of jobs that aren't yet submitted.
    
    These only include jobs that have no dependencies that haven't already been
    submitted.
    """
    submittable = []
    
    for job in waiting:
        unsatisfied = sum([(subjob.submitted==0) for subjob in job.dependencies])
        if unsatisfied == 0:
            submittable.append(job)
    
    return submittable

def submit_safe_jobs(directory, jobs, arch=None, extraargs=None):
    """Submits a list of Jobs or JobGroups to Grid Engine.
    
    Adds parameters for queue to use, stdout, stderr files, and
    mem_free parameter. extraargs are passed as is to qsub.
    
    Checks for dependencies and adds them to the submission command.
    """
    for job in jobs:
        job.out = os.path.join(directory, "stdout")
        job.err = os.path.join(directory, "stderr")
        
        args = " -N %s " % (job.name)
        args += " -o %s -e %s " % (job.out, job.err)
        args += " -l mem_free=%s " % (job.memfree)
        if arch is not None:
            args += " -l arch=%s " % arch;
               
        if job.queue != None:
            args += "-q %s " % job.queue

        if isinstance(job, JobGroup):
            args += "-t 1:%d " % (job.tasks)

        if len(job.dependencies) > 0:
            args += "-hold_jid "
            for dep in job.dependencies:
                args += dep.name + ","
            args = args[:-1]

        if job.priority is not None:
            args += "-p %d " % job.priority
        if job.project is not None:
            args += "-P %s " % job.project
        for rsc in job.rsc_reqs:
            args += '-l "%s" ' % rsc
                  
        qsubcmd = ("qsub %s %s" % (args, job.scriptPath)) 
        print qsubcmd
        os.system(qsubcmd)
        job.submitted = 1

def submit_jobs(directory, jobs, arch=None, extraargs=None):
    """Submits the jobs to the Grid Engine.
    """
    waiting = list(jobs)
    
    while len(waiting) > 0:
        submittable = extract_submittable_jobs(waiting)
        submit_safe_jobs(directory, submittable, arch, extraargs)
        map(waiting.remove, submittable)

def build_submission(directory, jobs, arch=None):
    """Creates necessary directories and scripts for and submits jobs."""    
    build_directories(directory)
    build_job_scripts(directory, jobs)
    submit_jobs(directory, jobs, arch)

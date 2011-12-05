"""Handles job submission to Sun Grid Engine cluster.

This module is meant to allow single job submissions as well as large parameter
sweep submissions to a Grid Engine cluster. Each individual cluster job is
represented by a Job object, and these objects can have dependencies on other
jobs. Once a set of job objects has been created, the sge.build_submission
function dispatches these jobs for execution on the cluster. The library
automatically redirects stdout and stderr of the submitted jobs.
"""

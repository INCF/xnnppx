# See file COPYING distributed with the xnnppx package for the copyright 
# and license.

"""XNAT Python workflow module"""

import sys
import os
import traceback
import datetime
import urllib2
import urlparse
import smtplib
import email.mime.text
import xml.dom.minidom
import suds.client
import suds.xsd.doctor
import suds.transport.http
import pyxnat
import time

# the following variables are set by XnatPythonLauncher:
#     arguments
#     from_email
#     log_file
#     mail_host
#     parameters
#     workflow_info

class WorkflowNotFoundError(Exception):
    """workflow not found"""

class HTTPSudsPreprocessor(urllib2.BaseHandler):

    def http_request(self, req):
        req.add_header('Cookie', 'JSESSIONID=%s' % self.jsessionid)
        return req

    https_request = http_request

class _WorkflowInfo:

    """class for mirroring workflow information (XML) in XNAT"""

    def _call(self, 
              jws, 
              operation, 
              inputs, 
              fix_import=False):
        """perform a SOAP call"""
        url = '%s/axis/%s' % (self._base_url, jws)
        if urlparse.urlparse(url).scheme == 'https':
            t = suds.transport.https.HttpTransport()
        else:
            t = suds.transport.http.HttpTransport()
        t.urlopener = urllib2.build_opener(HTTPSudsPreprocessor)
        for h in t.urlopener.handlers:
            if isinstance(h, HTTPSudsPreprocessor):
                h.jsessionid = self._session
        if fix_import:
            xsd_url = 'http://schemas.xmlsoap.org/soap/encoding/'
            imp = suds.xsd.doctor.Import(xsd_url)
            doctor = suds.xsd.doctor.ImportDoctor(imp)
            client = suds.client.Client('%s?wsdl' % url, 
                                        transport=t, 
                                        doctor=doctor)
        else:
            client = suds.client.Client('%s?wsdl' % url, transport=t)
        typed_inputs = []
        for (dtype, val) in inputs:
            ti = client.factory.create(dtype)
            ti.value = val
            typed_inputs.append(ti)
        # the WSDL returns the local IP address in the URLs; these need 
        # to be corrected if XNAT is behind a proxy
        client.set_options(location=url)
        f = getattr(client.service, operation)
        result = f(*typed_inputs)
        return result

    def __init__(self, base_url, username, password, workflow_id):
        # added these variables to pass to other functions
        # they are needed to make a new JSESSION id
        self._base_url = base_url
        self._username = username
        self._password = password
        
        i = pyxnat.Interface(base_url, username, password)
        i._get_entry_point()
        self._session = i._jsession[11:]
        # if http://schemas.xmlsoap.org/soap/encoding/ > self._base_url
        # we need to change the prefix
        prefix = 'ns0'
        if 'http://schemas.xmlsoap.org/soap/encoding/' > self._base_url:
            prefix = 'ns1'
        args = (('%s:string' % (prefix), self._session), 
                ('%s:string' % (prefix), 'wrk:workflowData.ID'), 
                ('%s:string' % (prefix), '='), 
                ('%s:string' % (prefix), workflow_id), 
                ('%s:string' % (prefix), 'wrk:workflowData'))
        
        self._doc = None
        
        try:
            workflow_ids = self._call('GetIdentifiers.jws', 'search', args)
            for w_id in workflow_ids:
                url = '%s/app/template/XMLSearch.vm/id/%s/data_type/wrk:workflowData' % (self._base_url, str(w_id))
                r = urllib2.Request(url)
                r.add_header('Cookie', 'JSESSIONID=%s' % self._session)
                data = urllib2.urlopen(r).read()
                doc = xml.dom.minidom.parseString(data)
                workflow_node = doc.getElementsByTagName('wrk:Workflow')[0]
                status = workflow_node.getAttribute('status').lower()
                if status in ('queued', 'awaiting action', 'hold'):
                    self._doc = doc
                    self.id = int(w_id)
                    break
        except Exception: # Continue if self._call fails because there are no workflow entries, catch other exceptions
            traceback.print_exc(file=sys.stdout)
            
        if self._doc is None:
            # this is a fix for 1.6.1, in that version, workflow is generated at a different step, so it does not exist by now
            # make initial_xml manually, then store in self._doc and call _update_xnat()
            server= arguments['host']
            pipeline_name = arguments['pipeline']
            project_name = arguments['project']
            data_type = arguments['dataType']
            launch_time = time.strftime('%Y-%m-%dT%X')
            initial_xml = xml.dom.minidom.Document()
            wrk = initial_xml.createElement("wrk:Workflow")
            wrk.setAttribute('data_type', data_type)
            wrk.setAttribute('ID', workflow_id)
            wrk.setAttribute('ExternalID', project_name)
            wrk.setAttribute('current_step_launch_time', launch_time)
            wrk.setAttribute('status', 'Running')
            wrk.setAttribute('pipeline_name', pipeline_name)
            wrk.setAttribute('launch_time', launch_time)
            wrk.setAttribute('percentageComplete', "0.0")
            
            # may want to get this info somewhow instead of hardcoding
            wrk.setAttribute('xmlns:arc', 'http://nrg.wustl.edu/arc')
            wrk.setAttribute('xmlns:val', 'http://nrg.wustl.edu/val')
            wrk.setAttribute('xmlns:pipe', 'http://nrg.wustl.edu/pipe')
            wrk.setAttribute('xmlns:wrk', 'http://nrg.wustl.edu/workflow')
            wrk.setAttribute('xmlns:scr', 'http://nrg.wustl.edu/scr')
            wrk.setAttribute('xmlns:xdat', 'http://nrg.wustl.edu/security')
            wrk.setAttribute('xmlns:cat', 'http://nrg.wustl.edu/catalog')
            wrk.setAttribute('xmlns:prov', 'http://www.nbirn.net/prov')
            wrk.setAttribute('xmlns:xnat', 'http://nrg.wustl.edu/xnat')
            wrk.setAttribute('xmlns:xnat_a', 'http://nrg.wustl.edu/xnat_assessments')
            wrk.setAttribute('xmlns:xsi', 'http://www.w3.org/2001/XMLSchema-instance')
            wrk.setAttribute('xsi:schemaLocation', 
                             'http://nrg.wustl.edu/workflow {0}/schemas/pipeline/workflow.xsd http://nrg.wustl.edu/catalog '.format(server) +
                             '{0}/schemas/catalog/catalog.xsd http://nrg.wustl.edu/pipe {0}/schemas/pipeline/repository.xsd '.format(server) +
                             'http://nrg.wustl.edu/scr {0}/schemas/screening/screeningAssessment.xsd http://nrg.wustl.edu/arc '.format(server) +
                             '{0}/schemas/project/project.xsd http://nrg.wustl.edu/val {0}/schemas/validation/protocolValidation.xsd '.format(server) +
                             'http://nrg.wustl.edu/xnat {0}/schemas/xnat/xnat.xsd http://nrg.wustl.edu/xnat_assessments '.format(server) +
                             '{0}/schemas/assessments/assessments.xsd http://www.nbirn.net/prov {0}/schemas/birn/birnprov.xsd '.format(server) +
                             'http://nrg.wustl.edu/security {0}/schemas/security/security.xsd'.format(server)
                             )
            initial_xml.appendChild(wrk)
            
            self._doc = initial_xml
            self._update_xnat()
            
        return

    def _close(self):
        """close the XNAT session (log out)"""
        self._call('CloseServiceSession.jws', 'execute', ())
        return

    def _update_xnat(self):
        """update XNAT with the current state of this (WorkflowInfo) object"""
        # get a new JESSSION id to avoid timeout, which will cause the pipeline to fail
        i = pyxnat.Interface(self._base_url, self._username, self._password)
        i._get_entry_point()
        self._session = i._jsession[11:]
        inputs = (('ns0:string', self._session), 
                  ('ns0:string', self._doc.toxml()), 
                  ('ns0:boolean', False), 
                  ('ns0:boolean', True))
        self._call('StoreXML.jws', 
                   'store', 
                   inputs, 
                   fix_import=True)
        return

    def _append_node(self, root, name, value):
        """add a simple text node with tag "name" and data "value" under 
        the node "root"
        """
        node = self._doc.createElement(name)
        node.appendChild(self._doc.createTextNode(value))
        root.appendChild(node)
        return

    def set_environment(self, arguments, parameters):
        """set the execution environment

        should be run only once before update() is called
        """
        # order is important
        workflow_node = self._doc.getElementsByTagName('wrk:Workflow')[0]
        ee_node = self._doc.createElement('wrk:executionEnvironment')
        ee_node.setAttribute('xsi:type', 'wrk:xnatExecutionEnvironment')
        workflow_node.appendChild(ee_node)
        self._append_node(ee_node, 'wrk:pipeline', arguments['pipeline'])
        self._append_node(ee_node, 'wrk:xnatuser', arguments['u'])
        self._append_node(ee_node, 'wrk:host', arguments['host'])
        params_node = self._doc.createElement('wrk:parameters')
        ee_node.appendChild(params_node)
        for key in parameters:
            param_node = self._doc.createElement('wrk:parameter')
            param_node.setAttribute('name', key)
            for val in parameters[key]:
                param_node.appendChild(self._doc.createTextNode(val))
            params_node.appendChild(param_node)
        for email in arguments['notify_emails']:
            self._append_node(ee_node, 'wrk:notify', email)
        self._append_node(ee_node, 'wrk:dataType', arguments['dataType'])
        self._append_node(ee_node, 'wrk:id', arguments['id'])
        if arguments['notify_flag']:
            self._append_node(ee_node, 'wrk:supressNotification', '0')
        else:
            self._append_node(ee_node, 'wrk:supressNotification', '1')
        return

    def update(self, step_id, step_description, percent_complete):
        """update the workflow in XNAT"""
        workflow_node = self._doc.getElementsByTagName('wrk:Workflow')[0]
        workflow_node.setAttribute('status', 'Running')
        t = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        workflow_node.setAttribute('current_step_launch_time', t)
        workflow_node.setAttribute('current_step_id', str(step_id))
        workflow_node.setAttribute('step_description', step_description)
        workflow_node.setAttribute('percentageComplete', str(percent_complete))
        self._update_xnat()
        return

    def complete(self):
        """mark the workflow comleted in XNAT and close the session"""
        workflow_node = self._doc.getElementsByTagName('wrk:Workflow')[0]
        workflow_node.setAttribute('status', 'Complete')
        t = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        workflow_node.setAttribute('current_step_launch_time', t)
        workflow_node.setAttribute('percentageComplete', '100.0')
        try:
            workflow_node.removeAttribute('current_step_id')
        except xml.dom.NotFoundErr:
            pass
        try:
            workflow_node.removeAttribute('step_description')
        except xml.dom.NotFoundErr:
            pass
        self._update_xnat()
        self._close()
        return

    def fail(self, description=None):
        """mark the workflow failed in XNAT and close the session"""
        workflow_node = self._doc.getElementsByTagName('wrk:Workflow')[0]
        workflow_node.setAttribute('status', 'Failed')
        if description is not None:
            workflow_node.setAttribute('step_description', description)
        self._update_xnat()
        self._close()
        return

def send_mail(to_addrs, subject, body):
    if not isinstance(to_addrs, (tuple, list)):
        raise TypeError, 'send_mail() expects a tuple or list of recipients'
    if not to_addrs:
        return
    message = email.message.Message()
    message['From'] = from_email
    for addr in to_addrs:
        message['To'] = addr
    message['Subject'] = subject
    message.set_payload(body)
    s = smtplib.SMTP(mail_host)
    s.sendmail(from_email, to_addrs, message.as_string())
    s.quit()
    return

def notification_info():
    """format pipeline arguments and parameters for a notification email"""
    if log_file is None:
        info = 'no log file (output is stdout/stderr)\n'
    else:
        info = 'log file: %s\n' % log_file
    info += '\n'
    info += 'arguments:\n'
    info += '\n'
    for key in sorted(arguments):
        if key == 'pwd':
            info += '    pwd = ********\n'
        else:
            info += '    %s = %s\n' % (key, str(arguments[key]))
    info += '\n'
    info += 'arguments:\n'
    info += '\n'
    for key in sorted(parameters):
        info += '    %s = %s\n' % (key, str(parameters[key]))
    return info

class ContextManager:

    """context manager for running pipelines

    the workflow is marked complete or failed as appropriate, and a notification e-mail is sent is required by the workflow arguments
    """

    def __enter__(self):
        return

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            workflow_info.complete()
            if arguments['notify_flag']:
                send_mail(arguments['notify_emails'],
                          'Pipeline complete',
                          notification_info())
        else:
            if log_file is None:
                workflow_info.fail()
            else:
                workflow_info.fail('see %s for errors' % log_file)
            if arguments['notify_flag']:
                send_mail(arguments['notify_emails'],
                          'Pipeline failed',
                          notification_info())
                sys.stderr.write(''.join(traceback.format_exception(exc_type, exc_val, exc_tb)))
        return

# eof

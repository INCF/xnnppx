import sys
import os
import shutil
import pyxnat
import nipype.interfaces.freesurfer as fs
import xnnppx

try:

    xnnppx.workflow_info.update('1', 'starting', '0.0')
    n_steps = 1+4*len(xnnppx.parameters['scanids'])

    interface = pyxnat.Interface(server=xnnppx.arguments['host'], 
                                 user=xnnppx.arguments['u'], 
                                 password=xnnppx.arguments['pwd'])

    project = interface.select.project(xnnppx.parameters['project'][0])
    subject = project.subject(xnnppx.parameters['subject'][0])
    experiment = subject.experiment(xnnppx.parameters['xnat_id'][0])

    scan_dir = '%s/scan' % xnnppx.parameters['builddir'][0]
    for (n, scan_id) in enumerate(xnnppx.parameters['scanids']):
        xnnppx.workflow_info.update('%da' % (n+1), 
                                    'setting up %s' % scan_id, 
                                    '%.1f' % (100*(1.+4*n)/n_steps))
        scan = experiment.scan(scan_id)
        nifti_name = '%s/%s.nii.gz' % (scan_dir, scan_id)
        # resource.file().put() fails if it's unicode, so encode as ASCII here
        nifti_name = nifti_name.encode('ascii')
        os.mkdir(scan_dir)
        try:
            xnnppx.workflow_info.update('%db' % (n+1), 
                                        'getting %s' % scan_id, 
                                        '%.1f' % (100*(1.+4*n+1)/n_steps))
            for f in scan.resource('DICOM').files():
                file_name = '%s/%s' % (scan_dir, f.attributes()['Name'])
                f.get(file_name)
            xnnppx.workflow_info.update('%dc' % (n+1), 
                                        'converting %s' % scan_id, 
                                        '%.1f' % (100*(1.+4*n+2)/n_steps))
            convert = fs.MRIConvert()
            convert.inputs.in_file = file_name
            convert.inputs.out_file = nifti_name
            convert.run()
            xnnppx.workflow_info.update('%dd' % (n+1), 
                                        'uploading %s' % scan_id, 
                                        '%.1f' % (100*(1.+4*n+3)/n_steps))
            resource = scan.resource('NIfTI')
            resource.attrs.mset({'format': 'NIfTI', 'content': 'RAW'})
            resource.file(os.path.basename(nifti_name)).put(nifti_name)
        finally:
            shutil.rmtree(scan_dir)

except:
    recipients = list(xnnppx.parameters['useremail'])
    recipients.extend(xnnppx.parameters['adminemail'])
    subject = 'convert pipeline for %s failed' % xnnppx.arguments['label']
    message = 'DICOM to NIfTI pipeline for %s failed\n\nsee the logs for details or contact your site administrator' % xnnppx.arguments['label']
    xnnppx.send_mail(recipients, subject, message)
else:
    recipients = list(xnnppx.parameters['useremail'])
    recipients.extend(xnnppx.parameters['adminemail'])
    subject = 'convert pipeline for %s is complete' % xnnppx.arguments['label']
    message = 'DICOM to NIfTI pipeline for %s is complete' % xnnppx.arguments['label']
    xnnppx.send_mail(recipients, subject, message)

# eof

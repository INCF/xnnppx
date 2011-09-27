import xnnppx

# xnnppx.arguments
# xnnppx.parameters

try:
    xnnppx.workflow_info.update('0', 'start', 0)
    # do stuff here...
except:
    if xnnppx.log_file is None:
        xnnppx.workflow_info.fail()
    else:
        xnnppx.workflow_info.fail('see %s for errors' % xnnppx.log_file)
    raise

# eof

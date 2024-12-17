# timed_print(msg,tabs)
#
#   Displays a message 'msg' on the console that is tagged with the current time.
#   The 'tabs' argument indicates the number of 3-space tabs to insert before the
#   message. This allows messages to be organized in some sort of outline.
#   The default number of tabs is zero.

import datetime
import os


def timed_print(msg, tabs=0, no_cr=False):
    if no_cr and ('PYDEVD_LOAD_VALUES_ASYNC' not in os.environ):
        print(datetime.datetime.now().strftime("%H:%M:%S"), "   " * tabs, msg, end='\r')
    else:
        print(datetime.datetime.now().strftime("%H:%M:%S"), "   " * tabs, msg)
    return msg

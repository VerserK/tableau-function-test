import logging

import azure.functions as func
from . import mailnotiWithSQL

mailnotiWithSQL.run()
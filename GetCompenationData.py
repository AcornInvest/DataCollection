import logging
from Intelliquant import Intelliquant

logger = logging.getLogger('GetCompensationData')

intel = Intelliquant()
intel.ChromeOn(logger, 'NoOfShares')
